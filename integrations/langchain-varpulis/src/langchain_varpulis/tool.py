"""LangChain tools for Varpulis event pattern detection.

Provides two tools:
- VarpulisPatternTool: One-shot pattern matching (load VPL, process events, return matches)
- VarpulisSession: Persistent session for stateful pattern detection (sequences, windows)

Both drive the Varpulis CLI via the JSON-line interactive protocol.
"""

from __future__ import annotations

import json
import subprocess
import shutil
from typing import Any, Optional

from langchain_core.tools import BaseTool
from pydantic import Field


class VarpulisSession:
    """A persistent Varpulis interactive session.

    Wraps ``varpulis interactive --json`` as a subprocess. The session maintains
    engine state (loaded patterns, window contents, SASE pattern runs) across
    events — enabling temporal pattern detection over sequences of agent actions.

    Usage::

        session = VarpulisSession()
        session.load_vpl('''
            event AgentAction:
                tool: str
                status: str

            stream RetryStorm = AgentAction.where(status == "error") as e1
                -> AgentAction.where(status == "error" and tool == e1.tool) as e2
                -> AgentAction.where(status == "error" and tool == e1.tool) as e3
                .within(1m)
                .emit(tool: e1.tool, retries: 3)
        ''')

        # Feed events as they happen
        matches = session.inject("AgentAction", {"tool": "search", "status": "error"})
        matches = session.inject("AgentAction", {"tool": "search", "status": "error"})
        matches = session.inject("AgentAction", {"tool": "search", "status": "error"})
        # → [{"stream": "RetryStorm", "event": {"tool": "search", "retries": 3}}]
    """

    def __init__(self, binary: Optional[str] = None):
        """Start a Varpulis interactive session.

        Args:
            binary: Path to the varpulis binary. If None, searches PATH.
        """
        self.binary = binary or shutil.which("varpulis")
        if not self.binary:
            raise FileNotFoundError(
                "varpulis binary not found in PATH. "
                "Install with: curl -sSf https://raw.githubusercontent.com/varpulis/varpulis/main/scripts/install.sh | sh"
            )

        self._proc = subprocess.Popen(
            [self.binary, "interactive", "--json"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,  # Line buffered
        )

        # Read the ready message
        ready = self._read_response()
        if ready.get("type") != "ready":
            raise RuntimeError(f"Unexpected initial response: {ready}")

    def _send(self, cmd: dict[str, Any]) -> None:
        assert self._proc.stdin is not None
        self._proc.stdin.write(json.dumps(cmd) + "\n")
        self._proc.stdin.flush()

    def _read_response(self) -> dict[str, Any]:
        assert self._proc.stdout is not None
        line = self._proc.stdout.readline()
        if not line:
            raise RuntimeError("Varpulis process terminated unexpectedly")
        return json.loads(line.strip())

    def _read_all_responses(self) -> list[dict[str, Any]]:
        """Read all immediately available responses (non-blocking after first)."""
        responses = [self._read_response()]
        # Check for more responses (output events, traces)
        import select
        while select.select([self._proc.stdout], [], [], 0.05)[0]:
            line = self._proc.stdout.readline()
            if line:
                responses.append(json.loads(line.strip()))
            else:
                break
        return responses

    def load_vpl(self, vpl: str) -> dict[str, Any]:
        """Load a VPL program into the session.

        Args:
            vpl: VPL source code.

        Returns:
            Load result with streams list.

        Raises:
            RuntimeError: If VPL has parse or compilation errors.
        """
        self._send({"cmd": "load_vpl", "vpl": vpl})
        result = self._read_response()
        if result.get("type") == "error":
            raise RuntimeError(f"VPL error: {result['message']}")
        return result

    def inject(self, event_type: str, data: dict[str, Any]) -> list[dict[str, Any]]:
        """Inject a single event and return any pattern matches.

        Args:
            event_type: Event type name (must match a declared event type).
            data: Event fields.

        Returns:
            List of output events (pattern matches). Empty if no match.
        """
        self._send({"cmd": "inject", "event_type": event_type, "data": data})
        responses = self._read_all_responses()
        return [r for r in responses if r.get("type") == "output"]

    def get_streams(self) -> list[str]:
        """Get list of loaded stream names."""
        self._send({"cmd": "get_streams"})
        result = self._read_response()
        return [s["name"] for s in result.get("streams", [])]

    def get_topology(self) -> dict[str, Any]:
        """Get pipeline topology as a graph."""
        self._send({"cmd": "get_topology"})
        return self._read_response()

    def close(self):
        """Shut down the session."""
        try:
            self._send({"cmd": "quit"})
            self._proc.wait(timeout=5)
        except Exception:
            self._proc.kill()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class VarpulisPatternTool(BaseTool):
    """LangChain tool for detecting temporal patterns in event sequences.

    This tool gives an AI agent the ability to monitor event streams and
    detect complex temporal patterns (sequences, Kleene closures, negation)
    in real-time.

    Example usage in a LangChain agent::

        from langchain_varpulis import VarpulisPatternTool

        tool = VarpulisPatternTool(
            vpl_pattern='''
                event ToolCall:
                    tool: str
                    status: str
                    tokens: int

                stream RetryStorm = ToolCall.where(status == "error") as e1
                    -> ToolCall.where(status == "error" and tool == e1.tool) as e2
                    -> ToolCall.where(status == "error" and tool == e1.tool) as e3
                    .within(60s)
                    .emit(tool: e1.tool, retries: 3, alert: "retry storm detected")
            ''',
            event_type="ToolCall",
        )

        # Use in a LangChain agent
        agent = create_tool_calling_agent(llm, [tool, ...], prompt)
    """

    name: str = "varpulis_pattern_detector"
    description: str = (
        "Detect temporal patterns in event sequences. "
        "Input should be a JSON object with event fields. "
        "Returns pattern matches if the event (combined with previous events) "
        "triggers a temporal pattern."
    )

    vpl_pattern: str = Field(description="VPL pattern definition")
    event_type: str = Field(default="Event", description="Default event type name")
    _session: Optional[VarpulisSession] = None

    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True

    def _ensure_session(self) -> VarpulisSession:
        if self._session is None:
            self._session = VarpulisSession()
            self._session.load_vpl(self.vpl_pattern)
        return self._session

    def _run(self, input_str: str) -> str:
        """Process an event through the pattern engine.

        Args:
            input_str: JSON string with event fields, e.g. '{"tool": "search", "status": "error"}'

        Returns:
            JSON string with matches, or "No pattern match" if no match.
        """
        session = self._ensure_session()

        try:
            data = json.loads(input_str)
        except json.JSONDecodeError:
            return f"Invalid JSON input: {input_str}"

        event_type = data.pop("event_type", self.event_type)
        matches = session.inject(event_type, data)

        if matches:
            return json.dumps(matches, indent=2)
        return "No pattern match (event processed, waiting for more events to complete the pattern)"
