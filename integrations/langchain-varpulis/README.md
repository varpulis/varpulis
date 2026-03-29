# langchain-varpulis

> Give your AI agent real-time event intelligence.

LangChain tool for detecting temporal patterns in event sequences using the [Varpulis](https://github.com/varpulis/varpulis) CEP engine.

## Install

```bash
pip install langchain-varpulis
```

Requires the `varpulis` binary in PATH:
```bash
curl -sSf https://raw.githubusercontent.com/varpulis/varpulis/main/scripts/install.sh | sh
```

## Quick Start

```python
from langchain_varpulis import VarpulisPatternTool

# Define a pattern: 3 errors on the same tool within 1 minute
tool = VarpulisPatternTool(
    vpl_pattern="""
        event ToolCall:
            tool: str
            status: str

        stream RetryStorm = ToolCall.where(status == "error") as e1
            -> ToolCall.where(status == "error" and tool == e1.tool) as e2
            -> ToolCall.where(status == "error" and tool == e1.tool) as e3
            .within(60s)
            .emit(tool: e1.tool, retries: 3, alert: "retry storm")
    """,
    event_type="ToolCall",
)

# Feed events (from your agent's tool calls)
result = tool.run('{"tool": "web_search", "status": "error"}')
# → "No pattern match (event processed, waiting for more events...)"

result = tool.run('{"tool": "web_search", "status": "error"}')
# → "No pattern match..."

result = tool.run('{"tool": "web_search", "status": "error"}')
# → [{"stream": "RetryStorm", "event": {"tool": "web_search", "retries": 3, "alert": "retry storm"}}]
```

## Session API

For lower-level control:

```python
from langchain_varpulis import VarpulisSession

with VarpulisSession() as session:
    session.load_vpl("event T: x: int\nstream S = T .where(x > 10) .emit(v: x)")

    matches = session.inject("T", {"x": 42})
    print(matches)  # [{"stream": "S", "event": {"v": 42}}]

    matches = session.inject("T", {"x": 5})
    print(matches)  # [] (filtered)

    print(session.get_streams())  # ["S"]
```

## Use Cases

- **Agent guardrails**: Detect retry storms, circular reasoning, budget overruns
- **Workflow monitoring**: Alert when step sequences indicate failure patterns
- **Real-time analysis**: Feed live data through patterns during agent execution

## Links

- [Varpulis Documentation](https://www.varpulis-cep.com/docs/)
- [VPL Language Tutorial](https://www.varpulis-cep.com/docs/tutorials/language-tutorial)
- [Interactive Shell Tutorial](https://www.varpulis-cep.com/docs/tutorials/interactive-shell-tutorial)

## License

MIT
