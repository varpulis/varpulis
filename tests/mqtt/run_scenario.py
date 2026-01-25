#!/usr/bin/env python3
"""
Scenario Runner - Execute YAML-defined test scenarios

Usage:
    python run_scenario.py scenarios/fraud_scenario.yaml
    python run_scenario.py scenarios/trading_scenario.yaml --validate
"""

import sys
import time
import json
import yaml
import random
import threading
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List, Optional
from collections import defaultdict

import click
import paho.mqtt.client as mqtt
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn
from rich.live import Live
from rich.panel import Panel

from simulator import (
    Event, Login, Transaction, Logout, PasswordChange,
    SensorReading, MarketTick, MQTTPublisher, fake
)

console = Console()


@dataclass
class PhaseResult:
    name: str
    duration: float
    events_sent: int
    actual_rate: float
    patterns_detected: Dict[str, int]


@dataclass
class ScenarioResult:
    name: str
    total_events: int
    total_duration: float
    phases: List[PhaseResult]
    patterns_expected: Dict[str, int]
    patterns_detected: Dict[str, int]
    latencies: List[float]
    passed: bool
    failures: List[str]


class PatternCollector:
    """Collects pattern detections from Varpulis via MQTT"""
    
    def __init__(self, host: str, port: int, topic: str = "varpulis/patterns/#"):
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.host = host
        self.port = port
        self.topic = topic
        self.patterns: Dict[str, List[dict]] = defaultdict(list)
        self.latencies: List[float] = []
        self._lock = threading.Lock()
        
    def start(self):
        def on_message(client, userdata, msg):
            try:
                data = json.loads(msg.payload.decode())
                pattern_name = data.get("pattern", "unknown")
                detected_at = datetime.fromisoformat(data.get("detected_at", ""))
                event_time = datetime.fromisoformat(data.get("event_time", ""))
                latency = (detected_at - event_time).total_seconds() * 1000
                
                with self._lock:
                    self.patterns[pattern_name].append(data)
                    self.latencies.append(latency)
            except Exception as e:
                console.print(f"[red]Error parsing pattern: {e}[/red]")
        
        self.client.on_message = on_message
        self.client.connect(self.host, self.port, 60)
        self.client.subscribe(self.topic)
        self.client.loop_start()
    
    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()
    
    def get_counts(self) -> Dict[str, int]:
        with self._lock:
            return {k: len(v) for k, v in self.patterns.items()}
    
    def get_latencies(self) -> List[float]:
        with self._lock:
            return self.latencies.copy()


class ScenarioRunner:
    """Runs YAML-defined test scenarios"""
    
    def __init__(self, scenario_path: Path):
        with open(scenario_path) as f:
            self.config = yaml.safe_load(f)
        
        self.name = self.config["name"]
        self.mqtt_config = self.config.get("mqtt", {})
        self.phases = self.config.get("phases", [])
        self.expected = self.config.get("expected_patterns", [])
        self.performance = self.config.get("performance", {})
        
        self.publisher = None
        self.collector = None
        self.results: List[PhaseResult] = []
        self.patterns_generated: Dict[str, int] = defaultdict(int)
    
    def setup(self):
        host = self.mqtt_config.get("host", "localhost")
        port = self.mqtt_config.get("port", 1883)
        
        self.publisher = MQTTPublisher(host=host, port=port)
        self.publisher.connect()
        
        self.collector = PatternCollector(host, port)
        self.collector.start()
    
    def teardown(self):
        if self.publisher:
            self.publisher.disconnect()
        if self.collector:
            self.collector.stop()
    
    def run(self) -> ScenarioResult:
        console.print(Panel(
            f"[bold]{self.name}[/bold]\n{self.config.get('description', '')}",
            title="Scenario"
        ))
        
        self.setup()
        total_events = 0
        start_time = time.time()
        
        try:
            for phase_config in self.phases:
                result = self._run_phase(phase_config)
                self.results.append(result)
                total_events += result.events_sent
        finally:
            self.teardown()
        
        total_duration = time.time() - start_time
        
        # Validate results
        failures = self._validate()
        
        return ScenarioResult(
            name=self.name,
            total_events=total_events,
            total_duration=total_duration,
            phases=self.results,
            patterns_expected=dict(self.patterns_generated),
            patterns_detected=self.collector.get_counts() if self.collector else {},
            latencies=self.collector.get_latencies() if self.collector else [],
            passed=len(failures) == 0,
            failures=failures
        )
    
    def _run_phase(self, phase: dict) -> PhaseResult:
        name = phase["name"]
        duration_str = phase.get("duration", "10s")
        duration = self._parse_duration(duration_str)
        rate = phase.get("rate", 100)
        events_config = phase.get("events", [])
        
        console.print(f"\n[cyan]Phase: {name}[/cyan] - {phase.get('description', '')}")
        
        events_sent = 0
        phase_start = time.time()
        interval = 1.0 / rate
        
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console
        ) as progress:
            task = progress.add_task(f"Phase: {name}", total=int(duration * rate))
            
            while (time.time() - phase_start) < duration:
                event, pattern = self._generate_event(events_config)
                if pattern:
                    self.patterns_generated[pattern] += 1
                
                self.publisher.publish(event)
                events_sent += 1
                progress.advance(task)
                
                time.sleep(interval)
        
        phase_duration = time.time() - phase_start
        actual_rate = events_sent / phase_duration if phase_duration > 0 else 0
        
        return PhaseResult(
            name=name,
            duration=phase_duration,
            events_sent=events_sent,
            actual_rate=actual_rate,
            patterns_detected=self.collector.get_counts() if self.collector else {}
        )
    
    def _generate_event(self, events_config: List[dict]) -> tuple:
        """Generate event based on weighted config"""
        weights = [e.get("weight", 1.0) for e in events_config]
        total = sum(weights)
        weights = [w / total for w in weights]
        
        event_type = random.choices(events_config, weights=weights)[0]["type"]
        
        pattern = None
        
        if event_type == "normal_session":
            user_id = f"user_{random.randint(0, 999):04d}"
            return Login(
                user_id=user_id,
                ip_address=fake.ipv4(),
                country=random.choice(["US", "UK", "FR", "DE"]),
                device_type=random.choice(["mobile", "desktop"])
            ).to_event(), None
        
        elif event_type == "account_takeover":
            user_id = f"user_{random.randint(0, 999):04d}"
            pattern = "AccountTakeover"
            return Transaction(
                user_id=user_id,
                amount=random.uniform(5000, 50000),
                merchant="Unknown",
                category="transfer"
            ).to_event(), pattern
        
        elif event_type == "impossible_travel":
            pattern = "ImpossibleTravel"
            return Login(
                user_id=f"user_{random.randint(0, 999):04d}",
                ip_address=fake.ipv4(),
                country=random.choice(["JP", "AU", "BR"]),
                device_type="mobile"
            ).to_event(), pattern
        
        elif event_type == "high_velocity":
            pattern = "HighVelocity"
            return Transaction(
                user_id=f"user_{random.randint(0, 99):04d}",
                amount=random.uniform(100, 1000),
                merchant=random.choice(["Amazon", "eBay"]),
                category="electronics"
            ).to_event(), pattern
        
        elif event_type == "market_tick":
            return MarketTick(
                symbol=random.choice(["AAPL", "GOOGL", "MSFT"]),
                price=random.uniform(100, 500),
                volume=random.randint(100, 10000),
                bid=random.uniform(99, 499),
                ask=random.uniform(101, 501)
            ).to_event(), None
        
        elif event_type == "volume_spike":
            pattern = "VolumeSpike"
            return MarketTick(
                symbol=random.choice(["AAPL", "GOOGL", "MSFT"]),
                price=random.uniform(100, 500),
                volume=random.randint(50000, 200000),
                bid=random.uniform(99, 499),
                ask=random.uniform(101, 501)
            ).to_event(), pattern
        
        elif event_type == "sensor_reading":
            return SensorReading(
                sensor_id=f"sensor_{random.randint(0, 99):03d}",
                temperature=random.uniform(18, 25),
                humidity=random.uniform(40, 60),
                pressure=random.uniform(1010, 1020)
            ).to_event(), None
        
        elif event_type == "temp_spike":
            pattern = "TemperatureSpike"
            return SensorReading(
                sensor_id=f"sensor_{random.randint(0, 99):03d}",
                temperature=random.uniform(40, 60),
                humidity=random.uniform(40, 60),
                pressure=random.uniform(1010, 1020)
            ).to_event(), pattern
        
        # Default
        return Login(
            user_id="default_user",
            ip_address="127.0.0.1",
            country="US",
            device_type="desktop"
        ).to_event(), None
    
    def _parse_duration(self, duration_str: str) -> float:
        """Parse duration string like '10s', '5m', '1h'"""
        if duration_str.endswith("s"):
            return float(duration_str[:-1])
        elif duration_str.endswith("m"):
            return float(duration_str[:-1]) * 60
        elif duration_str.endswith("h"):
            return float(duration_str[:-1]) * 3600
        return float(duration_str)
    
    def _validate(self) -> List[str]:
        """Validate results against expectations"""
        failures = []
        
        # Check pattern counts
        detected = self.collector.get_counts() if self.collector else {}
        for expected in self.expected:
            pattern = expected["pattern"]
            min_count = expected.get("min_count", 0)
            actual = detected.get(pattern, 0)
            
            if actual < min_count:
                failures.append(
                    f"Pattern {pattern}: expected >= {min_count}, got {actual}"
                )
        
        # Check latencies
        latencies = self.collector.get_latencies() if self.collector else []
        if latencies:
            p99 = sorted(latencies)[int(len(latencies) * 0.99)] if len(latencies) > 100 else max(latencies)
            max_p99 = self.performance.get("max_p99_latency_ms", float("inf"))
            
            if p99 > max_p99:
                failures.append(f"P99 latency {p99:.1f}ms exceeds max {max_p99}ms")
        
        return failures


def print_results(result: ScenarioResult):
    """Print scenario results"""
    status = "[green]✓ PASSED[/green]" if result.passed else "[red]✗ FAILED[/red]"
    
    console.print(f"\n{status} Scenario: {result.name}")
    
    # Summary table
    table = Table(title="Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    
    table.add_row("Total Events", f"{result.total_events:,}")
    table.add_row("Total Duration", f"{result.total_duration:.1f}s")
    table.add_row("Avg Throughput", f"{result.total_events/result.total_duration:.0f} events/s")
    
    if result.latencies:
        avg_lat = sum(result.latencies) / len(result.latencies)
        p99_lat = sorted(result.latencies)[int(len(result.latencies) * 0.99)] if len(result.latencies) > 100 else max(result.latencies)
        table.add_row("Avg Latency", f"{avg_lat:.1f}ms")
        table.add_row("P99 Latency", f"{p99_lat:.1f}ms")
    
    console.print(table)
    
    # Phase results
    phase_table = Table(title="Phases")
    phase_table.add_column("Phase")
    phase_table.add_column("Duration")
    phase_table.add_column("Events")
    phase_table.add_column("Rate")
    
    for phase in result.phases:
        phase_table.add_row(
            phase.name,
            f"{phase.duration:.1f}s",
            f"{phase.events_sent:,}",
            f"{phase.actual_rate:.0f}/s"
        )
    
    console.print(phase_table)
    
    # Failures
    if result.failures:
        console.print("\n[red]Failures:[/red]")
        for f in result.failures:
            console.print(f"  - {f}")


@click.command()
@click.argument("scenario_file", type=click.Path(exists=True))
@click.option("--validate", "-v", is_flag=True, help="Validate against Varpulis output")
def main(scenario_file: str, validate: bool):
    """Run a YAML-defined test scenario"""
    
    runner = ScenarioRunner(Path(scenario_file))
    result = runner.run()
    print_results(result)
    
    sys.exit(0 if result.passed else 1)


if __name__ == "__main__":
    main()
