#!/usr/bin/env python3
"""
Varpulis Event Simulator - MQTT Event Injection

Usage:
    python simulator.py --scenario fraud --rate 100 --duration 60
    python simulator.py --scenario trading --rate 1000 --burst
    python simulator.py --scenario iot --devices 50
"""

import json
import time
import random
import threading
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import Generator, Optional, Callable
from abc import ABC, abstractmethod

import click
import paho.mqtt.client as mqtt
from faker import Faker
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

fake = Faker()
console = Console()


# =============================================================================
# Event Types
# =============================================================================

@dataclass
class Event:
    event_type: str
    timestamp: str
    data: dict

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class Login:
    user_id: str
    ip_address: str
    country: str
    device_type: str
    
    def to_event(self) -> Event:
        return Event(
            event_type="Login",
            timestamp=datetime.utcnow().isoformat(),
            data=asdict(self)
        )


@dataclass
class Transaction:
    user_id: str
    amount: float
    merchant: str
    category: str
    
    def to_event(self) -> Event:
        return Event(
            event_type="Transaction",
            timestamp=datetime.utcnow().isoformat(),
            data=asdict(self)
        )


@dataclass
class Logout:
    user_id: str
    
    def to_event(self) -> Event:
        return Event(
            event_type="Logout",
            timestamp=datetime.utcnow().isoformat(),
            data=asdict(self)
        )


@dataclass
class PasswordChange:
    user_id: str
    
    def to_event(self) -> Event:
        return Event(
            event_type="PasswordChange",
            timestamp=datetime.utcnow().isoformat(),
            data=asdict(self)
        )


@dataclass 
class SensorReading:
    sensor_id: str
    temperature: float
    humidity: float
    pressure: float
    
    def to_event(self) -> Event:
        return Event(
            event_type="SensorReading",
            timestamp=datetime.utcnow().isoformat(),
            data=asdict(self)
        )


@dataclass
class MarketTick:
    symbol: str
    price: float
    volume: int
    bid: float
    ask: float
    
    def to_event(self) -> Event:
        return Event(
            event_type="MarketTick",
            timestamp=datetime.utcnow().isoformat(),
            data=asdict(self)
        )


# =============================================================================
# Scenarios
# =============================================================================

class Scenario(ABC):
    """Base class for test scenarios"""
    
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self.events_sent = 0
        self.patterns_expected = []
    
    @abstractmethod
    def generate_events(self) -> Generator[Event, None, None]:
        """Generate events for this scenario"""
        pass
    
    def get_expected_patterns(self) -> list:
        """Return expected pattern matches for validation"""
        return self.patterns_expected


class FraudDetectionScenario(Scenario):
    """
    Simulates fraud detection patterns:
    - Normal user sessions
    - Account takeover attempts (login -> password change -> large tx)
    - Impossible travel (logins from different countries)
    - High velocity transactions
    """
    
    def __init__(self, num_users: int = 100, fraud_ratio: float = 0.1):
        super().__init__(
            "fraud",
            "Fraud detection with account takeover, impossible travel, velocity"
        )
        self.num_users = num_users
        self.fraud_ratio = fraud_ratio
        self.users = [f"user_{i:04d}" for i in range(num_users)]
        self.countries = ["US", "UK", "FR", "DE", "JP", "CN", "BR", "AU"]
        self.devices = ["mobile", "desktop", "tablet", "unknown"]
        self.merchants = ["Amazon", "Walmart", "Target", "BestBuy", "Apple", "Netflix"]
        self.categories = ["electronics", "groceries", "entertainment", "gambling", "travel"]
    
    def generate_events(self) -> Generator[Event, None, None]:
        while True:
            user_id = random.choice(self.users)
            
            if random.random() < self.fraud_ratio:
                # Generate fraud pattern
                pattern_type = random.choice([
                    "account_takeover",
                    "impossible_travel", 
                    "high_velocity"
                ])
                yield from self._generate_fraud_pattern(user_id, pattern_type)
            else:
                # Normal session
                yield from self._generate_normal_session(user_id)
    
    def _generate_normal_session(self, user_id: str) -> Generator[Event, None, None]:
        """Normal user: login -> 1-3 transactions -> logout"""
        country = random.choice(self.countries)
        
        yield Login(
            user_id=user_id,
            ip_address=fake.ipv4(),
            country=country,
            device_type=random.choice(self.devices)
        ).to_event()
        
        for _ in range(random.randint(1, 3)):
            yield Transaction(
                user_id=user_id,
                amount=round(random.uniform(10, 500), 2),
                merchant=random.choice(self.merchants),
                category=random.choice(self.categories[:4])  # No gambling
            ).to_event()
        
        if random.random() > 0.3:
            yield Logout(user_id=user_id).to_event()
    
    def _generate_fraud_pattern(self, user_id: str, pattern: str) -> Generator[Event, None, None]:
        """Generate specific fraud patterns"""
        
        if pattern == "account_takeover":
            # Login -> PasswordChange -> Large Transaction -> Logout
            self.patterns_expected.append({
                "pattern": "AccountTakeover",
                "user_id": user_id
            })
            
            yield Login(
                user_id=user_id,
                ip_address=fake.ipv4(),
                country=random.choice(self.countries),
                device_type="unknown"
            ).to_event()
            
            yield PasswordChange(user_id=user_id).to_event()
            
            yield Transaction(
                user_id=user_id,
                amount=round(random.uniform(5000, 50000), 2),
                merchant=random.choice(self.merchants),
                category="electronics"
            ).to_event()
            
            yield Logout(user_id=user_id).to_event()
        
        elif pattern == "impossible_travel":
            # Login from US, then login from JP within 1 hour
            self.patterns_expected.append({
                "pattern": "ImpossibleTravel",
                "user_id": user_id
            })
            
            countries = random.sample(self.countries, 2)
            ip1 = fake.ipv4()
            ip2 = fake.ipv4()
            
            yield Login(
                user_id=user_id,
                ip_address=ip1,
                country=countries[0],
                device_type="desktop"
            ).to_event()
            
            yield Login(
                user_id=user_id,
                ip_address=ip2,
                country=countries[1],
                device_type="mobile"
            ).to_event()
        
        elif pattern == "high_velocity":
            # 10+ transactions in rapid succession
            self.patterns_expected.append({
                "pattern": "HighVelocity",
                "user_id": user_id
            })
            
            yield Login(
                user_id=user_id,
                ip_address=fake.ipv4(),
                country=random.choice(self.countries),
                device_type=random.choice(self.devices)
            ).to_event()
            
            for _ in range(random.randint(10, 20)):
                yield Transaction(
                    user_id=user_id,
                    amount=round(random.uniform(100, 1000), 2),
                    merchant=random.choice(self.merchants),
                    category=random.choice(self.categories)
                ).to_event()


class TradingScenario(Scenario):
    """
    Simulates financial market data:
    - Market ticks at high frequency
    - Price movements and volatility
    - Volume spikes
    """
    
    def __init__(self, symbols: Optional[list] = None):
        super().__init__(
            "trading",
            "High-frequency market data with price movements and volume spikes"
        )
        self.symbols = symbols or ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA"]
        self.prices = {s: random.uniform(100, 500) for s in self.symbols}
        self.volatility = {s: random.uniform(0.001, 0.01) for s in self.symbols}
    
    def generate_events(self) -> Generator[Event, None, None]:
        while True:
            symbol = random.choice(self.symbols)
            
            # Update price with random walk
            change = random.gauss(0, self.volatility[symbol])
            self.prices[symbol] *= (1 + change)
            
            price = self.prices[symbol]
            spread = price * random.uniform(0.0001, 0.001)
            
            # Occasional volume spike
            base_volume = random.randint(100, 10000)
            if random.random() < 0.05:
                base_volume *= random.randint(5, 20)
                self.patterns_expected.append({
                    "pattern": "VolumeSpike",
                    "symbol": symbol
                })
            
            yield MarketTick(
                symbol=symbol,
                price=round(price, 2),
                volume=base_volume,
                bid=round(price - spread/2, 2),
                ask=round(price + spread/2, 2)
            ).to_event()


class IoTScenario(Scenario):
    """
    Simulates IoT sensor data:
    - Temperature, humidity, pressure readings
    - Anomaly detection (out of range values)
    - Sensor failures
    """
    
    def __init__(self, num_sensors: int = 50):
        super().__init__(
            "iot",
            "IoT sensor data with anomalies and failures"
        )
        self.num_sensors = num_sensors
        self.sensors = [f"sensor_{i:03d}" for i in range(num_sensors)]
        self.base_temp = {s: random.uniform(18, 25) for s in self.sensors}
        self.base_humidity = {s: random.uniform(40, 60) for s in self.sensors}
        self.base_pressure = {s: random.uniform(1010, 1020) for s in self.sensors}
    
    def generate_events(self) -> Generator[Event, None, None]:
        while True:
            sensor_id = random.choice(self.sensors)
            
            # Normal readings with slight variation
            temp = self.base_temp[sensor_id] + random.gauss(0, 0.5)
            humidity = self.base_humidity[sensor_id] + random.gauss(0, 2)
            pressure = self.base_pressure[sensor_id] + random.gauss(0, 0.5)
            
            # Occasional anomalies
            if random.random() < 0.02:
                anomaly_type = random.choice(["temp_spike", "humidity_drop", "sensor_failure"])
                
                if anomaly_type == "temp_spike":
                    temp += random.uniform(10, 30)
                    self.patterns_expected.append({
                        "pattern": "TemperatureSpike",
                        "sensor_id": sensor_id
                    })
                elif anomaly_type == "humidity_drop":
                    humidity -= random.uniform(20, 40)
                    self.patterns_expected.append({
                        "pattern": "HumidityAnomaly", 
                        "sensor_id": sensor_id
                    })
                elif anomaly_type == "sensor_failure":
                    temp = -999.0
                    humidity = -999.0
                    pressure = -999.0
                    self.patterns_expected.append({
                        "pattern": "SensorFailure",
                        "sensor_id": sensor_id
                    })
            
            yield SensorReading(
                sensor_id=sensor_id,
                temperature=round(temp, 2),
                humidity=round(humidity, 2),
                pressure=round(pressure, 2)
            ).to_event()


# =============================================================================
# MQTT Publisher
# =============================================================================

class MQTTPublisher:
    """Publishes events to MQTT broker"""
    
    def __init__(self, host: str = "localhost", port: int = 1883, topic_prefix: str = "varpulis"):
        self.host = host
        self.port = port
        self.topic_prefix = topic_prefix
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.connected = False
        self.events_published = 0
        self.bytes_sent = 0
        
    def connect(self):
        def on_connect(client, userdata, flags, reason_code, properties):
            self.connected = True
            console.print(f"[green]✓[/green] Connected to MQTT broker {self.host}:{self.port}")
        
        def on_disconnect(client, userdata, flags, reason_code, properties):
            self.connected = False
            console.print(f"[yellow]![/yellow] Disconnected from MQTT broker")
        
        self.client.on_connect = on_connect
        self.client.on_disconnect = on_disconnect
        self.client.connect(self.host, self.port, 60)
        self.client.loop_start()
        
        # Wait for connection
        for _ in range(50):
            if self.connected:
                break
            time.sleep(0.1)
        
        if not self.connected:
            raise ConnectionError(f"Could not connect to MQTT broker at {self.host}:{self.port}")
    
    def publish(self, event: Event):
        topic = f"{self.topic_prefix}/events/{event.event_type}"
        payload = event.to_json()
        self.client.publish(topic, payload, qos=1)
        self.events_published += 1
        self.bytes_sent += len(payload)
    
    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()
    
    def stats(self) -> dict:
        return {
            "events_published": self.events_published,
            "bytes_sent": self.bytes_sent,
            "connected": self.connected
        }


# =============================================================================
# Simulator Runner
# =============================================================================

class SimulatorRunner:
    """Runs scenarios and publishes events"""
    
    def __init__(self, publisher: MQTTPublisher, scenario: Scenario):
        self.publisher = publisher
        self.scenario = scenario
        self.running = False
        self.start_time = None
        self.events_sent = 0
    
    def run(self, rate: int = 100, duration: int = 60, burst: bool = False):
        """
        Run the scenario
        
        Args:
            rate: Events per second
            duration: Duration in seconds (0 = infinite)
            burst: If True, send events as fast as possible
        """
        self.running = True
        self.start_time = time.time()
        self.events_sent = 0
        
        event_gen = self.scenario.generate_events()
        interval = 1.0 / rate if not burst else 0
        
        console.print(f"\n[bold blue]Starting scenario:[/bold blue] {self.scenario.name}")
        console.print(f"  Rate: {'burst' if burst else f'{rate}/sec'}")
        console.print(f"  Duration: {'infinite' if duration == 0 else f'{duration}s'}\n")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Sending events...", total=None)
            
            try:
                while self.running:
                    if duration > 0 and (time.time() - self.start_time) >= duration:
                        break
                    
                    event = next(event_gen)
                    self.publisher.publish(event)
                    self.events_sent += 1
                    
                    if self.events_sent % 100 == 0:
                        elapsed = time.time() - self.start_time
                        actual_rate = self.events_sent / elapsed if elapsed > 0 else 0
                        progress.update(
                            task,
                            description=f"Events: {self.events_sent:,} | Rate: {actual_rate:.0f}/s"
                        )
                    
                    if not burst:
                        time.sleep(interval)
            
            except KeyboardInterrupt:
                self.running = False
        
        self.print_summary()
    
    def print_summary(self):
        elapsed = time.time() - self.start_time
        rate = self.events_sent / elapsed if elapsed > 0 else 0
        
        table = Table(title="Simulation Summary")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Scenario", self.scenario.name)
        table.add_row("Duration", f"{elapsed:.2f}s")
        table.add_row("Events Sent", f"{self.events_sent:,}")
        table.add_row("Actual Rate", f"{rate:.0f} events/sec")
        table.add_row("Bytes Sent", f"{self.publisher.bytes_sent:,}")
        table.add_row("Expected Patterns", f"{len(self.scenario.patterns_expected)}")
        
        console.print(table)
        
        if self.scenario.patterns_expected:
            console.print("\n[bold]Expected Pattern Matches:[/bold]")
            pattern_counts = {}
            for p in self.scenario.patterns_expected:
                name = p["pattern"]
                pattern_counts[name] = pattern_counts.get(name, 0) + 1
            for name, count in pattern_counts.items():
                console.print(f"  - {name}: {count}")
    
    def stop(self):
        self.running = False


# =============================================================================
# CLI
# =============================================================================

SCENARIOS = {
    "fraud": FraudDetectionScenario,
    "trading": TradingScenario,
    "iot": IoTScenario,
}


@click.command()
@click.option("--scenario", "-s", type=click.Choice(list(SCENARIOS.keys())), required=True,
              help="Scenario to run")
@click.option("--rate", "-r", default=100, help="Events per second")
@click.option("--duration", "-d", default=60, help="Duration in seconds (0 = infinite)")
@click.option("--burst", "-b", is_flag=True, help="Send events as fast as possible")
@click.option("--host", "-h", default="localhost", help="MQTT broker host")
@click.option("--port", "-p", default=1883, help="MQTT broker port")
@click.option("--users", default=100, help="Number of users (fraud scenario)")
@click.option("--sensors", default=50, help="Number of sensors (iot scenario)")
def main(scenario: str, rate: int, duration: int, burst: bool, 
         host: str, port: int, users: int, sensors: int):
    """
    Varpulis Event Simulator
    
    Injects events via MQTT for testing CEP patterns.
    """
    console.print("[bold]Varpulis Event Simulator[/bold]")
    console.print("=" * 40)
    
    # Create scenario
    if scenario == "fraud":
        sc = FraudDetectionScenario(num_users=users)
    elif scenario == "iot":
        sc = IoTScenario(num_sensors=sensors)
    else:
        sc = SCENARIOS[scenario]()
    
    # Connect to MQTT
    publisher = MQTTPublisher(host=host, port=port)
    try:
        publisher.connect()
    except ConnectionError as e:
        console.print(f"[red]Error:[/red] {e}")
        console.print("\n[yellow]Hint:[/yellow] Start Mosquitto with:")
        console.print("  cd tests/mqtt && docker-compose up -d")
        return
    
    # Run simulation
    runner = SimulatorRunner(publisher, sc)
    try:
        runner.run(rate=rate, duration=duration, burst=burst)
    finally:
        publisher.disconnect()


if __name__ == "__main__":
    main()
