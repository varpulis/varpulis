#!/usr/bin/env python3
"""
E2E Showcase Consumer

Reads trading signals from Kafka and displays a live Rich terminal dashboard.
"""

import argparse
import json
import signal
import sys
import time
from collections import deque
from threading import Thread

try:
    from confluent_kafka import Consumer, KafkaError
except ImportError:
    print("ERROR: confluent-kafka not installed. Run: pip install confluent-kafka")
    sys.exit(1)

try:
    from rich.console import Console
    from rich.live import Live
    from rich.table import Table
    from rich.panel import Panel
    from rich.layout import Layout
    from rich.text import Text
except ImportError:
    print("ERROR: rich not installed. Run: pip install rich")
    sys.exit(1)


class SignalDashboard:
    def __init__(self, kafka_brokers: str, group_id: str = "e2e-consumer"):
        self.kafka_brokers = kafka_brokers
        self.group_id = group_id
        self.running = True
        self.start_time = time.time()

        # Counters
        self.breakout_count = 0
        self.avg_price_count = 0
        self.large_trade_count = 0
        self.total_consumed = 0

        # Recent items
        self.recent_breakouts: deque = deque(maxlen=8)
        self.recent_large_trades: deque = deque(maxlen=8)
        self.recent_averages: deque = deque(maxlen=5)

        # Rate tracking
        self.rate_window: deque = deque(maxlen=100)

    def consume_loop(self):
        conf = {
            "bootstrap.servers": self.kafka_brokers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
        consumer = Consumer(conf)
        consumer.subscribe(["trading.breakouts", "trading.averages", "trading.large-trades"])

        try:
            while self.running:
                msg = consumer.poll(0.1)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        sys.stderr.write(f"Kafka error: {msg.error()}\n")
                    continue

                topic = msg.topic()
                try:
                    data = json.loads(msg.value().decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue

                now = time.strftime("%H:%M:%S")
                self.total_consumed += 1
                self.rate_window.append(time.time())

                if topic == "trading.breakouts":
                    self.breakout_count += 1
                    self.recent_breakouts.appendleft({"time": now, **data})
                elif topic == "trading.averages":
                    self.avg_price_count += 1
                    self.recent_averages.appendleft({"time": now, **data})
                elif topic == "trading.large-trades":
                    self.large_trade_count += 1
                    self.recent_large_trades.appendleft({"time": now, **data})

        finally:
            consumer.close()

    def get_rate(self) -> float:
        now = time.time()
        # Count messages in last 10 seconds
        cutoff = now - 10
        recent = [t for t in self.rate_window if t > cutoff]
        return len(recent) / 10.0 if recent else 0.0

    def render(self) -> Panel:
        uptime = int(time.time() - self.start_time)
        rate = self.get_rate()

        layout = Layout()

        # Header
        header = Text()
        header.append(f"  Breakouts: {self.breakout_count:,}", style="bold green")
        header.append(f"   |   Avg Price Updates: {self.avg_price_count:,}", style="bold cyan")
        header.append(f"   |   Large Trades: {self.large_trade_count:,}", style="bold yellow")

        # Breakouts table
        breakout_table = Table(title="Recent Breakouts", expand=True, show_lines=False)
        breakout_table.add_column("Time", width=10)
        breakout_table.add_column("Symbol", width=8)
        breakout_table.add_column("Details")
        for b in list(self.recent_breakouts)[:6]:
            symbol = b.get("symbol", "?")
            start = b.get("start_price", 0)
            end = b.get("end_price", 0)
            change = ((end - start) / start * 100) if start else 0
            breakout_table.add_row(
                b["time"],
                str(symbol),
                f"${start:,.2f} -> ${end:,.2f} ({change:+.2f}%)",
            )

        # Large trades table
        trades_table = Table(title="Recent Large Trades", expand=True, show_lines=False)
        trades_table.add_column("Time", width=10)
        trades_table.add_column("Symbol", width=8)
        trades_table.add_column("Price", justify="right")
        trades_table.add_column("Volume", justify="right")
        for t in list(self.recent_large_trades)[:6]:
            trades_table.add_row(
                t["time"],
                str(t.get("symbol", "?")),
                f"${t.get('price', 0):,.2f}" if isinstance(t.get("price"), (int, float)) else "?",
                f"{t.get('volume', 0):,}" if isinstance(t.get("volume"), (int, float)) else "?",
            )

        # Footer
        footer = Text()
        footer.append(f"  Throughput: {rate:.1f} signals/s", style="bold")
        footer.append(f"   |   Total consumed: {self.total_consumed:,}")
        footer.append(f"   |   Uptime: {uptime}s")

        # Build layout
        layout.split_column(
            Layout(header, size=1),
            Layout(name="tables"),
            Layout(footer, size=1),
        )
        layout["tables"].split_row(
            Layout(breakout_table),
            Layout(trades_table),
        )

        return Panel(layout, title="Varpulis Financial CEP", border_style="blue")


def main():
    parser = argparse.ArgumentParser(description="E2E Consumer Dashboard")
    parser.add_argument("--kafka-brokers", default="localhost:19092", help="Kafka brokers")
    parser.add_argument("--group-id", default="e2e-consumer", help="Consumer group ID")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("  Varpulis E2E Consumer Dashboard")
    print(f"{'='*60}")
    print(f"  Kafka: {args.kafka_brokers}")
    print(f"  Topics: trading.breakouts, trading.averages, trading.large-trades")
    print(f"{'='*60}\n")

    dashboard = SignalDashboard(args.kafka_brokers, args.group_id)

    # Handle Ctrl+C
    def handle_sigint(sig, frame):
        dashboard.running = False

    signal.signal(signal.SIGINT, handle_sigint)

    # Start consumer thread
    consumer_thread = Thread(target=dashboard.consume_loop, daemon=True)
    consumer_thread.start()

    console = Console()
    try:
        with Live(dashboard.render(), console=console, refresh_per_second=2) as live:
            while dashboard.running:
                time.sleep(0.5)
                live.update(dashboard.render())
    except KeyboardInterrupt:
        pass
    finally:
        dashboard.running = False
        consumer_thread.join(timeout=2)
        print(f"\nTotal consumed: {dashboard.total_consumed:,} signals")


if __name__ == "__main__":
    main()
