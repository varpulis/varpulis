#!/usr/bin/env python3
"""
E2E Showcase Event Generator

Sends MarketTick and OHLCV events to MQTT for the financial markets CEP pipeline.
Adapted from demos/financial/generator.py with configurable rate and duration.
"""

import argparse
import json
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("ERROR: paho-mqtt not installed. Run: pip install paho-mqtt")
    sys.exit(1)


@dataclass
class Asset:
    symbol: str
    price: float
    volatility: float


class MarketGenerator:
    def __init__(self, client: mqtt.Client, rate: float = 10.0):
        self.client = client
        self.rate = rate
        self.event_count = 0
        self.assets = [
            Asset("BTC", 45000.0, 0.02),
            Asset("ETH", 2500.0, 0.025),
            Asset("SOL", 100.0, 0.03),
            Asset("AVAX", 35.0, 0.035),
            Asset("LINK", 15.0, 0.028),
        ]
        self.candle_counter = 0

    def publish(self, topic: str, data: dict):
        data["timestamp"] = datetime.now().isoformat()
        payload = json.dumps(data)
        self.client.publish(f"varpulis/events/{topic}", payload, qos=0)
        self.event_count += 1

    def update_price(self, asset: Asset):
        change_pct = random.gauss(0, asset.volatility)
        asset.price *= 1 + change_pct
        # Mean-revert: drift back toward initial price to prevent unrealistic values
        initial = {"BTC": 45000, "ETH": 2500, "SOL": 100, "AVAX": 35, "LINK": 15}
        target = initial.get(asset.symbol, asset.price)
        asset.price += (target - asset.price) * 0.001

    def generate_tick(self, asset: Asset):
        self.update_price(asset)
        spread = asset.price * 0.0005
        self.publish(
            "MarketTick",
            {
                "event_type": "MarketTick",
                "symbol": asset.symbol,
                "price": round(asset.price, 2),
                "bid": round(asset.price - spread / 2, 2),
                "ask": round(asset.price + spread / 2, 2),
                "volume": random.randint(100, 10000),
            },
        )

    def generate_candle(self, asset: Asset):
        open_price = asset.price
        changes = [random.gauss(0, asset.volatility / 2) for _ in range(4)]
        prices = [open_price * (1 + sum(changes[: i + 1])) for i in range(4)]
        self.publish(
            "OHLCV",
            {
                "event_type": "OHLCV",
                "symbol": asset.symbol,
                "open": round(open_price, 2),
                "high": round(max(prices) * 1.001, 2),
                "low": round(min(prices) * 0.999, 2),
                "close": round(prices[-1], 2),
                "volume": random.randint(1000, 100000),
            },
        )

    def run_cycle(self):
        for asset in self.assets:
            self.generate_tick(asset)

        self.candle_counter += 1
        if self.candle_counter >= 10:
            self.candle_counter = 0
            for asset in self.assets:
                self.generate_candle(asset)


def main():
    parser = argparse.ArgumentParser(description="E2E Market Event Generator")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=11883, help="MQTT broker port")
    parser.add_argument("--rate", type=float, default=10.0, help="Cycles per second")
    parser.add_argument("--duration", type=int, default=0, help="Duration in seconds (0=infinite)")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("  E2E Market Event Generator")
    print(f"{'='*60}")
    print(f"  Broker: {args.broker}:{args.port}")
    print(f"  Rate: {args.rate} cycles/sec ({args.rate * 5:.0f} ticks/sec)")
    if args.duration > 0:
        print(f"  Duration: {args.duration}s")
    print(f"{'='*60}\n")

    client = mqtt.Client(client_id=f"e2e-gen-{int(time.time())}")
    try:
        client.connect(args.broker, args.port, 60)
        client.loop_start()
    except Exception as e:
        print(f"ERROR: Cannot connect to MQTT at {args.broker}:{args.port}: {e}")
        sys.exit(1)

    gen = MarketGenerator(client, args.rate)
    start_time = time.time()
    interval = 1.0 / args.rate

    try:
        while True:
            cycle_start = time.time()
            gen.run_cycle()

            elapsed = time.time() - start_time
            eps = gen.event_count / elapsed if elapsed > 0 else 0
            btc = gen.assets[0]
            eth = gen.assets[1]
            print(
                f"\r[{elapsed:6.1f}s] BTC: ${btc.price:,.0f} | ETH: ${eth.price:,.0f} | "
                f"Events: {gen.event_count:,} ({eps:.0f}/s)",
                end="",
            )

            if args.duration > 0 and elapsed >= args.duration:
                break

            sleep_time = interval - (time.time() - cycle_start)
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n\nStopped by user")
    finally:
        client.loop_stop()
        client.disconnect()
        elapsed = time.time() - start_time
        eps = gen.event_count / elapsed if elapsed > 0 else 0
        print(f"\n\nTotal: {gen.event_count:,} events in {elapsed:.1f}s ({eps:.0f} events/s)")


if __name__ == "__main__":
    main()
