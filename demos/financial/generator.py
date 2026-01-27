#!/usr/bin/env python3
"""
Financial Markets Demo Event Generator

Generates RAW market data only. All technical indicators (SMA, RSI, MACD, 
Bollinger Bands) are calculated by the Varpulis CEP engine.
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
    """Simple asset with price - no indicator calculations"""
    symbol: str
    price: float
    volatility: float


class FinancialGenerator:
    """
    RAW Market Data Generator
    
    Emits ONLY raw market events:
    - MarketTick: Real-time price updates
    - OHLCV: Candlestick data
    
    All technical analysis (SMA, RSI, MACD, Bollinger Bands) 
    is performed by the Varpulis CEP engine.
    """
    
    def __init__(self, mqtt_client: mqtt.Client, rate: float = 2.0):
        self.client = mqtt_client
        self.rate = rate
        self.event_count = 0
        
        # Simple assets - just price and volatility
        self.assets = [
            Asset("BTC", 45000.0, 0.02),
            Asset("ETH", 2500.0, 0.025),
            Asset("SOL", 100.0, 0.03),
        ]
        self.candle_count = 0
        
    def publish(self, topic: str, data: dict):
        """Publish event to MQTT"""
        data["timestamp"] = datetime.now().isoformat()
        payload = json.dumps(data)
        self.client.publish(f"varpulis/events/{topic}", payload)
        self.event_count += 1
        
    def update_price(self, asset: Asset):
        """Simple random walk price update"""
        change_pct = random.gauss(0, asset.volatility)
        asset.price *= (1 + change_pct)
        
    def generate_tick(self, asset: Asset):
        """Generate raw market tick"""
        self.update_price(asset)
        
        spread = asset.price * 0.0005
        self.publish("MarketTick", {
            "event_type": "MarketTick",
            "symbol": asset.symbol,
            "price": round(asset.price, 2),
            "bid": round(asset.price - spread/2, 2),
            "ask": round(asset.price + spread/2, 2),
            "volume": random.randint(100, 10000),
        })
        
    def generate_candle(self, asset: Asset):
        """Generate raw OHLCV candle"""
        # Simple candle simulation
        open_price = asset.price
        changes = [random.gauss(0, asset.volatility/2) for _ in range(4)]
        prices = [open_price * (1 + sum(changes[:i+1])) for i in range(4)]
        
        self.publish("OHLCV", {
            "event_type": "OHLCV",
            "symbol": asset.symbol,
            "timeframe": "1m",
            "open": round(open_price, 2),
            "high": round(max(prices) * 1.001, 2),
            "low": round(min(prices) * 0.999, 2),
            "close": round(prices[-1], 2),
            "volume": random.randint(1000, 100000),
            "ts": datetime.now().isoformat()
        })
        
    def run_cycle(self):
        """Generate one cycle of raw events"""
        for asset in self.assets:
            self.generate_tick(asset)
            
        self.candle_count += 1
        if self.candle_count >= 5:
            self.candle_count = 0
            for asset in self.assets:
                self.generate_candle(asset)


def main():
    parser = argparse.ArgumentParser(description="Financial Markets RAW Event Generator")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--rate", type=float, default=2.0, help="Events per second")
    parser.add_argument("--duration", type=int, default=0, help="Duration in seconds (0=infinite)")
    args = parser.parse_args()
    
    print(f"\n{'='*60}")
    print("  Financial Markets RAW Event Generator")
    print("  Emits: MarketTick, OHLCV (raw data only)")
    print("  Indicators calculated by Varpulis CEP engine")
    print(f"{'='*60}")
    print(f"  Broker: {args.broker}:{args.port}")
    print(f"  Rate: {args.rate} events/sec")
    print(f"{'='*60}\n")
    
    client = mqtt.Client(client_id=f"financial-gen-{int(time.time())}")
    try:
        client.connect(args.broker, args.port, 60)
        client.loop_start()
    except Exception as e:
        print(f"ERROR: Cannot connect to MQTT: {e}")
        sys.exit(1)
    
    gen = FinancialGenerator(client, args.rate)
    start_time = time.time()
    interval = 1.0 / args.rate
    
    try:
        while True:
            cycle_start = time.time()
            gen.run_cycle()
            
            elapsed = int(time.time() - start_time)
            btc, eth = gen.assets[0], gen.assets[1]
            print(f"\r[{elapsed:4d}s] BTC: ${btc.price:,.0f} | ETH: ${eth.price:,.0f} | Events: {gen.event_count}", end="")
            
            if args.duration > 0 and elapsed >= args.duration:
                break
                
            sleep_time = interval - (time.time() - cycle_start)
            if sleep_time > 0:
                time.sleep(sleep_time)
                
    except KeyboardInterrupt:
        print("\n\n✓ Stopped")
    finally:
        client.loop_stop()
        client.disconnect()
        print(f"\nTotal events: {gen.event_count}")


if __name__ == "__main__":
    main()
