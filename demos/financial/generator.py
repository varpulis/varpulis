#!/usr/bin/env python3
"""
Financial Markets Demo Event Generator

Generates realistic market data for technical analysis patterns.
Includes price movements, MACD crossovers, and trading signals.
"""

import argparse
import json
import math
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("ERROR: paho-mqtt not installed. Run: pip install paho-mqtt")
    sys.exit(1)


@dataclass
class Asset:
    symbol: str
    price: float
    base_price: float
    volatility: float
    trend: float = 0.0  # -1 to 1
    
    # Technical indicators
    sma_20: float = 0.0
    sma_50: float = 0.0
    ema_12: float = 0.0
    ema_26: float = 0.0
    rsi: float = 50.0
    
    # Price history for calculations
    price_history: List[float] = None
    
    def __post_init__(self):
        self.price_history = [self.price] * 50


class FinancialScenario:
    """
    Financial Markets Demo Scenario
    
    Demonstrates:
    - Real-time price streaming
    - Moving averages (SMA, EMA)
    - Technical indicators (RSI, MACD, Bollinger Bands)
    - Signal detection (Golden Cross, Death Cross)
    - Pattern recognition
    """
    
    PATTERNS = """
    ┌─────────────────────────────────────────────────────────────────┐
    │  FINANCIAL MARKETS PATTERNS                                     │
    ├─────────────────────────────────────────────────────────────────┤
    │                                                                 │
    │  Events:                                                        │
    │    • MarketTick  →  Ticks stream (raw price updates)            │
    │    • OHLCV       →  Candles stream (candlestick data)           │
    │                                                                 │
    │  Stream Pipeline:                                               │
    │                                                                 │
    │    Ticks ─┬→ BTCTicks (filter symbol=="BTC")                    │
    │           └→ ETHTicks (filter symbol=="ETH")                    │
    │                                                                 │
    │    Candles ─┬→ SMA20 (avg close over 20 periods)                │
    │             ├→ SMA50 (avg close over 50 periods)                │
    │             ├→ EMA12 (exp avg for MACD)                         │
    │             ├→ EMA26 (exp avg for MACD)                         │
    │             ├→ RSI (relative strength index)                    │
    │             └→ BollingerBands (std dev bands)                   │
    │                                                                 │
    │    EMA12 + EMA26 ─→ MACD (JOIN) ─→ MACDSignal                   │
    │                                                                 │
    │    SMA20 + SMA50 + RSI + BB + MACD ─→ TechnicalAnalysis (JOIN)  │
    │                                                                 │
    │  Trading Signals:                                               │
    │    🟢 GoldenCross: SMA20 crosses above SMA50 (bullish)          │
    │    🔴 DeathCross: SMA20 crosses below SMA50 (bearish)           │
    │    🟢 RSIOversold: RSI < 30 (potential buy)                     │
    │    🔴 RSIOverbought: RSI > 70 (potential sell)                  │
    │    🟡 BBSqueeze: Bands narrowing (volatility incoming)          │
    │    🟢 BBBreakoutUp: Price breaks upper band                     │
    │    🔴 BBBreakoutDown: Price breaks lower band                   │
    │    🟢 MACDBullish: MACD crosses above signal line               │
    │    🔴 MACDBearish: MACD crosses below signal line               │
    │                                                                 │
    └─────────────────────────────────────────────────────────────────┘
    """
    
    def __init__(self, mqtt_client: mqtt.Client, rate: float = 2.0):
        self.client = mqtt_client
        self.rate = rate
        self.event_count = 0
        self.signal_count = 0
        
        # Initialize assets
        self.assets = [
            Asset("BTC", 45000.0, 45000.0, 0.02),
            Asset("ETH", 2500.0, 2500.0, 0.025),
            Asset("SOL", 100.0, 100.0, 0.03),
        ]
        
        # Scenario state
        self.time_elapsed = 0
        self.market_phase = "ranging"  # ranging, bullish, bearish
        self.candle_count = 0
        
        # Previous indicator values for crossover detection
        self.prev_sma_20 = {a.symbol: a.price for a in self.assets}
        self.prev_sma_50 = {a.symbol: a.price for a in self.assets}
        self.prev_macd = {a.symbol: 0.0 for a in self.assets}
        
    def publish(self, topic: str, data: dict):
        """Publish event to MQTT"""
        data["timestamp"] = datetime.now().isoformat()
        payload = json.dumps(data)
        self.client.publish(f"varpulis/events/{topic}", payload)
        self.event_count += 1
        
    def update_price(self, asset: Asset):
        """Update asset price with realistic movement"""
        # Base random walk
        change_pct = random.gauss(0, asset.volatility)
        
        # Add trend bias
        if self.market_phase == "bullish":
            change_pct += 0.001
        elif self.market_phase == "bearish":
            change_pct -= 0.001
            
        # Mean reversion
        deviation = (asset.price - asset.base_price) / asset.base_price
        change_pct -= deviation * 0.01
        
        # Apply change
        asset.price *= (1 + change_pct)
        
        # Update history
        asset.price_history.append(asset.price)
        if len(asset.price_history) > 100:
            asset.price_history.pop(0)
            
        # Update indicators
        self.update_indicators(asset)
        
    def update_indicators(self, asset: Asset):
        """Calculate technical indicators"""
        prices = asset.price_history
        
        # SMA 20 and 50
        if len(prices) >= 20:
            asset.sma_20 = sum(prices[-20:]) / 20
        if len(prices) >= 50:
            asset.sma_50 = sum(prices[-50:]) / 50
            
        # EMA 12 and 26 (simplified)
        alpha_12 = 2 / (12 + 1)
        alpha_26 = 2 / (26 + 1)
        asset.ema_12 = asset.price * alpha_12 + asset.ema_12 * (1 - alpha_12)
        asset.ema_26 = asset.price * alpha_26 + asset.ema_26 * (1 - alpha_26)
        
        # RSI (simplified)
        if len(prices) >= 14:
            gains = []
            losses = []
            for i in range(-14, 0):
                diff = prices[i] - prices[i-1]
                if diff > 0:
                    gains.append(diff)
                else:
                    losses.append(abs(diff))
            avg_gain = max(sum(gains) / 14, 0.0001) if gains else 0.0001
            avg_loss = max(sum(losses) / 14, 0.0001) if losses else 0.0001
            rs = avg_gain / avg_loss
            asset.rsi = 100 - (100 / (1 + rs))
            
    def generate_tick(self, asset: Asset) -> List[str]:
        """Generate market tick event"""
        alerts = []
        
        self.update_price(asset)
        
        # Bid/ask spread
        spread = asset.price * 0.0005
        bid = asset.price - spread / 2
        ask = asset.price + spread / 2
        
        self.publish("MarketTick", {
            "event_type": "MarketTick",
            "symbol": asset.symbol,
            "price": round(asset.price, 2),
            "bid": round(bid, 2),
            "ask": round(ask, 2),
            "volume": random.randint(100, 10000),
            "exchange": "DEMO"
        })
        
        return alerts
        
    def generate_candle(self, asset: Asset) -> List[str]:
        """Generate OHLCV candle event - RAW data only, no indicators
        
        Indicators (SMA, RSI, MACD, Bollinger) are calculated by Varpulis CEP engine
        using the rules defined in financial_markets.vpl
        """
        alerts = []
        
        # Simulate candle from recent prices
        recent = asset.price_history[-5:] if len(asset.price_history) >= 5 else asset.price_history
        open_price = recent[0]
        close_price = recent[-1]
        high_price = max(recent) * (1 + random.uniform(0, 0.005))
        low_price = min(recent) * (1 - random.uniform(0, 0.005))
        volume = random.randint(1000, 100000)
        
        # Publish RAW OHLCV data only - Varpulis calculates indicators
        self.publish("OHLCV", {
            "event_type": "OHLCV",
            "symbol": asset.symbol,
            "timeframe": "1m",
            "open": round(open_price, 2),
            "high": round(high_price, 2),
            "low": round(low_price, 2),
            "close": round(close_price, 2),
            "volume": volume,
            "ts": datetime.now().isoformat()
        })
        
        # NOTE: Signal detection (Golden Cross, Death Cross, RSI, MACD, Bollinger)
        # is now handled by Varpulis CEP engine using rules in financial_markets.vpl
        # The generator only produces RAW market data events.
        
        return alerts
        
    def generate_orderbook(self, asset: Asset):
        """Generate order book snapshot"""
        spread = asset.price * 0.001
        mid = asset.price
        
        bids = [(mid - spread * (i+1), random.randint(10, 500)) for i in range(5)]
        asks = [(mid + spread * (i+1), random.randint(10, 500)) for i in range(5)]
        
        self.publish("OrderBook", {
            "event_type": "OrderBook",
            "symbol": asset.symbol,
            "bids": [[round(p, 2), q] for p, q in bids],
            "asks": [[round(p, 2), q] for p, q in asks],
            "spread": round(spread * 2, 4),
            "depth": sum(q for _, q in bids) + sum(q for _, q in asks)
        })
        
    def generate_news(self):
        """Generate market news event"""
        headlines = [
            ("BTC", "Bitcoin ETF sees record inflows", "positive"),
            ("ETH", "Ethereum upgrade scheduled for next month", "positive"),
            ("BTC", "Regulatory concerns in Asia markets", "negative"),
            ("SOL", "Solana network congestion reported", "negative"),
            ("ETH", "Major DeFi protocol launches on Ethereum", "positive"),
            ("BTC", "Institutional adoption continues to grow", "positive"),
        ]
        symbol, headline, sentiment = random.choice(headlines)
        
        self.publish("NewsEvent", {
            "event_type": "NewsEvent",
            "symbol": symbol,
            "headline": headline,
            "sentiment": sentiment,
            "source": random.choice(["Reuters", "Bloomberg", "CoinDesk", "CryptoNews"]),
            "impact": random.choice(["low", "medium", "high"])
        })
        
    def run_cycle(self) -> List[str]:
        """Run one cycle of event generation"""
        alerts = []
        
        # Generate ticks for all assets
        for asset in self.assets:
            tick_alerts = self.generate_tick(asset)
            alerts.extend(tick_alerts)
            
        # Generate candles less frequently
        self.candle_count += 1
        if self.candle_count >= 5:
            self.candle_count = 0
            for asset in self.assets:
                candle_alerts = self.generate_candle(asset)
                alerts.extend(candle_alerts)
                
        # Generate order book every 3 cycles
        if self.time_elapsed % 3 == 0:
            for asset in self.assets:
                self.generate_orderbook(asset)
                
        # Generate news randomly (about 10% chance per cycle)
        if random.random() < 0.1:
            self.generate_news()
                
        # Scenario progression
        self.time_elapsed += 1
        
        # Market phase changes
        if self.time_elapsed == 30:
            self.market_phase = "bullish"
            alerts.append("⚡ MARKET: Entering bullish phase")
        elif self.time_elapsed == 60:
            self.market_phase = "bearish"
            alerts.append("⚡ MARKET: Entering bearish phase")
        elif self.time_elapsed == 90:
            self.market_phase = "ranging"
            alerts.append("⚡ MARKET: Returning to ranging")
            
        return alerts


def main():
    parser = argparse.ArgumentParser(description="Financial Markets Demo Event Generator")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--rate", type=float, default=2.0, help="Events per second")
    parser.add_argument("--duration", type=int, default=0, help="Duration in seconds (0=infinite)")
    args = parser.parse_args()
    
    # Print scenario info
    print(FinancialScenario.PATTERNS)
    print(f"\n{'='*65}")
    print(f"  Starting Financial Markets event generation")
    print(f"  Broker: {args.broker}:{args.port}")
    print(f"  Rate: {args.rate} events/sec")
    print(f"  Duration: {'infinite' if args.duration == 0 else f'{args.duration}s'}")
    print(f"{'='*65}\n")
    
    # Connect to MQTT
    client = mqtt.Client(client_id=f"financial-generator-{int(time.time())}")
    try:
        client.connect(args.broker, args.port, 60)
        client.loop_start()
    except Exception as e:
        print(f"ERROR: Cannot connect to MQTT broker: {e}")
        sys.exit(1)
    
    scenario = FinancialScenario(client, args.rate)
    start_time = time.time()
    interval = 1.0 / args.rate
    
    try:
        while True:
            cycle_start = time.time()
            
            # Run generation cycle
            alerts = scenario.run_cycle()
            
            # Print status with prices
            elapsed = int(time.time() - start_time)
            btc = scenario.assets[0]
            eth = scenario.assets[1]
            print(f"\r[{elapsed:4d}s] BTC: ${btc.price:,.0f} | ETH: ${eth.price:,.0f} | Signals: {scenario.signal_count:3d}", end="")
            
            # Print any alerts
            for alert in alerts:
                print(f"\n  {alert}")
                
            # Check duration
            if args.duration > 0 and elapsed >= args.duration:
                print(f"\n\n✓ Demo completed after {args.duration}s")
                break
                
            # Wait for next cycle
            sleep_time = interval - (time.time() - cycle_start)
            if sleep_time > 0:
                time.sleep(sleep_time)
                
    except KeyboardInterrupt:
        print("\n\n✓ Demo stopped by user")
    finally:
        client.loop_stop()
        client.disconnect()
        print(f"\nTotal events: {scenario.event_count}")
        print(f"Total signals: {scenario.signal_count}")


if __name__ == "__main__":
    main()
