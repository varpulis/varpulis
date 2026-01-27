#!/usr/bin/env python3
"""
SASE Security Demo Event Generator

Generates realistic security events for fraud detection patterns.
Includes login sequences, transactions, and attack patterns.
"""

import argparse
import json
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
class User:
    user_id: str
    name: str
    usual_ip: str
    usual_location: str
    risk_score: float = 0.0
    logged_in: bool = False
    last_login_time: Optional[float] = None


class SASEScenario:
    """
    SASE Security Demo Scenario
    
    Demonstrates:
    - SASE patterns (Sequence, Any, Skip-Till-Next-Match, Contiguity)
    - Login → Transaction → Logout sequences
    - Fraud detection (impossible travel, velocity attacks)
    - Multi-user correlation
    """
    
    PATTERNS = """
    ┌─────────────────────────────────────────────────────────────────┐
    │  SASE SECURITY PATTERNS                                         │
    ├─────────────────────────────────────────────────────────────────┤
    │                                                                 │
    │  Events:                                                        │
    │    • Login        →  Logins stream                              │
    │    • Transaction  →  Transactions stream                        │
    │    • Logout       →  Logouts stream                             │
    │    • PasswordChange → PasswordChanges stream                    │
    │                                                                 │
    │  SASE Patterns:                                                 │
    │                                                                 │
    │  1. LoginThenLargeTransaction (SEQUENCE strict)                 │
    │     Login(user) → Transaction(user, amount > 10000)             │
    │     within 5 minutes                                            │
    │                                                                 │
    │  2. FullSession (SEQUENCE relaxed)                              │
    │     Login → Transaction* → Logout                               │
    │     Same user, any number of transactions                       │
    │                                                                 │
    │  3. FraudChain (SEQUENCE with skip-till-next-match)             │
    │     Login(new_device) → PasswordChange → LargeTransaction       │
    │     Classic account takeover pattern                            │
    │                                                                 │
    │  4. ImpossibleTravel (SEQUENCE with condition)                  │
    │     Login(loc1) → Login(loc2)                                   │
    │     where distance(loc1, loc2) / time_diff > 500 km/h           │
    │                                                                 │
    │  5. VelocityAlert (AGGREGATE)                                   │
    │     count(Transaction) > 5 within 1 minute per user             │
    │                                                                 │
    │  6. SameIPMultipleUsers (AGGREGATE)                             │
    │     count(distinct user) > 3 from same IP within 5 min          │
    │                                                                 │
    │  Alerts to detect:                                              │
    │    🔴 Large transaction after login (> $10,000)                 │
    │    🔴 Impossible travel detected                                │
    │    🔴 Fraud chain (new device + password + large tx)            │
    │    🟡 High transaction velocity                                 │
    │    🟡 Multiple users from same IP                               │
    │                                                                 │
    └─────────────────────────────────────────────────────────────────┘
    """
    
    LOCATIONS = [
        ("Paris", "FR", "192.168.1."),
        ("New York", "US", "10.0.1."),
        ("Tokyo", "JP", "172.16.1."),
        ("London", "UK", "192.168.2."),
        ("Sydney", "AU", "10.0.2."),
    ]
    
    def __init__(self, mqtt_client: mqtt.Client, rate: float = 2.0):
        self.client = mqtt_client
        self.rate = rate
        self.event_count = 0
        self.alert_count = 0
        self.pattern_matches = 0
        
        # Initialize users
        self.users = [
            User("user-001", "Alice", "192.168.1.100", "Paris"),
            User("user-002", "Bob", "10.0.1.50", "New York"),
            User("user-003", "Charlie", "172.16.1.25", "Tokyo"),
            User("user-004", "Diana", "192.168.2.75", "London"),
            User("user-005", "Eve", "10.0.2.30", "Sydney"),  # Attacker
        ]
        
        # Scenario state
        self.time_elapsed = 0
        self.attack_phase = 0
        self.pending_transactions = []
        
    def publish(self, topic: str, data: dict):
        """Publish event to MQTT"""
        data["timestamp"] = datetime.now().isoformat()
        payload = json.dumps(data)
        self.client.publish(f"varpulis/events/{topic}", payload)
        self.event_count += 1
        
    def generate_login(self, user: User, ip: Optional[str] = None, 
                       location: Optional[str] = None, new_device: bool = False):
        """Generate login event"""
        ip = ip or user.usual_ip + str(random.randint(1, 254))
        location = location or user.usual_location
        
        self.publish("Login", {
            "event_type": "Login",
            "user_id": user.user_id,
            "username": user.name,
            "ip_address": ip,
            "location": location,
            "device_fingerprint": "new-device-xyz" if new_device else f"device-{user.user_id}",
            "success": True
        })
        
        user.logged_in = True
        user.last_login_time = time.time()
        return f"🔵 Login: {user.name} from {location} ({ip})"
        
    def generate_transaction(self, user: User, amount: float, 
                            merchant: str = "Amazon"):
        """Generate transaction event"""
        self.publish("Transaction", {
            "event_type": "Transaction",
            "user_id": user.user_id,
            "transaction_id": f"tx-{int(time.time()*1000)}",
            "amount": amount,
            "currency": "USD",
            "merchant": merchant,
            "category": "online" if amount < 1000 else "high_value"
        })
        
        alert = None
        if amount > 10000:
            self.alert_count += 1
            alert = f"🔴 ALERT: Large transaction ${amount:.2f} by {user.name}"
        return f"💳 Transaction: {user.name} → ${amount:.2f} at {merchant}", alert
        
    def generate_logout(self, user: User):
        """Generate logout event"""
        self.publish("Logout", {
            "event_type": "Logout",
            "user_id": user.user_id,
            "session_duration": int(time.time() - (user.last_login_time or time.time()))
        })
        user.logged_in = False
        return f"🔵 Logout: {user.name}"
        
    def generate_password_change(self, user: User):
        """Generate password change event"""
        self.publish("PasswordChange", {
            "event_type": "PasswordChange",
            "user_id": user.user_id,
            "username": user.name,
            "change_type": "reset"
        })
        return f"🔑 Password change: {user.name}"
        
    def run_normal_activity(self) -> List[str]:
        """Generate normal user activity"""
        alerts = []
        
        # Random user does something
        user = random.choice(self.users[:4])  # Exclude Eve (attacker)
        
        if not user.logged_in and random.random() < 0.3:
            # Login
            alerts.append(self.generate_login(user))
        elif user.logged_in:
            if random.random() < 0.6:
                # Normal transaction
                amount = random.uniform(10, 500)
                msg, alert = self.generate_transaction(user, amount)
                alerts.append(msg)
                if alert:
                    alerts.append(alert)
            elif random.random() < 0.2:
                # Logout
                alerts.append(self.generate_logout(user))
                
        return alerts
        
    def run_attack_sequence(self) -> List[str]:
        """Generate attack sequence - fraud chain"""
        alerts = []
        attacker = self.users[4]  # Eve
        victim = self.users[0]    # Alice
        
        if self.attack_phase == 1:
            # Phase 1: Attacker logs in from unusual location with new device
            alerts.append("⚡ ATTACK SEQUENCE STARTING: Account Takeover")
            alerts.append(self.generate_login(
                victim, 
                ip="45.33.32.156",  # Different IP
                location="Moscow",  # Different location
                new_device=True
            ))
            self.attack_phase = 2
            
        elif self.attack_phase == 2:
            # Phase 2: Change password
            alerts.append(self.generate_password_change(victim))
            self.attack_phase = 3
            
        elif self.attack_phase == 3:
            # Phase 3: Large transaction
            msg, alert = self.generate_transaction(victim, 15000, "CryptoExchange")
            alerts.append(msg)
            if alert:
                alerts.append(alert)
            self.alert_count += 1
            self.pattern_matches += 1
            alerts.append("🔴 PATTERN MATCH: FraudChain detected!")
            self.attack_phase = 4
            
        elif self.attack_phase == 4:
            # Phase 4: Impossible travel - Alice logs in from original location
            alerts.append("⚡ ATTACK: Impossible Travel Test")
            alerts.append(self.generate_login(victim))  # Back to Paris
            self.alert_count += 1
            self.pattern_matches += 1
            alerts.append("🔴 PATTERN MATCH: ImpossibleTravel detected!")
            self.attack_phase = 5
            
        elif self.attack_phase == 5:
            # Phase 5: Velocity attack
            alerts.append("⚡ ATTACK: Velocity Attack")
            for i in range(6):
                msg, _ = self.generate_transaction(victim, random.uniform(100, 500))
                alerts.append(msg)
            self.alert_count += 1
            self.pattern_matches += 1
            alerts.append("🟡 PATTERN MATCH: VelocityAlert detected!")
            self.attack_phase = 0  # Reset
            
        return alerts
        
    def run_cycle(self) -> List[str]:
        """Run one cycle of event generation"""
        alerts = []
        
        # Normal activity
        alerts.extend(self.run_normal_activity())
        
        # Scenario progression
        self.time_elapsed += 1
        
        # Start attack at t=20s
        if self.time_elapsed == 20 and self.attack_phase == 0:
            self.attack_phase = 1
            
        # Run attack phases
        if self.attack_phase > 0 and self.time_elapsed % 3 == 0:
            alerts.extend(self.run_attack_sequence())
            
        return alerts


def main():
    parser = argparse.ArgumentParser(description="SASE Security Demo Event Generator")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--rate", type=float, default=2.0, help="Events per second")
    parser.add_argument("--duration", type=int, default=0, help="Duration in seconds (0=infinite)")
    args = parser.parse_args()
    
    # Print scenario info
    print(SASEScenario.PATTERNS)
    print(f"\n{'='*65}")
    print(f"  Starting SASE Security event generation")
    print(f"  Broker: {args.broker}:{args.port}")
    print(f"  Rate: {args.rate} events/sec")
    print(f"  Duration: {'infinite' if args.duration == 0 else f'{args.duration}s'}")
    print(f"{'='*65}\n")
    
    # Connect to MQTT
    client = mqtt.Client(client_id=f"sase-generator-{int(time.time())}")
    try:
        client.connect(args.broker, args.port, 60)
        client.loop_start()
    except Exception as e:
        print(f"ERROR: Cannot connect to MQTT broker: {e}")
        sys.exit(1)
    
    scenario = SASEScenario(client, args.rate)
    start_time = time.time()
    interval = 1.0 / args.rate
    
    try:
        while True:
            cycle_start = time.time()
            
            # Run generation cycle
            alerts = scenario.run_cycle()
            
            # Print status
            elapsed = int(time.time() - start_time)
            print(f"\r[{elapsed:4d}s] Events: {scenario.event_count:5d} | Patterns: {scenario.pattern_matches:3d}", end="")
            
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
        print(f"Total pattern matches: {scenario.pattern_matches}")


if __name__ == "__main__":
    main()
