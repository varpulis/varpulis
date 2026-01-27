#!/usr/bin/env python3
"""
HVAC Demo Event Generator

Generates realistic HVAC sensor events for the Varpulis demo.
Includes normal operation, anomalies, and degradation patterns.
"""

import argparse
import json
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("ERROR: paho-mqtt not installed. Run: pip install paho-mqtt")
    sys.exit(1)


@dataclass
class Zone:
    name: str
    base_temp: float
    base_humidity: float
    setpoint: float
    temp: float = 0.0
    humidity: float = 0.0
    
    def __post_init__(self):
        self.temp = self.base_temp
        self.humidity = self.base_humidity


@dataclass
class HVACUnit:
    unit_id: str
    zone: str
    mode: str = "cooling"
    power_kw: float = 2.5
    compressor_health: float = 100.0
    fan_speed: int = 50
    refrigerant_level: float = 100.0
    runtime_hours: int = 0


class HVACScenario:
    """
    HVAC Demo Scenario
    
    Demonstrates:
    - Temperature monitoring across zones
    - Anomaly detection (temp spikes, humidity issues)
    - Equipment degradation patterns
    - Energy consumption correlation
    - Multi-zone comfort index
    """
    
    PATTERNS = """
    ┌─────────────────────────────────────────────────────────────────┐
    │  HVAC MONITORING PATTERNS                                       │
    ├─────────────────────────────────────────────────────────────────┤
    │                                                                 │
    │  Events:                                                        │
    │    • TemperatureReading  →  Temperatures stream                 │
    │    • HumidityReading     →  Humidity stream                     │
    │    • HVACStatus          →  HVAC stream                         │
    │    • EnergyMeter         →  Energy stream                       │
    │                                                                 │
    │  Stream Pipeline:                                               │
    │    Temperatures ─┬─→ ZoneTemperatures ─→ TemperatureAnomaly     │
    │                  │                                              │
    │                  └─→ ServerRoomAlert (zone == "server_room")    │
    │                                                                 │
    │    Humidity ────────→ ZoneHumidity ────→ HumidityAnomaly        │
    │                                                                 │
    │    ZoneTemp + ZoneHumidity ─→ ComfortIndex (JOIN)               │
    │                                                                 │
    │    HVAC ─→ HVACMetrics ─┬→ PowerSpike                           │
    │                         ├→ CompressorDegradation (SEQUENCE)     │
    │                         ├→ RefrigerantLeak (SEQUENCE)           │
    │                         └→ FanDegradation (SEQUENCE)            │
    │                                                                 │
    │  Alerts to detect:                                              │
    │    🔴 Temperature > 28°C or < 15°C                              │
    │    🔴 Server room > 25°C                                        │
    │    🟡 Humidity > 70% or < 30%                                   │
    │    🔴 Power spike > 5kW                                         │
    │    🟡 Compressor efficiency dropping                            │
    │    🔴 Refrigerant leak detected                                 │
    │                                                                 │
    └─────────────────────────────────────────────────────────────────┘
    """
    
    def __init__(self, mqtt_client: mqtt.Client, rate: float = 2.0):
        self.client = mqtt_client
        self.rate = rate
        self.event_count = 0
        self.alert_count = 0
        
        # Initialize zones
        self.zones = [
            Zone("Zone_A", 21.5, 45, 22),
            Zone("Zone_B", 22.0, 48, 22),
            Zone("Zone_C", 21.8, 46, 22),
            Zone("server_room", 20.0, 40, 18),
        ]
        
        # Initialize HVAC units
        self.hvac_units = [
            HVACUnit("HVAC-001", "Zone_A"),
            HVACUnit("HVAC-002", "Zone_B"),
            HVACUnit("HVAC-003", "Zone_C"),
            HVACUnit("HVAC-004", "server_room", power_kw=5.0),
        ]
        
        # Scenario state
        self.time_elapsed = 0
        self.anomaly_active = False
        self.degradation_phase = 0
        
    def publish(self, topic: str, data: dict):
        """Publish event to MQTT"""
        data["timestamp"] = datetime.now().isoformat()
        payload = json.dumps(data)
        self.client.publish(f"varpulis/events/{topic}", payload)
        self.event_count += 1
        
    def generate_temperature_event(self, zone: Zone):
        """Generate temperature reading for a zone"""
        # Normal variation
        variation = random.gauss(0, 0.3)
        zone.temp = zone.temp * 0.95 + (zone.base_temp + variation) * 0.05
        
        # Drift towards setpoint (HVAC control)
        if abs(zone.temp - zone.setpoint) > 0.5:
            zone.temp += (zone.setpoint - zone.temp) * 0.1
        
        # Apply anomaly if active - aggressive increase to trigger alerts
        if self.anomaly_active and zone.name == "Zone_A":
            zone.temp = max(zone.temp, 26) + random.uniform(0.8, 2.0)  # Force high temp
        
        self.publish("TemperatureReading", {
            "event_type": "TemperatureReading",
            "zone": zone.name,
            "temperature": round(zone.temp, 1),
            "setpoint": zone.setpoint,
            "unit": "celsius"
        })
        
        # Check for alert condition
        if zone.temp > 28 or zone.temp < 15:
            self.alert_count += 1
            return f"🔴 ALERT: {zone.name} temp {zone.temp:.1f}°C"
        elif zone.name == "server_room" and zone.temp > 25:
            self.alert_count += 1
            return f"🔴 ALERT: Server room temp {zone.temp:.1f}°C > 25°C"
        return None
        
    def generate_humidity_event(self, zone: Zone):
        """Generate humidity reading for a zone"""
        variation = random.gauss(0, 2)
        zone.humidity = max(20, min(80, zone.humidity + variation * 0.3))
        
        self.publish("HumidityReading", {
            "event_type": "HumidityReading",
            "zone": zone.name,
            "humidity": round(zone.humidity, 0),
            "unit": "percent"
        })
        
        if zone.humidity > 70 or zone.humidity < 30:
            self.alert_count += 1
            return f"🟡 WARNING: {zone.name} humidity {zone.humidity:.0f}%"
        return None
        
    def generate_hvac_status(self, unit: HVACUnit):
        """Generate HVAC unit status"""
        # Simulate degradation - aggressive to trigger alerts quickly
        if self.degradation_phase > 0:
            unit.compressor_health = max(50, unit.compressor_health - 3.0)  # Faster degradation
            unit.refrigerant_level = max(70, unit.refrigerant_level - 2.0)  # Faster leak
        
        # Power varies with load
        load_factor = 0.8 + random.uniform(-0.2, 0.2)
        current_power = unit.power_kw * load_factor
        
        # Power spike during anomaly
        if self.anomaly_active and unit.zone == "Zone_A":
            current_power *= 1.5
        
        unit.runtime_hours += 1
        
        self.publish("HVACStatus", {
            "event_type": "HVACStatus",
            "unit_id": unit.unit_id,
            "zone": unit.zone,
            "mode": unit.mode,
            "power_kw": round(current_power, 2),
            "compressor_health": round(unit.compressor_health, 1),
            "fan_speed": unit.fan_speed,
            "refrigerant_level": round(unit.refrigerant_level, 1),
            "runtime_hours": unit.runtime_hours
        })
        
        alerts = []
        if current_power > 5:
            self.alert_count += 1
            alerts.append(f"🔴 ALERT: {unit.unit_id} power spike {current_power:.1f}kW")
        if unit.compressor_health < 80:
            alerts.append(f"🟡 WARNING: {unit.unit_id} compressor health {unit.compressor_health:.0f}%")
        if unit.refrigerant_level < 85:
            alerts.append(f"🟡 WARNING: {unit.unit_id} refrigerant {unit.refrigerant_level:.0f}%")
        return alerts
        
    def generate_energy_event(self):
        """Generate energy meter reading"""
        total_power = sum(u.power_kw for u in self.hvac_units)
        variation = random.uniform(-0.5, 0.5)
        
        self.publish("EnergyMeter", {
            "event_type": "EnergyMeter",
            "meter_id": "MAIN-001",
            "power_kw": round(total_power + variation, 2),
            "voltage": round(230 + random.uniform(-5, 5), 1),
            "current": round((total_power * 1000) / 230, 1),
            "power_factor": round(0.95 + random.uniform(-0.05, 0.02), 2)
        })
        
    def run_cycle(self) -> list:
        """Run one cycle of event generation"""
        alerts = []
        
        # Temperature for each zone
        for zone in self.zones:
            alert = self.generate_temperature_event(zone)
            if alert:
                alerts.append(alert)
                
        # Humidity (less frequent)
        if self.time_elapsed % 3 == 0:
            for zone in self.zones:
                alert = self.generate_humidity_event(zone)
                if alert:
                    alerts.append(alert)
                    
        # HVAC status (less frequent)
        if self.time_elapsed % 5 == 0:
            for unit in self.hvac_units:
                unit_alerts = self.generate_hvac_status(unit)
                alerts.extend(unit_alerts)
                
        # Energy meter
        if self.time_elapsed % 2 == 0:
            self.generate_energy_event()
            
        # Scenario progression
        self.time_elapsed += 1
        
        # Trigger anomaly at t=30s
        if self.time_elapsed == 30:
            self.anomaly_active = True
            alerts.append("⚡ SCENARIO: Temperature anomaly starting in Zone_A")
            
        # End anomaly at t=50s
        if self.time_elapsed == 50:
            self.anomaly_active = False
            alerts.append("✓ SCENARIO: Temperature anomaly resolved")
            
        # Start degradation at t=60s
        if self.time_elapsed == 60:
            self.degradation_phase = 1
            alerts.append("⚡ SCENARIO: Equipment degradation starting")
            
        return alerts


def main():
    parser = argparse.ArgumentParser(description="HVAC Demo Event Generator")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--rate", type=float, default=2.0, help="Events per second")
    parser.add_argument("--duration", type=int, default=0, help="Duration in seconds (0=infinite)")
    args = parser.parse_args()
    
    # Print scenario info
    print(HVACScenario.PATTERNS)
    print(f"\n{'='*65}")
    print(f"  Starting HVAC event generation")
    print(f"  Broker: {args.broker}:{args.port}")
    print(f"  Rate: {args.rate} events/sec")
    print(f"  Duration: {'infinite' if args.duration == 0 else f'{args.duration}s'}")
    print(f"{'='*65}\n")
    
    # Connect to MQTT
    client = mqtt.Client(client_id=f"hvac-generator-{int(time.time())}")
    try:
        client.connect(args.broker, args.port, 60)
        client.loop_start()
    except Exception as e:
        print(f"ERROR: Cannot connect to MQTT broker: {e}")
        sys.exit(1)
    
    scenario = HVACScenario(client, args.rate)
    start_time = time.time()
    interval = 1.0 / args.rate
    
    try:
        while True:
            cycle_start = time.time()
            
            # Run generation cycle
            alerts = scenario.run_cycle()
            
            # Print status
            elapsed = int(time.time() - start_time)
            print(f"\r[{elapsed:4d}s] Events: {scenario.event_count:5d} | Alerts: {scenario.alert_count:3d}", end="")
            
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
        print(f"Total alerts: {scenario.alert_count}")


if __name__ == "__main__":
    main()
