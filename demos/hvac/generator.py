#!/usr/bin/env python3
"""
HVAC Demo RAW Event Generator

Generates RAW sensor data only. All anomaly detection, pattern recognition,
and alerting is performed by the Varpulis CEP engine.
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
class Zone:
    """Simple zone with current readings"""
    name: str
    temp: float
    humidity: float
    setpoint: float


@dataclass  
class HVACUnit:
    """Simple HVAC unit state"""
    unit_id: str
    zone: str
    power_kw: float
    compressor_health: float = 100.0
    refrigerant_level: float = 100.0


class HVACGenerator:
    """
    RAW HVAC Sensor Data Generator
    
    Emits ONLY raw sensor events:
    - TemperatureReading
    - HumidityReading  
    - HVACStatus
    - EnergyMeter
    
    All anomaly detection and alerting is done by Varpulis CEP engine.
    """
    
    def __init__(self, mqtt_client: mqtt.Client):
        self.client = mqtt_client
        self.event_count = 0
        self.cycle = 0
        
        self.zones = [
            Zone("Zone_A", 21.5, 45, 22),
            Zone("Zone_B", 22.0, 48, 22),
            Zone("Zone_C", 21.8, 46, 22),
            Zone("server_room", 20.0, 40, 18),
        ]
        
        self.hvac_units = [
            HVACUnit("HVAC-001", "Zone_A", 2.5),
            HVACUnit("HVAC-002", "Zone_B", 2.5),
            HVACUnit("HVAC-003", "Zone_C", 2.5),
            HVACUnit("HVAC-004", "server_room", 5.0),
        ]
        
    def publish(self, topic: str, data: dict):
        data["timestamp"] = datetime.now().isoformat()
        self.client.publish(f"varpulis/events/{topic}", json.dumps(data))
        self.event_count += 1
        
    def generate_temperature(self, zone: Zone):
        """Emit raw temperature reading"""
        zone.temp += random.gauss(0, 0.5)
        self.publish("TemperatureReading", {
            "event_type": "TemperatureReading",
            "zone": zone.name,
            "temperature": round(zone.temp, 1),
            "setpoint": zone.setpoint,
            "unit": "celsius"
        })
        
    def generate_humidity(self, zone: Zone):
        """Emit raw humidity reading"""
        zone.humidity = max(20, min(80, zone.humidity + random.gauss(0, 2)))
        self.publish("HumidityReading", {
            "event_type": "HumidityReading",
            "zone": zone.name,
            "humidity": round(zone.humidity, 0),
            "unit": "percent"
        })
        
    def generate_hvac_status(self, unit: HVACUnit):
        """Emit raw HVAC status"""
        power = unit.power_kw * (0.8 + random.uniform(-0.2, 0.4))
        self.publish("HVACStatus", {
            "event_type": "HVACStatus",
            "unit_id": unit.unit_id,
            "zone": unit.zone,
            "mode": "cooling",
            "power_kw": round(power, 2),
            "compressor_health": round(unit.compressor_health, 1),
            "fan_speed": 50,
            "refrigerant_level": round(unit.refrigerant_level, 1),
        })
        
    def generate_energy(self):
        """Emit raw energy meter reading"""
        total_power = sum(u.power_kw for u in self.hvac_units) + random.uniform(-1, 1)
        self.publish("EnergyMeter", {
            "event_type": "EnergyMeter",
            "meter_id": "MAIN-001",
            "power_kw": round(total_power, 2),
            "voltage": round(230 + random.uniform(-5, 5), 1),
            "current": round((total_power * 1000) / 230, 1),
        })
        
    def run_cycle(self):
        """Generate one cycle of raw sensor events"""
        for zone in self.zones:
            self.generate_temperature(zone)
            
        if self.cycle % 3 == 0:
            for zone in self.zones:
                self.generate_humidity(zone)
                
        if self.cycle % 5 == 0:
            for unit in self.hvac_units:
                self.generate_hvac_status(unit)
                
        if self.cycle % 2 == 0:
            self.generate_energy()
            
        self.cycle += 1


def main():
    parser = argparse.ArgumentParser(description="HVAC RAW Event Generator")
    parser.add_argument("--broker", default="localhost")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--rate", type=float, default=2.0)
    parser.add_argument("--duration", type=int, default=0)
    args = parser.parse_args()
    
    print(f"\n{'='*60}")
    print("  HVAC RAW Event Generator")
    print("  Emits: TemperatureReading, HumidityReading, HVACStatus, EnergyMeter")
    print("  Anomaly detection by Varpulis CEP engine")
    print(f"{'='*60}")
    print(f"  Broker: {args.broker}:{args.port}")
    print(f"  Rate: {args.rate} events/sec")
    print(f"{'='*60}\n")
    
    client = mqtt.Client(client_id=f"hvac-gen-{int(time.time())}")
    try:
        client.connect(args.broker, args.port, 60)
        client.loop_start()
    except Exception as e:
        print(f"ERROR: Cannot connect to MQTT: {e}")
        sys.exit(1)
    
    gen = HVACGenerator(client)
    start_time = time.time()
    interval = 1.0 / args.rate
    
    try:
        while True:
            cycle_start = time.time()
            gen.run_cycle()
            
            elapsed = int(time.time() - start_time)
            print(f"\r[{elapsed:4d}s] Events: {gen.event_count}", end="")
            
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
