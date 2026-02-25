import type { ScenarioDefinition } from '@/types/scenario'

export const iotConcurrentScenario: ScenarioDefinition = {
  id: 'iot-concurrent',
  title: 'IoT Parallel Processing',
  subtitle: 'Concurrent Partitioned Streams',
  icon: 'mdi-chip',
  color: 'teal',
  summary:
    'Process millions of IoT sensor readings in parallel using .concurrent() with partition-key routing. Each sensor\'s events stay ordered while throughput scales linearly with workers.',
  vplSource: `stream SensorAlerts = SensorReading
    .concurrent(workers: 4, partition_key: "sensor_id")
    .where(temperature > 85 or humidity > 95 or pressure < 950)
    .emit(
        alert_type: "sensor_anomaly",
        sensor_id: sensor_id,
        zone: zone,
        temperature: temperature,
        humidity: humidity,
        pressure: pressure
    )`,
  patterns: [
    {
      name: 'Parallel Sensor Processing',
      description:
        'Sensor events are hash-partitioned by sensor_id across 4 worker threads. Events from the same sensor always land on the same worker, preserving per-sensor ordering. The .where() filter runs independently on each partition, then results merge back into a single stream.',
      vplSnippet: `.concurrent(workers: 4, partition_key: "sensor_id")
    .where(temperature > 85 or humidity > 95 or pressure < 950)`,
    },
    {
      name: 'Anomaly Detection',
      description:
        'Three anomaly conditions checked simultaneously: overheating (>85C), extreme humidity (>95%), or low pressure (<950 hPa). Any condition triggers an alert with full sensor context.',
      vplSnippet: `.where(temperature > 85 || humidity > 95 || pressure < 950)
    .emit(alert_type: "sensor_anomaly", sensor_id: sensor_id, zone: zone)`,
    },
  ],
  steps: [
    {
      title: 'Normal Operations',
      narration:
        'Four sensors in different factory zones report normal readings. All values within thresholds. No alerts expected — this establishes the baseline.',
      eventsText: `SensorReading { sensor_id: "s1", zone: "assembly", temperature: 22, humidity: 45, pressure: 1013 }
BATCH 200
SensorReading { sensor_id: "s2", zone: "welding", temperature: 35, humidity: 30, pressure: 1012 }
BATCH 200
SensorReading { sensor_id: "s3", zone: "paint", temperature: 28, humidity: 60, pressure: 1015 }
BATCH 200
SensorReading { sensor_id: "s4", zone: "storage", temperature: 18, humidity: 50, pressure: 1010 }`,
      expectedAlerts: [],
      phase: 'normal',
    },
    {
      title: 'Overheating Alert',
      narration:
        'The welding zone sensor spikes to 92C while other sensors remain normal. The concurrent pipeline partitions by sensor_id, so s2\'s events are processed on a dedicated worker. The high temperature triggers an anomaly alert.',
      eventsText: `SensorReading { sensor_id: "s1", zone: "assembly", temperature: 23, humidity: 44, pressure: 1013 }
BATCH 200
SensorReading { sensor_id: "s2", zone: "welding", temperature: 92, humidity: 28, pressure: 1011 }
BATCH 200
SensorReading { sensor_id: "s3", zone: "paint", temperature: 27, humidity: 58, pressure: 1014 }
BATCH 200
SensorReading { sensor_id: "s4", zone: "storage", temperature: 19, humidity: 52, pressure: 1011 }`,
      expectedAlerts: ['sensor_anomaly'],
      phase: 'attack',
    },
    {
      title: 'Multiple Zone Alerts',
      narration:
        'Cascading failure: welding zone overheats (95C), paint zone humidity spikes (98%), and storage zone pressure drops (940 hPa). Three alerts fire simultaneously, each processed on its partition\'s worker thread — true parallelism.',
      eventsText: `SensorReading { sensor_id: "s1", zone: "assembly", temperature: 24, humidity: 46, pressure: 1012 }
BATCH 200
SensorReading { sensor_id: "s2", zone: "welding", temperature: 95, humidity: 25, pressure: 1010 }
BATCH 200
SensorReading { sensor_id: "s3", zone: "paint", temperature: 30, humidity: 98, pressure: 1013 }
BATCH 200
SensorReading { sensor_id: "s4", zone: "storage", temperature: 17, humidity: 48, pressure: 940 }`,
      expectedAlerts: ['sensor_anomaly'],
      phase: 'attack',
    },
    {
      title: 'Recovery',
      narration:
        'All sensors return to normal ranges after maintenance intervention. Zero alerts confirm the system correctly tracks current state, not historical anomalies.',
      eventsText: `SensorReading { sensor_id: "s1", zone: "assembly", temperature: 22, humidity: 45, pressure: 1013 }
BATCH 200
SensorReading { sensor_id: "s2", zone: "welding", temperature: 38, humidity: 30, pressure: 1012 }
BATCH 200
SensorReading { sensor_id: "s3", zone: "paint", temperature: 27, humidity: 55, pressure: 1014 }
BATCH 200
SensorReading { sensor_id: "s4", zone: "storage", temperature: 19, humidity: 50, pressure: 1010 }`,
      expectedAlerts: [],
      phase: 'normal',
    },
  ],
}
