import type { PipelineState } from '@/types/pipeline'

/** A realistic IoT monitoring demo pipeline */
export const DEMO_PIPELINE: PipelineState = {
  connectors: [
    {
      id: 'conn_1',
      name: 'MqttBroker',
      type: 'mqtt',
      config: {
        host: 'localhost',
        port: '1883',
        client_id: 'varpulis-cep',
      },
    },
  ],
  events: [
    {
      id: 'evt_1',
      name: 'SensorReading',
      fields: [
        { name: 'sensor_id', type: 'str', optional: false },
        { name: 'zone', type: 'str', optional: false },
        { name: 'temperature', type: 'float', optional: false },
        { name: 'humidity', type: 'float', optional: false },
        { name: 'ts', type: 'timestamp', optional: false },
      ],
    },
  ],
  streams: [
    {
      id: 'stream_1',
      name: 'Telemetry',
      source: 'SensorReading',
      sourceType: 'event',
      sourceRefs: [],
      operations: [
        { id: 'op_1', type: 'from', value: 'MqttBroker, topic: "sensors/#"', expanded: false },
      ],
    },
    {
      id: 'stream_2',
      name: 'HighTemp',
      source: 'Telemetry',
      sourceType: 'stream',
      sourceRefs: [],
      operations: [
        { id: 'op_2', type: 'where', value: 'temperature > 85.0', expanded: false },
        { id: 'op_3', type: 'emit', value: 'sensor: sensor_id, temp: temperature, severity: "warning"', expanded: false },
      ],
    },
    {
      id: 'stream_3',
      name: 'ZoneStats',
      source: 'Telemetry',
      sourceType: 'stream',
      sourceRefs: [],
      operations: [
        { id: 'op_4', type: 'partition_by', value: 'zone', expanded: false },
        { id: 'op_5', type: 'window', value: '5m, sliding: 1m', expanded: false },
        { id: 'op_6', type: 'aggregate', value: 'avg_temp: avg(temperature), max_temp: max(temperature), readings: count()', expanded: false },
      ],
    },
    {
      id: 'stream_4',
      name: 'ZoneAlert',
      source: 'ZoneStats',
      sourceType: 'stream',
      sourceRefs: [],
      operations: [
        { id: 'op_7', type: 'having', value: 'avg_temp > 30.0', expanded: false },
        { id: 'op_8', type: 'emit', value: 'zone: zone, avg: avg_temp, severity: "major"', expanded: false },
        { id: 'op_9', type: 'to', value: 'MqttBroker, topic: "alerts"', expanded: false },
      ],
    },
    {
      id: 'stream_5',
      name: 'TempSpike',
      source: 'Telemetry',
      sourceType: 'stream',
      sourceRefs: [],
      operations: [
        { id: 'op_10', type: 'where', value: 'temperature > 75.0', expanded: false },
        { id: 'op_11', type: 'forecast', value: 'mode: "balanced", confidence: 0.7, horizon: 2h', expanded: false },
        { id: 'op_12', type: 'emit', value: 'sensor: sensor_id, prob: forecast_probability', expanded: false },
      ],
    },
  ],
}
