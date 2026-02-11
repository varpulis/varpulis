import type { ScenarioDefinition } from '@/types/scenario'

export const predictiveMaintenanceScenario: ScenarioDefinition = {
  id: 'predictive-maintenance',
  title: 'Predictive Equipment Failure',
  subtitle: 'Manufacturing / IoT',
  icon: 'mdi-cog-sync',
  color: 'green',
  summary:
    'Unplanned downtime costs manufacturers $260K/hour. Detect bearing degradation and overheating cascades before equipment fails.',
  vplSource: `stream BearingDegradation = VibrationReading as first
    -> VibrationReading where machine_id == first.machine_id as second
    .within(2h)
    .where(second.amplitude > first.amplitude * 1.3)
    .emit(
        alert_type: "bearing_degradation",
        machine_id: first.machine_id,
        initial_amplitude: first.amplitude,
        current_amplitude: second.amplitude
    )

stream OverheatingCascade = TemperatureReading as baseline
    -> TemperatureReading where zone_id == baseline.zone_id as spike
    .within(30m)
    .where(spike.temp_c - baseline.temp_c > 15.0)
    .emit(
        alert_type: "overheating",
        zone_id: baseline.zone_id,
        baseline_temp: baseline.temp_c,
        spike_temp: spike.temp_c
    )`,
  patterns: [
    {
      name: 'Bearing Degradation',
      description:
        'Detects vibration amplitude increasing by more than 30% between two readings on the same machine within 2 hours. Early warning of mechanical wear. Multiple consecutive readings create many matching pairs.',
      vplSnippet: `VibrationReading as first
    -> VibrationReading where machine_id == first.machine_id as second
    .within(2h)
    .where(second.amplitude > first.amplitude * 1.3)`,
    },
    {
      name: 'Overheating Cascade',
      description:
        'Detects a temperature spike of more than 15 degrees Celsius in the same zone within 30 minutes. Indicates cooling system failure or runaway exothermic process.',
      vplSnippet: `TemperatureReading as baseline
    -> TemperatureReading where zone_id == baseline.zone_id as spike
    .within(30m)
    .where(spike.temp_c - baseline.temp_c > 15.0)`,
    },
  ],
  steps: [
    {
      title: 'Normal Operations',
      narration:
        'Three CNC machines report vibration and temperature readings, all within normal operating ranges. Vibration amplitudes are stable and temperature changes are minimal. Zero alerts expected.',
      eventsText: `VibrationReading { machine_id: "CNC-01", amplitude: 0.25, unit: "mm" }
BATCH 10000
VibrationReading { machine_id: "CNC-02", amplitude: 0.28, unit: "mm" }
BATCH 10000
VibrationReading { machine_id: "CNC-03", amplitude: 0.22, unit: "mm" }
BATCH 15000
TemperatureReading { zone_id: "ZoneA", temp_c: 40.0, sensor: "T-001" }
BATCH 10000
TemperatureReading { zone_id: "ZoneB", temp_c: 38.0, sensor: "T-002" }
BATCH 15000
VibrationReading { machine_id: "CNC-01", amplitude: 0.26, unit: "mm" }
BATCH 10000
VibrationReading { machine_id: "CNC-02", amplitude: 0.29, unit: "mm" }
BATCH 10000
TemperatureReading { zone_id: "ZoneA", temp_c: 41.0, sensor: "T-001" }`,
      expectedAlerts: [],
      phase: 'normal',
    },
    {
      title: 'Bearing Degradation',
      narration:
        'CNC-01 vibration amplitude steadily climbs from 0.30 to 1.30 mm over 6 readings \u2014 each successive pair where the ratio exceeds 1.3\u00d7 triggers a bearing_degradation alert. Meanwhile, CNC-02 and CNC-03 readings stay flat. Multiple matches fire as each earlier low reading pairs with each later high reading.',
      eventsText: `VibrationReading { machine_id: "CNC-01", amplitude: 0.30, unit: "mm" }
BATCH 12000
VibrationReading { machine_id: "CNC-02", amplitude: 0.27, unit: "mm" }
BATCH 10000
VibrationReading { machine_id: "CNC-01", amplitude: 0.50, unit: "mm" }
BATCH 12000
VibrationReading { machine_id: "CNC-03", amplitude: 0.23, unit: "mm" }
BATCH 10000
VibrationReading { machine_id: "CNC-01", amplitude: 0.70, unit: "mm" }
BATCH 12000
VibrationReading { machine_id: "CNC-02", amplitude: 0.28, unit: "mm" }
BATCH 10000
VibrationReading { machine_id: "CNC-01", amplitude: 0.90, unit: "mm" }
BATCH 12000
VibrationReading { machine_id: "CNC-01", amplitude: 1.10, unit: "mm" }
BATCH 10000
VibrationReading { machine_id: "CNC-03", amplitude: 0.24, unit: "mm" }
BATCH 12000
VibrationReading { machine_id: "CNC-01", amplitude: 1.30, unit: "mm" }`,
      expectedAlerts: ['bearing_degradation'],
      phase: 'attack',
    },
    {
      title: 'Overheating Cascade',
      narration:
        'Zone A temperature climbs rapidly from 45\u00b0C through 52, 61, to 78\u00b0C \u2014 each reading that exceeds the baseline by more than 15\u00b0C fires an overheating alert. Zone B stays stable around 40-42\u00b0C, producing zero alerts. Multiple baseline-to-spike pairs trigger.',
      eventsText: `TemperatureReading { zone_id: "ZoneA", temp_c: 45.0, sensor: "T-001" }
BATCH 5000
TemperatureReading { zone_id: "ZoneB", temp_c: 40.0, sensor: "T-002" }
BATCH 5000
TemperatureReading { zone_id: "ZoneA", temp_c: 52.0, sensor: "T-001" }
BATCH 5000
TemperatureReading { zone_id: "ZoneB", temp_c: 41.0, sensor: "T-002" }
BATCH 5000
TemperatureReading { zone_id: "ZoneA", temp_c: 61.0, sensor: "T-001" }
BATCH 5000
TemperatureReading { zone_id: "ZoneB", temp_c: 42.0, sensor: "T-002" }
BATCH 5000
TemperatureReading { zone_id: "ZoneA", temp_c: 78.0, sensor: "T-001" }`,
      expectedAlerts: ['overheating'],
      phase: 'attack',
    },
  ],
}
