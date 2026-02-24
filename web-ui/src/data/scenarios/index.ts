import type { ScenarioDefinition } from '@/types/scenario'
import { blindSpotScenario } from './blind-spot'
import { haystackScenario } from './haystack'
import { socScaleScenario } from './soc-scale'
import { fraudDetectionScenario } from './fraud-detection'
import { predictiveMaintenanceScenario } from './predictive-maintenance'
import { insiderTradingScenario } from './insider-trading'
import { cyberThreatScenario } from './cyber-threat'
import { patientSafetyScenario } from './patient-safety'
import { iotConcurrentScenario } from './iot-concurrent'
import { aiFraudScoringScenario } from './ai-fraud-scoring'
import { multiRegionScenario } from './multi-region'

export const scenarios: ScenarioDefinition[] = [
  blindSpotScenario,
  haystackScenario,
  socScaleScenario,
  fraudDetectionScenario,
  predictiveMaintenanceScenario,
  insiderTradingScenario,
  cyberThreatScenario,
  patientSafetyScenario,
  iotConcurrentScenario,
  aiFraudScoringScenario,
  multiRegionScenario,
]

export {
  blindSpotScenario,
  haystackScenario,
  socScaleScenario,
  fraudDetectionScenario,
  predictiveMaintenanceScenario,
  insiderTradingScenario,
  cyberThreatScenario,
  patientSafetyScenario,
  iotConcurrentScenario,
  aiFraudScoringScenario,
  multiRegionScenario,
}
