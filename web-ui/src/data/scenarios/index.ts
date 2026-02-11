import type { ScenarioDefinition } from '@/types/scenario'
import { blindSpotScenario } from './blind-spot'
import { haystackScenario } from './haystack'
import { socScaleScenario } from './soc-scale'
import { fraudDetectionScenario } from './fraud-detection'
import { predictiveMaintenanceScenario } from './predictive-maintenance'
import { insiderTradingScenario } from './insider-trading'
import { cyberThreatScenario } from './cyber-threat'
import { patientSafetyScenario } from './patient-safety'

export const scenarios: ScenarioDefinition[] = [
  blindSpotScenario,
  haystackScenario,
  socScaleScenario,
  fraudDetectionScenario,
  predictiveMaintenanceScenario,
  insiderTradingScenario,
  cyberThreatScenario,
  patientSafetyScenario,
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
}
