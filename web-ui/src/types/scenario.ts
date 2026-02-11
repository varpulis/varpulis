export interface DemoStep {
  title: string
  narration: string
  eventsText: string
  expectedAlerts: string[]
  phase: 'normal' | 'attack' | 'negative'
}

export interface PatternInfo {
  name: string
  description: string
  vplSnippet: string
}

export interface ScenarioDefinition {
  id: string
  title: string
  subtitle: string
  icon: string
  color: string
  summary: string
  vplSource: string
  patterns: PatternInfo[]
  steps: DemoStep[]
}
