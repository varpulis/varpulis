import type { ScenarioDefinition } from '@/types/scenario'

export const aiFraudScoringScenario: ScenarioDefinition = {
  id: 'ai-fraud-scoring',
  title: 'AI Fraud Scoring',
  subtitle: 'GPU-Accelerated ML Inference',
  icon: 'mdi-brain',
  color: 'deep-orange',
  summary:
    'Score every transaction in real-time with an ONNX neural network. GPU acceleration and batch inference process thousands of events per second with sub-millisecond latency.',
  vplSource: `event Transaction:
    transaction_id: str
    account: str
    amount: float
    velocity: float
    distance: float

stream FraudAlerts = Transaction
    .score(model: "/app/models/fraud_scorer.onnx", inputs: [amount, velocity, distance], outputs: [fraud_prob], batch_size: 4)
    .where(fraud_prob > 0.7)
    .emit(
        alert_type: "ai_fraud_alert",
        transaction_id: transaction_id,
        account: account,
        amount: amount,
        fraud_probability: fraud_prob
    )`,
  patterns: [
    {
      name: 'Neural Network Scoring',
      description:
        'Each transaction is scored by an ONNX neural network trained on historical fraud data. The model takes three features (amount, velocity, distance) and outputs a probability between 0 and 1. GPU acceleration with batch_size: 32 processes events in parallel.',
      vplSnippet: `.score(model: "/app/models/fraud_scorer.onnx",
    inputs: [amount, velocity, distance],
    outputs: [fraud_prob],
    batch_size: 4)`,
    },
    {
      name: 'Threshold-Based Alerting',
      description:
        'Only transactions scoring above 0.7 fraud probability trigger alerts. This strikes a balance between catching fraud (recall) and minimizing false positives (precision). The threshold is easily tunable in the VPL.',
      vplSnippet: `.where(fraud_prob > 0.7)
    .emit(alert_type: "ai_fraud_alert", fraud_probability: fraud_prob)`,
    },
  ],
  steps: [
    {
      title: 'Legitimate Transactions',
      narration:
        'Normal customer activity: small purchases from known locations at typical velocity. The ML model scores these with low fraud probability (<0.1). No alerts triggered.',
      eventsText: `Transaction { transaction_id: "tx-001", account: "alice", amount: 42.50, velocity: 1, distance: 5 }
BATCH 300
Transaction { transaction_id: "tx-002", account: "bob", amount: 120.00, velocity: 2, distance: 10 }
BATCH 300
Transaction { transaction_id: "tx-003", account: "carol", amount: 85.00, velocity: 1, distance: 3 }`,
      expectedAlerts: [],
      phase: 'normal',
    },
    {
      title: 'Suspicious Transaction',
      narration:
        'A high-value transaction from an unusual location with rapid velocity. The neural network assigns a fraud probability of ~0.95. The .where(fraud_prob > 0.7) filter passes it through, generating an AI fraud alert.',
      eventsText: `Transaction { transaction_id: "tx-004", account: "alice", amount: 50.00, velocity: 1, distance: 5 }
BATCH 300
Transaction { transaction_id: "tx-005", account: "dave", amount: 4800.00, velocity: 18, distance: 750 }
BATCH 300
Transaction { transaction_id: "tx-006", account: "bob", amount: 35.00, velocity: 1, distance: 8 }`,
      expectedAlerts: ['ai_fraud_alert'],
      phase: 'attack',
    },
    {
      title: 'Batch Fraud Burst',
      narration:
        'Card testing attack: multiple rapid small transactions followed by a large one. The model detects the abnormal velocity and distance patterns. GPU batch inference processes all 5 transactions in a single forward pass for maximum throughput.',
      eventsText: `Transaction { transaction_id: "tx-007", account: "eve", amount: 1.00, velocity: 25, distance: 200 }
BATCH 100
Transaction { transaction_id: "tx-008", account: "eve", amount: 2.00, velocity: 30, distance: 300 }
BATCH 100
Transaction { transaction_id: "tx-009", account: "eve", amount: 1.50, velocity: 35, distance: 400 }
BATCH 100
Transaction { transaction_id: "tx-010", account: "eve", amount: 5000.00, velocity: 40, distance: 900 }
BATCH 100
Transaction { transaction_id: "tx-011", account: "carol", amount: 55.00, velocity: 1, distance: 5 }`,
      expectedAlerts: ['ai_fraud_alert'],
      phase: 'attack',
    },
    {
      title: 'Normal Afternoon Activity',
      narration:
        'Regular afternoon shopping — moderate amounts, normal velocity, local distances. All transactions score well below the 0.7 threshold. Zero false positives confirm model quality.',
      eventsText: `Transaction { transaction_id: "tx-012", account: "alice", amount: 200.00, velocity: 2, distance: 15 }
BATCH 300
Transaction { transaction_id: "tx-013", account: "bob", amount: 75.00, velocity: 1, distance: 5 }
BATCH 300
Transaction { transaction_id: "tx-014", account: "carol", amount: 320.00, velocity: 3, distance: 20 }`,
      expectedAlerts: [],
      phase: 'normal',
    },
  ],
}
