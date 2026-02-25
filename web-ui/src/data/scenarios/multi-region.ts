import type { ScenarioDefinition } from '@/types/scenario'

export const multiRegionScenario: ScenarioDefinition = {
  id: 'multi-region',
  title: 'Multi-Region Federation',
  subtitle: 'Cross-Region Event Routing',
  icon: 'mdi-earth',
  color: 'indigo',
  summary:
    'Federate Varpulis clusters across multiple regions with automatic catalog sync, health monitoring, and cross-region event routing. Detect global patterns that span data centers.',
  vplSource: `event Transaction:
    account: str
    amount: float
    region: str
    merchant: str

event Chargeback:
    account: str
    amount: float
    region: str
    reason: str

stream GlobalFraud = Transaction as t
    -> Chargeback where account == t.account as c
    .within(24h)
    .where(t.region != c.region)
    .emit(
        alert_type: "cross_region_fraud",
        account: t.account,
        transaction_region: t.region,
        chargeback_region: c.region,
        amount: t.amount
    )`,
  patterns: [
    {
      name: 'Cross-Region Fraud',
      description:
        'Detects when a transaction in one region is followed by a chargeback filed in a different region within 24 hours. This pattern spans data centers and requires federation to correlate events across regions.',
      vplSnippet: `Transaction as t
    -> Chargeback where account == t.account as c
    .within(24h)
    .where(t.region != c.region)`,
    },
    {
      name: 'Federation Routing',
      description:
        'Events are automatically routed between regions based on pipeline subscriptions. When us-east detects a Transaction, it\'s forwarded to eu-west if a pipeline there subscribes to Transaction events. NATS provides the transport layer.',
      vplSnippet: `// Federation config (TOML)
// [federation]
// regions = ["us-east", "eu-west", "ap-south"]
// sync_interval_secs = 30
// routing: Transaction* -> all regions`,
    },
  ],
  steps: [
    {
      title: 'Normal Multi-Region Activity',
      narration:
        'Transactions processed across three regions: US, EU, and APAC. Each stays within its region. No cross-region chargebacks. Federation health: all 3 regions online.',
      eventsText: `Transaction { account: "user-101", amount: 250.00, region: "us-east", merchant: "amazon.com" }
BATCH 500
Transaction { account: "user-202", amount: 180.00, region: "eu-west", merchant: "zalando.de" }
BATCH 500
Transaction { account: "user-303", amount: 90.00, region: "ap-south", merchant: "flipkart.in" }`,
      expectedAlerts: [],
      phase: 'normal',
    },
    {
      title: 'Cross-Region Fraud Detected',
      narration:
        'A transaction occurs in us-east, then a chargeback for the same account is filed in eu-west. The federation layer routes the Chargeback event to the us-east cluster where the pattern matcher correlates it with the original Transaction. Alert fires: cross-region fraud.',
      eventsText: `Transaction { account: "user-101", amount: 3500.00, region: "us-east", merchant: "electronics-store.com" }
BATCH 1000
Chargeback { account: "user-101", amount: 3500.00, region: "eu-west", reason: "unauthorized" }`,
      expectedAlerts: ['cross_region_fraud'],
      phase: 'attack',
    },
    {
      title: 'Region Failover',
      narration:
        'The ap-south region goes offline (simulated health degradation). Federation automatically marks it as degraded, then offline. Remaining regions continue processing. Cross-region patterns still detect fraud between us-east and eu-west.',
      eventsText: `Transaction { account: "user-202", amount: 5000.00, region: "eu-west", merchant: "luxury-brand.fr" }
BATCH 1000
Chargeback { account: "user-202", amount: 5000.00, region: "us-east", reason: "item_not_received" }
BATCH 500
Transaction { account: "user-404", amount: 120.00, region: "us-east", merchant: "bookstore.com" }`,
      expectedAlerts: ['cross_region_fraud'],
      phase: 'attack',
    },
    {
      title: 'Same-Region Chargebacks',
      narration:
        'Chargebacks filed in the same region as the original transaction. These are normal disputes, not cross-region fraud. The .where(t.region != c.region) condition correctly filters them out. Zero false positives.',
      eventsText: `Transaction { account: "user-501", amount: 80.00, region: "us-east", merchant: "diner.com" }
BATCH 500
Chargeback { account: "user-501", amount: 80.00, region: "us-east", reason: "duplicate_charge" }
BATCH 500
Transaction { account: "user-601", amount: 200.00, region: "eu-west", merchant: "fashion.eu" }
BATCH 500
Chargeback { account: "user-601", amount: 200.00, region: "eu-west", reason: "wrong_item" }`,
      expectedAlerts: [],
      phase: 'negative',
    },
  ],
}
