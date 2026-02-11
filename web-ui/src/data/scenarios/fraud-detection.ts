import type { ScenarioDefinition } from '@/types/scenario'

export const fraudDetectionScenario: ScenarioDefinition = {
  id: 'fraud-detection',
  title: 'Credit Card Fraud Detection',
  subtitle: 'Financial Services',
  icon: 'mdi-credit-card-alert',
  color: 'red',
  summary:
    'Banks lose $32B/year to card fraud. Detect account takeover, card testing, and impossible travel patterns in real-time.',
  vplSource: `stream AccountTakeover = Login as login
    -> PasswordChange where user_id == login.user_id as pwd_change
    -> Purchase where user_id == login.user_id as purchase
    .within(30m)
    .not(Logout where user_id == login.user_id)
    .emit(
        alert_type: "account_takeover",
        user_id: login.user_id,
        device: login.device_id,
        purchase_amount: purchase.amount
    )

stream CardTesting = SmallPurchase as first
    -> all SmallPurchase where card_id == first.card_id as tests
    -> LargePurchase where card_id == first.card_id as large
    .within(60m)
    .emit(
        alert_type: "card_testing",
        card_id: first.card_id,
        large_amount: large.amount
    )

stream ImpossibleTravel = Login as login1
    -> Login where user_id == login1.user_id as login2
    .within(1h)
    .where(login2.country != login1.country)
    .emit(
        alert_type: "impossible_travel",
        user_id: login1.user_id,
        location1: login1.country,
        location2: login2.country
    )`,
  patterns: [
    {
      name: 'Account Takeover',
      description:
        'Detects Login followed by PasswordChange followed by Purchase within 30 minutes, with no intervening Logout. Classic account compromise pattern.',
      vplSnippet: `Login as login
    -> PasswordChange where user_id == login.user_id
    -> Purchase where user_id == login.user_id
    .within(30m)
    .not(Logout where user_id == login.user_id)`,
    },
    {
      name: 'Card Testing',
      description:
        'Detects a series of small purchases followed by a large purchase on the same card within 60 minutes. Fraudsters validate stolen cards with micro-charges before making big purchases. With 10 small purchases Varpulis finds 511 subsequences vs 1 for traditional CEP.',
      vplSnippet: `SmallPurchase as first
    -> all SmallPurchase where card_id == first.card_id
    -> LargePurchase where card_id == first.card_id
    .within(60m)`,
    },
    {
      name: 'Impossible Travel',
      description:
        'Detects logins from two different countries within 1 hour for the same user. Physically impossible unless credentials are compromised.',
      vplSnippet: `Login as login1
    -> Login where user_id == login1.user_id as login2
    .within(1h)
    .where(login2.country != login1.country)`,
    },
  ],
  steps: [
    {
      title: 'Legitimate Activity',
      narration:
        'Three normal users go about their day \u2014 logging in from known devices, making everyday purchases, and logging out. This establishes a baseline of clean activity. Zero alerts expected.',
      eventsText: `Login { user_id: "alice", device_id: "alice_phone", country: "FR", ip: "80.1.2.3" }
BATCH 5000
Purchase { user_id: "alice", amount: 42.50, merchant: "grocery_store" }
BATCH 3000
Login { user_id: "bob", device_id: "bob_laptop", country: "DE", ip: "91.10.20.30" }
BATCH 4000
Purchase { user_id: "bob", amount: 18.90, merchant: "coffee_shop" }
BATCH 2000
Login { user_id: "carol", device_id: "carol_tablet", country: "US", ip: "72.44.55.66" }
BATCH 6000
Logout { user_id: "alice" }
BATCH 3000
Purchase { user_id: "carol", amount: 65.00, merchant: "bookstore" }
BATCH 4000
Logout { user_id: "bob" }`,
      expectedAlerts: [],
      phase: 'normal',
    },
    {
      title: 'Account Takeover',
      narration:
        'An attacker logs in from a new device, immediately changes the password to lock out the real owner, then makes a high-value purchase. Meanwhile, normal users continue transacting \u2014 only the attacker triggers the Login \u2192 PasswordChange \u2192 Purchase chain with no intervening Logout.',
      eventsText: `Login { user_id: "victim_dan", device_id: "unknown_device_99", country: "US", ip: "1.2.3.4" }
BATCH 3000
Purchase { user_id: "alice", amount: 12.00, merchant: "bakery" }
BATCH 2000
PasswordChange { user_id: "victim_dan" }
BATCH 4000
Login { user_id: "bob", device_id: "bob_laptop", country: "DE", ip: "91.10.20.30" }
BATCH 3000
Purchase { user_id: "bob", amount: 22.50, merchant: "lunch_cafe" }
BATCH 5000
Purchase { user_id: "victim_dan", amount: 4999.00, merchant: "electronics_store" }
BATCH 2000
Logout { user_id: "bob" }
BATCH 3000
Purchase { user_id: "carol", amount: 8.50, merchant: "parking" }`,
      expectedAlerts: ['account_takeover'],
      phase: 'attack',
    },
    {
      title: 'Card Testing (Kleene Star)',
      narration:
        'A fraudster validates stolen card 42 with 10 rapid micro-purchases at gas stations and vending machines, interleaved with legitimate transactions from other cards. The final large purchase at a jewelry store completes the pattern. With 10 small purchases (1 first + 9 Kleene), Varpulis enumerates all 511 subsequences \u2014 a traditional CEP engine reports just 1.',
      eventsText: `SmallPurchase { card_id: "stolen_42", amount: 1.00, merchant: "gas_station_A" }
BATCH 2000
SmallPurchase { card_id: "legit_77", amount: 35.00, merchant: "deli" }
BATCH 2000
SmallPurchase { card_id: "stolen_42", amount: 0.50, merchant: "vending_1" }
BATCH 2000
SmallPurchase { card_id: "stolen_42", amount: 1.25, merchant: "gas_station_B" }
BATCH 3000
SmallPurchase { card_id: "legit_88", amount: 28.00, merchant: "pharmacy" }
BATCH 2000
SmallPurchase { card_id: "stolen_42", amount: 0.75, merchant: "vending_2" }
BATCH 2000
SmallPurchase { card_id: "stolen_42", amount: 1.50, merchant: "gas_station_C" }
BATCH 2000
SmallPurchase { card_id: "stolen_42", amount: 2.00, merchant: "parking_meter" }
BATCH 2000
SmallPurchase { card_id: "legit_77", amount: 15.00, merchant: "sandwich_shop" }
BATCH 2000
SmallPurchase { card_id: "stolen_42", amount: 1.00, merchant: "toll_booth" }
BATCH 2000
SmallPurchase { card_id: "stolen_42", amount: 0.50, merchant: "vending_3" }
BATCH 3000
SmallPurchase { card_id: "stolen_42", amount: 1.75, merchant: "gas_station_D" }
BATCH 2000
SmallPurchase { card_id: "legit_99", amount: 45.00, merchant: "restaurant" }
BATCH 2000
SmallPurchase { card_id: "stolen_42", amount: 1.00, merchant: "gas_station_E" }
BATCH 5000
LargePurchase { card_id: "stolen_42", amount: 2500.00, merchant: "jewelry_store" }`,
      expectedAlerts: ['card_testing'],
      phase: 'attack',
    },
    {
      title: 'Impossible Travel + Negation Proof',
      narration:
        'The attacker logs in from the US, then 20 minutes later from Nigeria \u2014 impossible travel triggers. Also: a legitimate user logs in, logs OUT, then purchases. Because the Logout breaks the sequence, no account_takeover fires \u2014 proving negation works correctly.',
      eventsText: `Login { user_id: "traveler_x", device_id: "phone_1", country: "US", ip: "10.0.0.1" }
BATCH 3000
Login { user_id: "safe_user", device_id: "known_dev", country: "FR", ip: "82.1.2.3" }
BATCH 5000
PasswordChange { user_id: "safe_user" }
BATCH 4000
Logout { user_id: "safe_user" }
BATCH 3000
Purchase { user_id: "safe_user", amount: 200.00, merchant: "department_store" }
BATCH 5000
Login { user_id: "traveler_x", device_id: "laptop_2", country: "NG", ip: "41.58.100.1" }
BATCH 3000
Login { user_id: "carol", device_id: "carol_tablet", country: "US", ip: "72.44.55.66" }
BATCH 4000
Logout { user_id: "carol" }`,
      expectedAlerts: ['impossible_travel'],
      phase: 'attack',
    },
  ],
}
