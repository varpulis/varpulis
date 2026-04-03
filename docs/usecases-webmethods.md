# Varpulis + webMethods — Integration Use Cases

## Architecture (all use cases)

Two connection paths depending on the event source:

**Business events (documents, API calls):** Flow services already process these.
Add a `pub.publish:stream` step via WmStreaming to publish to Kafka — one line
in the flow.

```
IS flow service ──pub.publish:stream──→ Kafka ──→ Varpulis ──→ Kafka/HTTP
                                                                   ↓
                                                            Alert / Circuit Break
```

**Audit/system events (service invocations, errors, metrics):** IS natively
forwards these to webMethods Messaging (UM) — no custom code. Two options to
get them into Varpulis:

```
IS audit ──native──→ UM ──jms-kafka-bridge──→ Kafka ──→ Varpulis
IS audit ──native──→ Audit DB (PostgreSQL) ──cdc──→ Varpulis
```

The database path via `postgres_cdc` avoids the UM-to-Kafka bridge entirely and
is the simplest option when the audit log database is already PostgreSQL.
WmStreaming does NOT natively receive audit events — publishing audit data to
Kafka would require writing custom services, which adds unnecessary work when
the database or UM paths already deliver.

## Latency Summary

| Layer | Latency |
|-------|---------|
| Pattern detection (NFA + ZDD) | < 1ms per event |
| Forecast prediction (PST) | ~11 microseconds |
| Forecast warmup | Default 100 events, configurable per stream |
| End-to-end with Kafka | Kafka consumer lag + processing, typically < 2s |
| Negative patterns (NOT) | Alert fires at window expiry when expected event is absent |

---

## UC1 — End-to-End SLA Breach Prediction

**The pain:** A typical webMethods landscape has multi-hop flows: IS-1 receives
an order, enriches via IS-2, posts to SAP via IS-3. Each hop has its own audit
log, but nobody correlates them end-to-end. When an SLA is breached, you find
out after the customer calls.

**Why current tools fail:** IS audit logging, UM monitoring, and Optimize for
Infrastructure all look at individual hops. There's no temporal correlation
across the chain.

**Varpulis angle:**

```vpl
event OrderReceived:
    orderId: string
    partner: string

event OrderEnriched:
    orderId: string

event OrderConfirmed:
    orderId: string

-- Enrichment lag: flag if enrichment takes more than 5 minutes
-- (early warning — distinct from the end-to-end SLA)
stream EnrichmentLag = OrderReceived as recv
    -> NOT OrderEnriched .where(orderId == recv.orderId)
    .within(5m)
    .partition_by(recv.orderId)
    .emit(order: recv.orderId, partner: recv.partner,
          warning: "enrichment exceeds 5m")
    .to(kafka(brokers: "ops-kafka:9092", topic: "sla-warnings"))

-- End-to-end SLA: full flow must complete within 30 minutes
stream SlaBreachRisk = OrderReceived as recv
    -> OrderEnriched as enr
    -> OrderConfirmed as conf
    .within(30m)
    .partition_by(recv.orderId)
    .forecast(confidence: 0.8, horizon: 10m, warmup: 200)
    .where(forecast_probability < 0.5)
    .emit(order: recv.orderId, partner: recv.partner,
          breach_prob: forecast_probability, eta: forecast_time)
    .to(kafka(brokers: "ops-kafka:9092", topic: "sla-alerts"))
```

VPL supports a single `.within()` per stream (global, anchored to the first
matched event). Per-hop timeouts require separate streams — here
`EnrichmentLag` catches slow enrichment at 5m while `SlaBreachRisk` covers the
full 30m end-to-end window. The `.forecast()` uses `warmup: 200` (overrides
engine default of 100) — no predictions before 200 events have been observed.

**Differentiator:** PST forecasting predicts breaches before they happen. No
other tool in the webMethods ecosystem does this.

**Connection:** Audit events. Simplest path: `postgres_cdc` against the IS audit
database. Alternatively, IS natively forwards audit events to UM, which can be
bridged to Kafka.

---

## UC2 — B2B Document Flow Anomalies (Trading Networks)

**The pain:** Trading Networks processes EDI — 850 (PO), 855 (PO Ack), 856
(ASN), 810 (Invoice), 997 (Functional Ack). Partners are supposed to follow
sequences, but in practice: 997 acknowledgments go missing (costs penalties),
ASNs never arrive after PO confirmations, and partners silently stop sending.

**Why current tools fail:** TN has document tracking, but per-document, not
per-sequence. Finding "partner X hasn't sent a 997 for any of the last 15 POs"
requires manual SQL against TN tables.

**Varpulis angle — three forward-looking patterns:**

```vpl
event EdiDoc:
    docType: string
    partnerId: string
    controlNumber: string

-- Pattern 1: Missing functional acknowledgment
-- PO sent, no 997 within 4 hours → contractual penalty risk
stream MissingAck = EdiDoc as po .where(po.docType == "850")
    -> NOT EdiDoc .where(docType == "997" AND partnerId == po.partnerId)
    .within(4h)
    .partition_by(po.partnerId)
    .emit(partner: po.partnerId, po_control: po.controlNumber,
          violation: "997 not received within 4h")
    .to(http(url: "https://ops.internal/edi-alerts", method: "POST"))

-- Pattern 2: Stale PO — confirmation sent but ASN never follows
stream StaleConfirmation = EdiDoc as conf .where(conf.docType == "855")
    -> NOT EdiDoc .where(docType == "856" AND partnerId == conf.partnerId)
    .within(48h)
    .partition_by(conf.partnerId)
    .emit(partner: conf.partnerId,
          violation: "ASN not received within 48h of PO confirmation")

-- Pattern 3: Full happy-path enforcement
-- Entire PO→Confirmation→ASN→Invoice flow must complete within 7 days.
-- The .within(7d) window starts from the first matched event (the 850 PO).
-- The NOT fires if the invoice hasn't arrived before the 7d window expires.
stream IncompleteFlow = EdiDoc as po .where(po.docType == "850")
    -> EdiDoc as conf .where(conf.docType == "855" AND conf.partnerId == po.partnerId)
    -> EdiDoc as asn .where(asn.docType == "856" AND asn.partnerId == po.partnerId)
    -> NOT EdiDoc .where(docType == "810" AND partnerId == po.partnerId)
    .within(7d)
    .partition_by(po.partnerId)
    .emit(partner: po.partnerId, po_control: po.controlNumber,
          violation: "Invoice not received within 7 days of PO")
```

All three patterns are forward-looking (event A, then expect B within window) —
this plays to SASE's natural semantics. The `.within()` is global per stream:
it constrains the entire sequence from the first matched event.

**Business value:** Each missing 997 can trigger contractual penalties. Each
stale PO means shipments stuck. Directly quantifiable in dollars per partner per
month.

**Connection:** Business documents. TN flow services already handle these — add
a `pub.publish:stream` step to publish to Kafka via WmStreaming. Alternatively,
use `postgres_cdc` to watch inserts into the TN document tables.

**Latency:** Pattern detection < 1ms. For negative patterns (NOT), the alert
fires at window expiry when the expected event hasn't arrived — the alert
latency is the window duration by design (4h, 48h, 7d respectively).

---

## UC3 — Error Cascade Detection + Circuit Breaking

**The pain:** When a backend system (SAP, Salesforce, a legacy mainframe) goes
down, dozens of IS services start failing and retrying simultaneously. Trigger
queues back up across IS. The retry mechanism is per-service — nobody connects
"these 40 different flows are all failing because SAP is down." By the time ops
notices, IS is thrashing and retry queues are saturated.

**Why current tools fail:** IS monitors individual service invocations. Optimize
for Infrastructure shows server-level metrics. Neither correlates temporal
failure patterns across services.

**Varpulis angle:**

```vpl
event ServiceError:
    serviceName: string
    errorCode: string
    targetSystem: string

-- 5+ distinct services failing against the same target within 2 minutes.
-- Uses SEQ() with Kleene+ for unbounded error accumulation.
-- count() returns the number of matched events in the closure.
pattern ErrorBurst = SEQ(
    ServiceError as first,
    ServiceError+ as errors
) within 2m partition by targetSystem

stream ErrorCascade = ErrorBurst
    .where(count(errors) >= 4)
    .emit(target: first.targetSystem,
          affected: count(errors) + 1)
    .to(http(url: "https://ops.internal/circuit-break", method: "POST"))
```

The HTTP sink triggers a circuit breaker that pauses all flows targeting that
system, rather than letting them hammer a dead endpoint.

**Latency:** Sub-millisecond per event. The `.within(2m)` window means the
Kleene+ accumulates errors — the `.where()` filter evaluates on each new match,
so the alert fires as soon as the 5th distinct service fails.

---

## UC4 — Compliance Sequence Enforcement

**The pain:** Regulated industries must prove that business processes execute in
the correct order within time windows:

- **Pharma (GxP):** batch release → QA sign-off → shipping → delivery confirmation
- **Finance (MiFID II):** order received → best execution check → execution → transaction report
- **Supply chain:** goods receipt → quality inspection → stock placement

webMethods orchestrates these flows, but proving compliance (that the sequence
was never violated) requires after-the-fact auditing of logs.

**Varpulis angle:**

```vpl
event BatchRelease:
    batchId: string
    product: string

event QaSignoff:
    batchId: string
    inspector: string

event ShipmentInitiated:
    batchId: string

-- GxP violation: QA sign-off not received within 2 hours of batch release
stream GxpViolation = BatchRelease as br
    -> NOT QaSignoff .where(batchId == br.batchId)
    .within(2h)
    .partition_by(br.batchId)
    .emit(batch: br.batchId, product: br.product,
          violation: "QA sign-off missed within 2h window")
    .to(kafka(brokers: "audit-kafka:9092", topic: "compliance-violations"))

-- Happy-path tracking: full compliant release flow
stream ValidRelease = BatchRelease as br
    -> QaSignoff as qa .where(qa.batchId == br.batchId)
    -> ShipmentInitiated as ship .where(ship.batchId == br.batchId)
    .within(24h)
    .partition_by(br.batchId)
    .emit(batch: br.batchId, inspector: qa.inspector, status: "compliant")
```

**Differentiator:** Compliance rules expressed as readable, auditable `.vpl`
files. Auditors can read the rules without understanding code. Violations
detected in real-time, not discovered in the next quarterly audit.

**Latency:** < 1ms per event. Violation alerts fire at window expiry (2h) when
the expected event is absent. Compliant-path matches fire immediately when the
last event in the sequence arrives.

---

## UC5 — API Gateway Abuse Detection

**The pain:** webMethods API Gateway does rate limiting and throttling per API
key. But sophisticated abuse patterns slip through: credential stuffing
(different credentials, same IP, bursts on login), sequential enumeration, and
slow exfiltration that stays just under rate limits.

**Why current tools fail:** Rate limiting is per-key, per-window, stateless. It
doesn't correlate temporal sequences of calls.

**Varpulis angle:**

```vpl
event ApiCall:
    clientIp: string
    endpoint: string
    statusCode: int
    apiKey: string

-- Credential stuffing: 10+ failed logins from same IP within 5 minutes
-- Uses SEQ() with Kleene+ for unbounded attempt accumulation
pattern StuffingPattern = SEQ(
    ApiCall where endpoint == "/auth/login" and statusCode == 401 as first,
    ApiCall+ where endpoint == "/auth/login" and statusCode == 401 as attempts
) within 5m partition by clientIp

stream CredentialStuffing = StuffingPattern
    .where(count(attempts) >= 9)
    .emit(source_ip: first.clientIp,
          attempt_count: count(attempts) + 1,
          action: "block_ip")
    .to(http(url: "https://apigw.internal/blacklist", method: "POST"))

-- API enumeration: high-frequency probing on a path prefix
pattern EnumPattern = SEQ(
    ApiCall where starts_with(endpoint, "/api/users/") as first,
    ApiCall+ where starts_with(endpoint, "/api/users/") as probes
) within 1m partition by clientIp

stream ApiEnumeration = EnumPattern
    .where(count(probes) >= 49)
    .emit(source_ip: first.clientIp,
          probe_count: count(probes) + 1,
          action: "throttle")
    .to(http(url: "https://apigw.internal/throttle", method: "POST"))
```

`starts_with()` is a built-in string function — no glob or regex needed for
prefix matching. Both patterns are complete end-to-end: pattern detected, alert
emitted, action taken via HTTP sink to the gateway's blacklist/throttle API.

**Connection:** API Gateway access logs to Elasticsearch (Varpulis has an ES
connector). For Kafka, add a `pub.publish:stream` step in the gateway policy's
logging flow.

**Latency:** < 1ms per event. Block/throttle action fires as soon as the count
threshold is crossed within the window.

---

## Recommended Starting Order

1. **UC3 (Error Cascade)** first: minimal setup, universal pain, visible action
   (circuit break), no warmup period, fires immediately on threshold.

2. **UC1 (SLA Prediction)** for the impressive demo: unique forecasting angle,
   but needs warmup period (200 events minimum in this config) before
   predictions begin.

3. **UC2 (B2B Anomalies)** for the business case: dollar amounts on every
   missed 997.
