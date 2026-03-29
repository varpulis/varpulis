# n8n Community Forum Post Draft

## Title: New Community Node: Temporal Pattern Detection with Varpulis

## Category: Share a workflow

---

Hey everyone! 👋

I just published **n8n-nodes-varpulis** — a community node that adds **temporal pattern detection** to your n8n workflows.

### What does it do?

Most n8n workflows trigger on a single event: "when a payment fails, send a Slack message." But what if you want to detect **patterns over time**?

- 3 failed payments from the same customer within 1 hour → churn risk
- Login from new IP → large transfer → another transfer within 5 minutes → fraud
- Temperature spike → pressure drop → vibration alert within 10 minutes → equipment failure

The Varpulis node detects these **temporal sequences** — patterns that only emerge when you look at events over time.

### How it works

The node runs a **VPL pattern** (a simple DSL for temporal patterns) and processes events through the [Varpulis CEP engine](https://github.com/varpulis/varpulis), compiled to **WebAssembly** — so it runs entirely in-process, no external services needed.

**Two outputs:**
- **Matches** — events that complete a pattern
- **Passthrough** — all events (for logging/archiving)

### Example: Stripe Churn Detection

```
[Stripe Webhook] → [Varpulis Pattern] → [Slack Alert]
```

VPL pattern in the node:
```
event Payment:
    customer_id: str
    status: str
    amount: float

stream ChurnRisk = Payment.where(status == "failed") as p1
    -> Payment.where(status == "failed" and customer_id == p1.customer_id) as p2
    -> Payment.where(status == "failed" and customer_id == p1.customer_id) as p3
    .within(1h)
    .emit(customer: p1.customer_id, failures: 3, alert: "churn risk")
```

### Install

```bash
npm install @varpulis/n8n-nodes-varpulis
```

Or from the n8n community nodes menu.

### Links

- [GitHub](https://github.com/varpulis/varpulis/tree/main/integrations/n8n-nodes-varpulis)
- [npm](https://www.npmjs.com/package/@varpulis/n8n-nodes-varpulis)
- [Varpulis Documentation](https://www.varpulis-cep.com/docs/)
- [VPL Language Tutorial](https://www.varpulis-cep.com/docs/tutorials/language-tutorial)

Would love feedback! What temporal patterns would be useful in your workflows?
