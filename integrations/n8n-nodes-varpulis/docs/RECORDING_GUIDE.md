# n8n Demo Recording Guide

Record a ~60 second screen recording showing Varpulis detecting payment fraud in n8n.

## Setup

```bash
# 1. Build the node
cd integrations/n8n-nodes-varpulis
npm run build

# 2. Install into n8n
export N8N_USER_FOLDER="/tmp/n8n-demo"
mkdir -p "$N8N_USER_FOLDER/.n8n/nodes"
cd "$N8N_USER_FOLDER/.n8n/nodes"
npm init -y && npm install /path/to/cep/integrations/n8n-nodes-varpulis

# 3. Start n8n
npx n8n start
# Open http://localhost:5678
# Create account if first time
```

## Recording Steps

Start your screen recorder (OBS, macOS screen record, etc.) at **1400x900** or similar.

### Step 1: Create Workflow (5 sec)
- Click **"Start from scratch"** (or **+** → New workflow)
- **Screenshot: `docs/01-empty-canvas.png`**

### Step 2: Add Manual Trigger (5 sec)
- Click **"Add first step..."** (the dashed + box)
- Type **"Manual"** → click **"Manual Trigger"**
- **Screenshot: `docs/02-trigger-added.png`**

### Step 3: Add Varpulis Node (10 sec)
- Click the **+** button on the right edge of the Manual Trigger node
- Type **"Varpulis"** in the search
- You should see **"Varpulis CEP"** with a ⚡ lightning icon
- **Screenshot: `docs/03-varpulis-search.png`**
- Click **"Varpulis CEP"** → it gets added to the canvas, connected to the trigger
- **Screenshot: `docs/04-varpulis-on-canvas.png`**

### Step 4: Configure the Pattern (15 sec)
- Double-click the **Varpulis CEP** node to open its config
- In **"VPL Pattern"**, paste this:

```
event Payment:
    customer_id: str
    status: str
    amount: float

stream ChurnRisk = Payment as p1
    -> Payment where status == "failed" and customer_id == p1.customer_id as p2
    -> Payment where status == "failed" and customer_id == p1.customer_id as p3
    .within(60s)
    .where(p1.status == "failed")
    .emit(customer: p1.customer_id, failures: 3, alert: "churn risk")
```

- Set **"Event Type"** to: `Payment`
- **Screenshot: `docs/05-varpulis-config.png`** (show the full config panel with VPL visible)
- Click **"Back to canvas"** (or X to close)

### Step 5: Pin Test Data (10 sec)
- Double-click the **Manual Trigger** node
- Go to the **"Output"** tab
- Click **"Pin data"** (or the pin icon)
- Paste this JSON array:

```json
[
  {"customer_id": "cust-42", "status": "failed", "amount": 99.99},
  {"customer_id": "cust-42", "status": "failed", "amount": 149.00},
  {"customer_id": "cust-42", "status": "failed", "amount": 50.00},
  {"customer_id": "cust-99", "status": "success", "amount": 200.00}
]
```

- Click **"Save"**
- **Screenshot: `docs/06-pinned-data.png`**
- Close the panel

### Step 6: Execute Workflow (10 sec)
- Click the red **"Execute workflow"** button at the bottom
- Wait for execution to complete (should be instant)
- **Screenshot: `docs/07-execution-complete.png`** (should show green checkmarks on nodes)

### Step 7: View Results (10 sec)
- Double-click the **Varpulis CEP** node to see output
- The **"Matches"** tab should show 1 item with `customer: "cust-42", failures: 3, alert: "churn risk"`
- The **"Passthrough"** tab should show all 4 items
- **Screenshot: `docs/08-matches-output.png`** (this is the money shot!)
- **Screenshot: `docs/09-passthrough-output.png`** (optional)

### Step 8: Stop Recording

## Files to Create

Put screenshots in `integrations/n8n-nodes-varpulis/docs/`:

| File | What It Shows |
|------|--------------|
| `01-empty-canvas.png` | Fresh workflow canvas |
| `02-trigger-added.png` | Manual Trigger node on canvas |
| `03-varpulis-search.png` | "Varpulis CEP" in node search |
| `04-varpulis-on-canvas.png` | Both nodes connected on canvas |
| `05-varpulis-config.png` | VPL pattern in config panel (**key shot**) |
| `06-pinned-data.png` | Test data pinned to trigger |
| `07-execution-complete.png` | Green checkmarks after execution |
| `08-matches-output.png` | **THE MONEY SHOT** — pattern match result |
| `n8n-demo.gif` | Full recording as GIF (optional, from screen recorder) |

## After Recording

The README already references `docs/n8n-demo.gif` and `docs/n8n-varpulis-search.png`. Replace them with your new files. Key images for README:

1. **Hero GIF** → `docs/n8n-demo.gif` — the full 60-second recording
2. **Config screenshot** → `docs/05-varpulis-config.png` — shows VPL in n8n
3. **Results screenshot** → `docs/08-matches-output.png` — shows pattern match

Update the README image references if you rename files.

## Tips

- Use a clean browser profile (no extensions, no bookmarks bar)
- Set browser zoom to 100%
- Window size ~1400x900
- Dark mode or light mode — your preference, but be consistent
- Move slowly and deliberately — the recording should be watchable at 1x speed
- If using OBS: record at 30fps, export as GIF with `ffmpeg -i recording.mp4 -vf "fps=10,scale=900:-1" docs/n8n-demo.gif`
