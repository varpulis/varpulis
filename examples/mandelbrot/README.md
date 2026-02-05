# Mandelbrot Set Demo

Demonstrates VPL's `emit` statement and `.process()` operation by computing a
1000x1000 Mandelbrot set image in parallel across 16 contexts.

Each context computes a 250x250 tile, using nested `for` loops with `emit`
statements to generate 62,500 pixel events per tile (1,000,000 total). Events
are published to MQTT and rendered by a Python subscriber.

## Architecture

```
timer(1s) --> .process(compute_tile(...)) --> emit Pixel(...) --> MQTT --> render.py
   x16 contexts (4x4 grid of tiles)
```

## Prerequisites

- Docker (for the MQTT broker)
- Python 3.8+ with dependencies: `pip install -r examples/mandelbrot/requirements.txt`

## Quick Start

```bash
./examples/mandelbrot/run.sh
```

This starts the MQTT broker, Python renderer, Varpulis server, and deploys
`mandelbrot.vpl`. The image is saved to `examples/mandelbrot/mandelbrot.png`
when all 1,000,000 pixels have been received.

## Manual Setup

```bash
# 1. Start MQTT broker
docker run -d -p 1883:1883 eclipse-mosquitto

# 2. Install Python dependencies
pip install -r examples/mandelbrot/requirements.txt

# 3. Start the renderer (runs in background, saves to mandelbrot.png)
python examples/mandelbrot/render.py &

# 4. Start the Varpulis engine
cargo run --release -- server --port 9000 --api-key test

# 5. Deploy the Mandelbrot program
curl -X POST http://localhost:9000/api/v1/pipelines \
  -H "x-api-key: test" \
  -H "Content-Type: application/json" \
  -d '{"name":"mandelbrot","source":"<contents of mandelbrot.vpl>"}'
```

The renderer will display progress and save `mandelbrot.png` when all 1,000,000
pixels have been received.

## Web App (Real-time Rendering)

```bash
./examples/mandelbrot/run-web.sh
```

Then open http://localhost:8080 and click "Start Computation" to watch the
Mandelbrot set render in real-time via WebSocket.

Architecture:
```
timer(1s) --> .process() --> emit Pixel --> MQTT --> WebSocket --> Browser
              (16 tiles)                    (broker)  (bridge)    (canvas)
```

## Distributed Mode

The Mandelbrot demo can also run in distributed mode across 4 worker processes,
each handling one row of the 4x4 tile grid:

```bash
./examples/mandelbrot/distributed/deploy.sh
```

Architecture:
```
Coordinator (port 9100)
    ├── Worker 0 (port 9000): row0 → tiles (0,0)-(3,0)
    ├── Worker 1 (port 9001): row1 → tiles (0,1)-(3,1)
    ├── Worker 2 (port 9002): row2 → tiles (0,2)-(3,2)
    └── Worker 3 (port 9003): row3 → tiles (0,3)-(3,3)
```

Events are routed by the coordinator: `ComputeTile0*` to worker-0, `ComputeTile1*`
to worker-1, etc. See [`distributed/`](distributed/) and the
[Cluster Architecture](../../docs/architecture/cluster.md) docs.

### Benchmarking (single vs distributed)

```bash
python3 examples/mandelbrot/distributed/bench.py
```

## Key VPL Features Used

- **`emit` statement**: Generates multiple events from imperative code
  (loops, conditionals, function calls)
- **`.process()` operation**: Evaluates an expression for side effects,
  collecting emitted events as the stream output
- **`context` declarations**: Assigns streams to separate execution contexts
  for parallel processing
- **`timer()` source**: Triggers computation after a 1-second delay
- **`fn` declarations**: User-defined functions for mandelbrot iteration
  and tile computation
