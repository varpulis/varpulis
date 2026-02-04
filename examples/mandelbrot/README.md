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
