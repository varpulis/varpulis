# Varpulis E2E Tests with k3d

This directory contains end-to-end tests for the Varpulis Flow Editor using k3d (Kubernetes in Docker).

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  k3d cluster                    │
│  ┌───────────────┐    ┌───────────────────────┐│
│  │  code-server  │    │   varpulis-runtime    ││
│  │  (VSCode Web) │───▶│   (test executor)     ││
│  └───────────────┘    └───────────────────────┘│
│         │                                       │
│         ▼                                       │
│  ┌───────────────┐                              │
│  │   mosquitto   │  (MQTT broker for tests)    │
│  └───────────────┘                              │
└─────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────┐
│           Playwright E2E Tests                  │
│  - Create/edit .vflow files                     │
│  - Export to .vpl                               │
│  - Execute VPL and verify output                │
└─────────────────────────────────────────────────┘
```

## Prerequisites

- k3d installed
- Docker running
- Node.js 18+ (for Playwright)

## Quick Start

```bash
# Create cluster and deploy
./setup.sh

# Run tests
npm test

# Cleanup
./teardown.sh
```

## Test Scenarios

1. **Flow Editor UI** - Create nodes, connect them, configure connectors
2. **VPL Export** - Export graph to valid VPL code
3. **VPL Execution** - Run exported VPL and verify output
4. **Round-trip** - Save .vflow, reload, verify integrity
