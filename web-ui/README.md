# Varpulis Control Plane Web UI

Full-featured operational dashboard for pipeline monitoring, metrics visualization, and cluster management.

![Dashboard](docs/screenshots/dashboard.png)

## Features

### Dashboard
Real-time cluster overview with summary cards, throughput charts, and alerts.

### Cluster Management
![Cluster](docs/screenshots/cluster-workers.png)

- View and manage worker nodes
- Interactive topology visualization
- Health monitoring with auto-refresh
- Worker drain and removal operations

### Pipeline Groups
![Pipelines](docs/screenshots/pipelines.png)

- Deploy, monitor, and teardown pipeline groups
- Multi-step deployment wizard
- Route configuration
- Pipeline placement visibility

### VPL Editor
![Editor](docs/screenshots/editor.png)

- Monaco-based editor with VPL syntax highlighting
- Auto-completion for keywords and operators
- Integrated event tester for debugging
- Save/load from local storage

### Metrics Dashboard
![Metrics](docs/screenshots/metrics.png)

- Grafana-style throughput charts
- Latency histogram with percentile markers
- Per-worker metrics breakdown
- Configurable time ranges

### Settings
![Settings](docs/screenshots/settings.png)

- Theme customization (dark/light)
- Connection configuration
- Refresh intervals
- Import/export settings

## Technology Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Framework** | Vue 3 (Composition API) | Reactive UI framework |
| **UI Library** | Vuetify 3 | Material Design components |
| **State** | Pinia | Centralized state management |
| **Charts** | Apache ECharts | Real-time data visualization |
| **Code Editor** | Monaco Editor | VPL syntax highlighting |
| **Topology Graph** | Vue Flow | Interactive node graphs |
| **Build Tool** | Vite | Fast development and builds |
| **HTTP Client** | Axios | API communication |
| **WebSocket** | reconnecting-websocket | Real-time updates |

## Quick Start

### Development

```bash
# Install dependencies
npm install

# Start development server
npm run dev
```

Open http://localhost:5173 in your browser.

### Production Build

```bash
# Type check and build
npm run build

# Preview production build
npm run preview
```

### Docker

```bash
# Build Docker image
docker build -t varpulis-ui .

# Run container
docker run -p 8080:8080 varpulis-ui
```

### Full Stack with Docker Compose

```bash
# From repository root
docker compose -f deploy/docker/docker-compose.cluster.yml up -d
```

This starts:
- **Coordinator**: Port 9100
- **Workers**: Ports 9000-9003
- **Web UI**: Port 8080
- **MQTT**: Port 1883
- **Prometheus**: Port 9091
- **Grafana**: Port 3000

## Project Structure

```
web-ui/
├── src/
│   ├── api/              # HTTP and WebSocket clients
│   │   ├── index.ts      # Axios instance with interceptors
│   │   ├── cluster.ts    # Coordinator API client
│   │   ├── pipelines.ts  # Pipeline API client
│   │   └── websocket.ts  # WebSocket client
│   ├── components/       # Vue components
│   │   ├── cluster/      # Worker cards, topology graph
│   │   ├── common/       # Shared UI components
│   │   ├── editor/       # VPL editor, event tester
│   │   ├── metrics/      # Charts and metric cards
│   │   └── pipelines/    # Pipeline list, deploy dialog
│   ├── composables/      # Vue composables
│   │   ├── usePolling.ts     # Periodic data fetching
│   │   ├── useWebSocket.ts   # WebSocket management
│   │   └── useNotifications.ts
│   ├── plugins/          # Vuetify, ECharts setup
│   ├── router/           # Vue Router configuration
│   ├── stores/           # Pinia state stores
│   │   ├── cluster.ts    # Workers, topology, alerts
│   │   ├── pipelines.ts  # Pipeline groups
│   │   ├── metrics.ts    # Time series data
│   │   ├── websocket.ts  # Connection state
│   │   └── settings.ts   # User preferences
│   ├── types/            # TypeScript definitions
│   └── views/            # Page components
├── docs/
│   ├── screenshots/      # UI screenshots
│   └── USER_GUIDE.md     # Detailed user guide
├── nginx/                # Production Nginx config
├── tests/                # Playwright tests
├── Dockerfile            # Multi-stage Docker build
└── playwright.config.ts  # Test configuration
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `BASE_URL` | Base URL for router | `/` |

### Vite Proxy Configuration

During development, API requests are proxied:
- `/api/*` → `http://localhost:9100`
- `/ws` → `ws://localhost:9100`

### Nginx Configuration

Production Nginx handles:
- SPA routing (all routes → `index.html`)
- API reverse proxy to coordinator
- WebSocket proxy
- Gzip compression
- Security headers
- Static asset caching

## Screenshots

Generate documentation screenshots:

```bash
npm run screenshots
```

Screenshots are saved to `docs/screenshots/`.

## Testing

```bash
# Run all tests
npm run test

# Run screenshot tests only
npm run screenshots
```

## API Integration

### REST Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/cluster/workers` | List workers |
| GET | `/api/v1/cluster/workers/{id}` | Get worker details |
| DELETE | `/api/v1/cluster/workers/{id}` | Remove worker |
| POST | `/api/v1/cluster/workers/{id}/drain` | Drain worker |
| GET | `/api/v1/cluster/pipeline-groups` | List pipeline groups |
| POST | `/api/v1/cluster/pipeline-groups` | Deploy new group |
| GET | `/api/v1/cluster/pipeline-groups/{id}` | Get group details |
| DELETE | `/api/v1/cluster/pipeline-groups/{id}` | Teardown group |
| POST | `/api/v1/cluster/pipeline-groups/{id}/inject` | Inject event |
| GET | `/api/v1/cluster/topology` | Get cluster topology |

### WebSocket Messages

**Client → Server:**
- `get_metrics` - Request current metrics
- `get_streams` - Request active streams
- `inject_event` - Inject test event
- `subscribe` / `unsubscribe` - Topic subscription

**Server → Client:**
- `metrics` - Metrics update
- `output_event` - Emitted event
- `worker_status` - Worker health change
- `alert` - System alert

## Documentation

- [User Guide](docs/USER_GUIDE.md) - Detailed feature documentation with screenshots

## Browser Support

- Chrome 90+
- Firefox 88+
- Safari 14+
- Edge 90+

## License

Apache 2.0
