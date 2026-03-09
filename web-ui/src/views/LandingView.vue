<script lang="ts">
export default { name: 'LandingView' }
</script>

<script setup lang="ts">
import { useRouter } from 'vue-router'
import { useTheme } from 'vuetify'
import { computed } from 'vue'

const router = useRouter()
const theme = useTheme()
const isDark = computed(() => theme.global.current.value.dark)

function toggleTheme() {
  theme.global.name.value = isDark.value ? 'light' : 'dark'
}

const heroVpl = `connector = stdin()

stream FraudAlert = login as l -> transfer as t .within(5m)
    .where(l.user_id == t.user_id && t.amount > 5000)
    .emit(alert: "Suspicious transfer", user: l.user_id, amount: t.amount)`

const flinkComparison = `// Apache Flink (Java) - 50+ lines
DataStream<Event> logins = env.addSource(kafka)
    .filter(e -> e.getType().equals("login"));
DataStream<Event> transfers = env.addSource(kafka)
    .filter(e -> e.getType().equals("transfer"));

Pattern<Event, ?> pattern = Pattern.<Event>begin("login")
    .where(new SimpleCondition<Event>() {
        public boolean filter(Event e) {
            return e.getType().equals("login");
        }
    })
    .followedBy("transfer")
    .where(new IterativeCondition<Event>() {
        public boolean filter(Event transfer, Context<Event> ctx) {
            Event login = ctx.getEventsForPattern("login")
                .iterator().next();
            return transfer.getUserId().equals(login.getUserId())
                && transfer.getAmount() > 5000;
        }
    })
    .within(Time.minutes(5));
// ... 20+ more lines for pattern select, sink, etc.`

const benchmarks = [
  { label: 'Event Throughput', value: '250K+', unit: 'events/sec', icon: 'mdi-lightning-bolt' },
  { label: 'Memory Efficiency', value: '3-16x', unit: 'less than Apama', icon: 'mdi-memory' },
  { label: 'Multi-Query', value: '100x', unit: 'faster (Hamlet)', icon: 'mdi-chart-timeline-variant-shimmer' },
  { label: 'Prediction Latency', value: '51ns', unit: 'per symbol', icon: 'mdi-crystal-ball' },
]

const useCases = [
  {
    title: 'Fraud Detection',
    icon: 'mdi-shield-alert',
    color: 'red',
    description: 'Detect multi-step fraud patterns in real-time with sequence matching and forecasting.',
    route: '/scenarios/fraud-detection',
  },
  {
    title: 'IoT Monitoring',
    icon: 'mdi-thermometer-alert',
    color: 'blue',
    description: 'Monitor sensor data, detect anomalies, and trigger alerts with window aggregation.',
    route: '/scenarios/predictive-maintenance',
  },
  {
    title: 'Trading Signals',
    icon: 'mdi-chart-line',
    color: 'green',
    description: 'Identify market patterns, volume spikes, and trading signals at sub-millisecond latency.',
    route: '/scenarios/insider-trading',
  },
  {
    title: 'Cybersecurity',
    icon: 'mdi-shield-lock',
    color: 'purple',
    description: 'Detect kill chains, brute force attacks, and lateral movement with sequence patterns.',
    route: '/scenarios/cyber-threat',
  },
]

const features = [
  { icon: 'mdi-language-rust', title: 'Built in Rust', desc: 'Zero-copy parsing, minimal allocations, no garbage collection pauses.' },
  { icon: 'mdi-connection', title: '11 Connectors', desc: 'Kafka, MQTT, NATS, Pulsar, Redis Streams, HTTP, WebSocket, and more.' },
  { icon: 'mdi-crystal-ball', title: 'PST Forecasting', desc: 'Predict next events using Prediction Suffix Trees. Sub-microsecond latency.' },
  { icon: 'mdi-server-network', title: 'Cluster Mode', desc: 'Distribute workloads across workers with coordinator-based orchestration.' },
  { icon: 'mdi-code-tags', title: 'Full LSP Support', desc: 'VS Code extension with completions, hover docs, diagnostics, and semantic tokens.' },
  { icon: 'mdi-monitor-dashboard', title: 'Web Dashboard', desc: 'Deploy, monitor, and manage pipelines through a modern web UI.' },
  { icon: 'mdi-chip', title: 'Concurrent Processing', desc: 'Partition events across worker threads with .concurrent() for linear scaling.' },
  { icon: 'mdi-brain', title: 'GPU ML Scoring', desc: 'Score events with ONNX models using GPU acceleration and batch inference.' },
  { icon: 'mdi-earth', title: 'Multi-Region Federation', desc: 'Federate clusters across regions with catalog sync and cross-region routing.' },
  { icon: 'mdi-domain', title: 'Multi-Tenant SaaS', desc: 'Hierarchical orgs, per-tenant isolation, RBAC, quotas, and pipeline inheritance.' },
]

function goToDemo(route: string) {
  router.push(route)
}
</script>

<template>
  <div class="landing-page">
    <!-- Top Nav -->
    <v-app-bar flat color="transparent" class="landing-nav">
      <v-app-bar-title class="font-weight-bold">
        <v-icon color="primary" class="mr-1">mdi-shield-check</v-icon>
        Varpulis
      </v-app-bar-title>
      <template #append>
        <v-btn variant="text" to="/scenarios">Demos</v-btn>
        <v-btn variant="text" to="/pricing">Pricing</v-btn>
        <v-btn variant="text" to="/docs">Docs</v-btn>
        <v-btn variant="text" to="/blog">Blog</v-btn>
        <v-btn variant="text" to="/playground">Playground</v-btn>
        <v-btn :icon="isDark ? 'mdi-weather-sunny' : 'mdi-weather-night'" variant="text" @click="toggleTheme" />
        <v-btn variant="outlined" class="ml-2" to="/login">Sign In</v-btn>
        <v-btn color="primary" class="ml-2" to="/signup">Get Started</v-btn>
      </template>
    </v-app-bar>

    <!-- Hero Section -->
    <section class="hero-section">
      <v-container class="text-center py-16">
        <h1 class="text-h2 font-weight-bold mb-4">
          Detect patterns in event streams.
          <br />
          <span class="text-primary">In 3 lines.</span>
        </h1>
        <p class="text-h6 text-medium-emphasis mb-8 mx-auto" style="max-width: 700px">
          Varpulis is a blazing-fast Complex Event Processing engine with its own domain language (VPL).
          Define patterns, detect fraud, monitor IoT, and forecast events — all declaratively.
        </p>

        <v-card class="mx-auto mb-8 text-left" max-width="800" variant="outlined">
          <v-card-text class="pa-0">
            <pre class="hero-code pa-4">{{ heroVpl }}</pre>
          </v-card-text>
        </v-card>

        <div class="d-flex justify-center ga-4 flex-wrap">
          <v-btn color="primary" size="large" to="/signup" prepend-icon="mdi-rocket-launch">
            Start Free Trial
          </v-btn>
          <v-btn variant="outlined" size="large" to="/playground" prepend-icon="mdi-play-circle">
            Try the Playground
          </v-btn>
          <v-btn variant="outlined" size="large" to="/scenarios" prepend-icon="mdi-presentation-play">
            Live Demos
          </v-btn>
        </div>
      </v-container>
    </section>

    <!-- Benchmarks -->
    <section class="py-12">
      <v-container>
        <h2 class="text-h4 font-weight-bold text-center mb-8">Performance that speaks for itself</h2>
        <v-row>
          <v-col v-for="b in benchmarks" :key="b.label" cols="12" sm="6" md="3">
            <v-card variant="tonal" class="text-center pa-6" height="100%">
              <v-icon size="40" color="primary" class="mb-3">{{ b.icon }}</v-icon>
              <div class="text-h3 font-weight-bold text-primary">{{ b.value }}</div>
              <div class="text-body-2 text-medium-emphasis">{{ b.unit }}</div>
              <div class="text-subtitle-2 mt-2">{{ b.label }}</div>
            </v-card>
          </v-col>
        </v-row>
      </v-container>
    </section>

    <!-- VPL vs Flink Comparison -->
    <section class="comparison-section py-12">
      <v-container>
        <h2 class="text-h4 font-weight-bold text-center mb-8">VPL vs. Apache Flink</h2>
        <v-row>
          <v-col cols="12" md="6">
            <v-card variant="outlined" class="h-100">
              <v-card-title class="d-flex align-center">
                <v-chip color="success" size="small" class="mr-2">VPL</v-chip>
                3 lines
              </v-card-title>
              <v-card-text>
                <pre class="comparison-code">{{ heroVpl }}</pre>
              </v-card-text>
            </v-card>
          </v-col>
          <v-col cols="12" md="6">
            <v-card variant="outlined" class="h-100">
              <v-card-title class="d-flex align-center">
                <v-chip color="grey" size="small" class="mr-2">Flink</v-chip>
                50+ lines
              </v-card-title>
              <v-card-text>
                <pre class="comparison-code" style="font-size: 0.7rem">{{ flinkComparison }}</pre>
              </v-card-text>
            </v-card>
          </v-col>
        </v-row>
      </v-container>
    </section>

    <!-- Use Cases -->
    <section class="py-12">
      <v-container>
        <h2 class="text-h4 font-weight-bold text-center mb-8">Built for real-world use cases</h2>
        <v-row>
          <v-col v-for="uc in useCases" :key="uc.title" cols="12" sm="6" md="3">
            <v-card
              variant="outlined"
              class="pa-6 h-100 use-case-card"
              @click="goToDemo(uc.route)"
              style="cursor: pointer"
            >
              <v-icon size="36" :color="uc.color" class="mb-3">{{ uc.icon }}</v-icon>
              <h3 class="text-h6 mb-2">{{ uc.title }}</h3>
              <p class="text-body-2 text-medium-emphasis">{{ uc.description }}</p>
              <v-btn variant="text" size="small" color="primary" class="mt-2 px-0" append-icon="mdi-arrow-right">
                Try it
              </v-btn>
            </v-card>
          </v-col>
        </v-row>
      </v-container>
    </section>

    <!-- Features -->
    <section class="features-section py-12">
      <v-container>
        <h2 class="text-h4 font-weight-bold text-center mb-8">Everything you need</h2>
        <v-row>
          <v-col v-for="f in features" :key="f.title" cols="12" sm="6" md="4">
            <div class="d-flex mb-6">
              <v-icon size="28" color="primary" class="mr-3 mt-1">{{ f.icon }}</v-icon>
              <div>
                <h4 class="text-subtitle-1 font-weight-bold">{{ f.title }}</h4>
                <p class="text-body-2 text-medium-emphasis">{{ f.desc }}</p>
              </div>
            </div>
          </v-col>
        </v-row>
      </v-container>
    </section>

    <!-- Final CTA -->
    <section class="cta-section py-16">
      <v-container class="text-center">
        <h2 class="text-h4 font-weight-bold mb-4">Ready to detect patterns?</h2>
        <p class="text-body-1 text-medium-emphasis mb-8">
          Get started in seconds with the interactive playground, or install the CLI.
        </p>
        <div class="d-flex justify-center ga-4 flex-wrap">
          <v-btn color="primary" size="large" to="/signup" prepend-icon="mdi-rocket-launch">
            Start Free Trial
          </v-btn>
          <v-btn variant="outlined" size="large" to="/playground" prepend-icon="mdi-play-circle">
            Try the Playground
          </v-btn>
        </div>
        <div class="mt-6">
          <code class="text-body-2 pa-2 rounded install-cmd">cargo install varpulis-cli</code>
        </div>
      </v-container>
    </section>

    <!-- Footer -->
    <v-footer class="py-6">
      <v-container>
        <v-row>
          <v-col cols="12" sm="4">
            <div class="text-subtitle-2 font-weight-bold mb-2">Product</div>
            <div><router-link to="/scenarios" class="text-body-2 text-medium-emphasis text-decoration-none">Demos</router-link></div>
            <div><router-link to="/playground" class="text-body-2 text-medium-emphasis text-decoration-none">Playground</router-link></div>
            <div><router-link to="/pricing" class="text-body-2 text-medium-emphasis text-decoration-none">Pricing</router-link></div>
          </v-col>
          <v-col cols="12" sm="4">
            <div class="text-subtitle-2 font-weight-bold mb-2">Resources</div>
            <div><router-link to="/docs" class="text-body-2 text-medium-emphasis text-decoration-none">Documentation</router-link></div>
            <div><router-link to="/blog" class="text-body-2 text-medium-emphasis text-decoration-none">Blog</router-link></div>
            <div><router-link to="/changelog" class="text-body-2 text-medium-emphasis text-decoration-none">Changelog</router-link></div>
          </v-col>
          <v-col cols="12" sm="4" class="text-sm-right">
            <div class="text-body-2 text-medium-emphasis">&copy; {{ new Date().getFullYear() }} Varpulis</div>
            <div class="text-body-2 text-medium-emphasis">Built with Rust</div>
          </v-col>
        </v-row>
      </v-container>
    </v-footer>
  </div>
</template>

<style scoped>
.landing-page {
  min-height: 100vh;
}

.landing-nav {
  backdrop-filter: blur(8px);
}

.hero-section {
  background: linear-gradient(135deg, rgba(var(--v-theme-primary), 0.05), rgba(var(--v-theme-primary), 0.02));
  min-height: 80vh;
  display: flex;
  align-items: center;
  padding-top: 64px;
}

.hero-code {
  font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
  font-size: 0.9rem;
  line-height: 1.6;
  background: rgba(var(--v-theme-surface-variant), 0.3);
  border-radius: 8px;
  white-space: pre-wrap;
  color: rgb(var(--v-theme-on-surface));
}

.comparison-section {
  background: rgba(var(--v-theme-surface-variant), 0.1);
}

.comparison-code {
  font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
  font-size: 0.8rem;
  line-height: 1.5;
  white-space: pre-wrap;
  max-height: 350px;
  overflow-y: auto;
}

.use-case-card:hover {
  border-color: rgb(var(--v-theme-primary));
  transform: translateY(-2px);
  transition: all 0.2s ease;
}

.features-section {
  background: rgba(var(--v-theme-surface-variant), 0.1);
}

.cta-section {
  background: linear-gradient(135deg, rgba(var(--v-theme-primary), 0.08), rgba(var(--v-theme-primary), 0.03));
}

.install-cmd {
  background: rgba(var(--v-theme-surface-variant), 0.5);
  font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
}
</style>
