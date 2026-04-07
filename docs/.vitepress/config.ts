import { defineConfig } from 'vitepress'
import { readFileSync } from 'fs'
import { resolve, dirname } from 'path'
import { fileURLToPath } from 'url'

const __dirname = dirname(fileURLToPath(import.meta.url))

// Load VPL TextMate grammar for syntax highlighting
const vplGrammar = JSON.parse(
  readFileSync(resolve(__dirname, 'vpl.tmLanguage.json'), 'utf-8')
)

export default defineConfig({
  title: 'Varpulis',
  description: 'Rust stream processing engine for real-time detection. Open-source Apache Flink alternative built for detection engineering, fraud prevention, and MITRE ATT&CK coverage. 1.5M events/sec. Single 15MB binary. No JVM.',
  lang: 'en-US',

  base: '/docs/',

  sitemap: {
    hostname: 'https://www.varpulis-cep.com',
    transformItems: (items) => items.map(item => ({
      ...item,
      url: item.url.startsWith('docs/') ? item.url : `docs/${item.url}`,
    })),
  },

  markdown: {
    languages: [
      {
        ...vplGrammar,
        name: 'vpl',
        aliases: ['varpulis'],
      },
    ],
  },

  ignoreDeadLinks: [
    /^http:\/\/localhost/,
    /\.\.\/\.\.\//,  // ignore links outside docs tree (../../)
    /\.\.\/demos\//,
    /\.\.\/reference\/(lsp|mcp)$/,
  ],

  head: [
    ['link', { rel: 'icon', href: '/docs/favicon.ico' }],

    // SEO meta tags — data-driven keyword targeting (stream processing + detection engineering wedge)
    ['meta', { name: 'keywords', content: 'stream processing, rust, apache flink alternative, detection engineering, detection as code, real-time detection, mitre att&ck, sigma rules, fraud detection, threat detection, pattern matching, streaming engine, event processing, soc automation, ai soc, siem alternative, no jvm, single binary, kafka, mqtt, nats' }],
    ['meta', { name: 'author', content: 'Varpulis' }],
    ['meta', { name: 'robots', content: 'index, follow, max-image-preview:large' }],

    // Open Graph
    ['meta', { property: 'og:type', content: 'website' }],
    ['meta', { property: 'og:site_name', content: 'Varpulis' }],
    ['meta', { property: 'og:title', content: 'Varpulis — Rust Stream Processing for Real-Time Detection' }],
    ['meta', { property: 'og:description', content: 'Open-source Rust stream processing engine. Apache Flink alternative for detection engineering, fraud prevention, and MITRE ATT&CK coverage. 1.5M events/sec. Single 15MB binary. No JVM.' }],
    ['meta', { property: 'og:url', content: 'https://www.varpulis-cep.com/docs/' }],

    // Twitter Card
    ['meta', { name: 'twitter:card', content: 'summary_large_image' }],
    ['meta', { name: 'twitter:title', content: 'Varpulis — Rust Stream Processing for Real-Time Detection' }],
    ['meta', { name: 'twitter:description', content: 'Open-source Rust stream processing engine. Apache Flink alternative for detection engineering, fraud prevention, and MITRE ATT&CK coverage.' }],
  ],

  themeConfig: {
    logo: '/logo.svg',

    nav: [
      { text: 'Home', link: '/' },
      { text: 'Getting Started', link: '/tutorials/getting-started' },
      { text: 'Language', link: '/language/overview' },
      { text: 'Architecture', link: '/architecture/system' },
      { text: 'Scenarios', link: '/scenarios/' },
      { text: 'Comparisons', link: '/comparisons/varpulis-vs-flink' },
      { text: 'GitHub', link: 'https://github.com/varpulis/varpulis' }
    ],

    sidebar: {
      '/tutorials/': [
        {
          text: 'Getting Started',
          items: [
            { text: 'Quick Start', link: '/tutorials/getting-started' },
            { text: 'Interactive Shell', link: '/tutorials/interactive-shell-tutorial' },
            { text: 'VPL Language Tutorial', link: '/tutorials/language-tutorial' },
          ]
        },
        {
          text: 'Core Features',
          items: [
            { text: 'Alert Notifications', link: '/tutorials/alert-tutorial' },
            { text: 'Contexts & Parallelism', link: '/tutorials/contexts-tutorial' },
            { text: 'Cluster Mode', link: '/tutorials/cluster-tutorial' },
            { text: 'Outer Joins', link: '/tutorials/outer-joins-tutorial' },
            { text: 'Checkpointing', link: '/tutorials/checkpointing-tutorial' },
          ]
        },
        {
          text: 'Advanced',
          items: [
            { text: 'Trend Aggregation', link: '/tutorials/trend-aggregation-tutorial' },
            { text: 'Forecasting', link: '/tutorials/forecasting-tutorial' },
            { text: 'Encryption at Rest', link: '/tutorials/encryption-at-rest-tutorial' },
          ]
        },
        {
          text: 'Connectors',
          items: [
            { text: 'NATS', link: '/tutorials/nats-connector' },
            { text: 'PostgreSQL CDC', link: '/tutorials/postgres-cdc-tutorial' },
            { text: 'SSO / OIDC', link: '/tutorials/sso-oidc-tutorial' },
          ]
        },
      ],

      '/language/': [
        {
          text: 'VPL Language',
          items: [
            { text: 'Overview', link: '/language/overview' },
            { text: 'Syntax', link: '/language/syntax' },
            { text: 'Types', link: '/language/types' },
            { text: 'Operators', link: '/language/operators' },
            { text: 'Keywords', link: '/language/keywords' },
            { text: 'Built-in Functions', link: '/language/builtins' },
            { text: 'Connectors', link: '/language/connectors' },
            { text: 'Grammar', link: '/language/grammar' },
          ]
        },
      ],

      '/architecture/': [
        {
          text: 'Architecture',
          items: [
            { text: 'System Overview', link: '/architecture/system' },
            { text: 'Cluster Mode', link: '/architecture/cluster' },
            { text: 'Parallelism', link: '/architecture/parallelism' },
            { text: 'State Management', link: '/architecture/state-management' },
            { text: 'Observability', link: '/architecture/observability' },
            { text: 'Output Event Relay', link: '/architecture/output-event-relay' },
          ]
        },
        {
          text: 'Connectors & Transport',
          items: [
            { text: 'NATS Transport', link: '/architecture/nats-transport' },
            { text: 'PostgreSQL CDC', link: '/architecture/postgres-cdc' },
          ]
        },
        {
          text: 'Advanced Engines',
          items: [
            { text: 'Trend Aggregation', link: '/architecture/trend-aggregation' },
            { text: 'Forecasting', link: '/architecture/forecasting' },
          ]
        },
        {
          text: 'SaaS & Security',
          items: [
            { text: 'Multi-Tenancy', link: '/architecture/multi-tenancy' },
            { text: 'Authentication', link: '/architecture/authentication' },
          ]
        },
      ],

      '/reference/': [
        {
          text: 'Reference',
          items: [
            { text: 'CLI Reference', link: '/reference/cli-reference' },
            { text: 'Interactive Protocol', link: '/reference/interactive-protocol' },
            { text: 'Windows & Aggregations', link: '/reference/windows-aggregations' },
            { text: 'Joins', link: '/reference/joins' },
            { text: 'Enrichment', link: '/reference/enrichment' },
            { text: 'Trend Aggregation', link: '/reference/trend-aggregation' },
            { text: 'MCP Integration', link: '/reference/mcp-integration' },
          ]
        },
      ],

      '/guides/': [
        {
          text: 'Guides',
          items: [
            { text: 'Configuration', link: '/guides/configuration' },
            { text: 'Debugging Pipelines', link: '/guides/debugging-pipelines' },
            { text: 'Schema Inference', link: '/guides/schema-inference' },
            { text: 'Performance Tuning', link: '/guides/performance-tuning' },
            { text: 'Contexts', link: '/guides/contexts' },
            { text: 'SASE Patterns', link: '/guides/sase-patterns' },
            { text: 'Connector Security', link: '/guides/connector-security' },
            { text: 'Stripe Setup', link: '/guides/stripe-setup' },
            { text: 'Troubleshooting', link: '/guides/troubleshooting' },
          ]
        },
      ],

      '/operations/': [
        {
          text: 'Operations',
          items: [
            { text: 'Runbook', link: '/operations/runbook' },
            { text: 'Alerting', link: '/operations/alerting' },
            { text: 'SLOs', link: '/operations/slo' },
            { text: 'Capacity Planning', link: '/operations/capacity-planning' },
            { text: 'TLS Certificates', link: '/operations/tls-certificate-management' },
          ]
        },
      ],

      '/scenarios/': [
        {
          text: 'Real-World Scenarios',
          items: [
            { text: 'Overview', link: '/scenarios/' },
            { text: 'Fraud Detection', link: '/scenarios/fraud-detection' },
            { text: 'Predictive Maintenance', link: '/scenarios/predictive-maintenance' },
            { text: 'Insider Trading', link: '/scenarios/insider-trading' },
            { text: 'Cyber Kill Chain', link: '/scenarios/cyber-kill-chain' },
            { text: 'Patient Safety', link: '/scenarios/patient-safety' },
          ]
        },
      ],

      '/examples/': [
        {
          text: 'Examples',
          items: [
            { text: 'HVAC Building', link: '/examples/hvac-building' },
            { text: 'Financial Markets', link: '/examples/financial-markets' },
          ]
        },
      ],

      '/adr/': [
        {
          text: 'Architecture Decision Records',
          items: [
            { text: 'Index', link: '/adr/' },
            { text: 'ADR-001: Pest Parser', link: '/adr/001-pest-parser' },
            { text: 'ADR-002: Warp HTTP', link: '/adr/002-warp-http' },
            { text: 'ADR-003: Coordinator/Worker', link: '/adr/003-coordinator-worker' },
            { text: 'ADR-004: SASE+ Semantics', link: '/adr/004-sase-plus-semantics' },
            { text: 'ADR-005: Hamlet Aggregation', link: '/adr/005-hamlet-trend-aggregation' },
          ]
        },
      ],

      '/spec/': [
        {
          text: 'Specification',
          items: [
            { text: 'Overview', link: '/spec/overview' },
            { text: 'Benchmarks', link: '/spec/benchmarks' },
            { text: 'Roadmap', link: '/spec/roadmap' },
            { text: 'Glossary', link: '/spec/glossary' },
          ]
        },
      ],

      '/development/': [
        {
          text: 'Development',
          items: [
            { text: 'Status', link: '/development/STATUS' },
            { text: 'MSRV Policy', link: '/development/MSRV_POLICY' },
            { text: 'Audit Report', link: '/development/AUDIT_REPORT' },
          ]
        },
      ],

      '/comparisons/': [
        {
          text: 'Comparisons',
          items: [
            { text: 'Varpulis vs Apache Flink', link: '/comparisons/varpulis-vs-flink' },
            { text: 'Varpulis vs Kafka Streams', link: '/comparisons/varpulis-vs-kafka-streams' },
            { text: 'Varpulis vs Esper (legacy)', link: '/comparisons/varpulis-vs-esper' },
          ]
        },
      ],
    },

    socialLinks: [
      { icon: 'discord', link: 'https://discord.gg/nVyctE8vPz' },
      { icon: 'github', link: 'https://github.com/varpulis/varpulis' }
    ],

    footer: {
      message: 'Varpulis - Next-generation streaming analytics engine',
      copyright: 'Copyright &copy; 2025-2026'
    },

    search: {
      provider: 'local'
    },

    outline: {
      level: [2, 3]
    },
  }
})
