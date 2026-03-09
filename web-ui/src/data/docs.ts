import type { DocCategory } from '@/types/doc'

export const docCategories: DocCategory[] = [
  {
    name: 'Getting Started',
    icon: 'mdi-rocket-launch',
    pages: [
      { slug: 'tutorials/getting-started', title: 'Installation & First Program', category: 'Getting Started', order: 1 },
      { slug: 'tutorials/language-tutorial', title: 'VPL Language Tutorial', category: 'Getting Started', order: 2 },
      { slug: 'guides/configuration', title: 'Configuration Guide', category: 'Getting Started', order: 3 },
    ],
  },
  {
    name: 'VPL Language',
    icon: 'mdi-code-tags',
    pages: [
      { slug: 'language/overview', title: 'Overview', category: 'VPL Language', order: 1 },
      { slug: 'language/syntax', title: 'Syntax Reference', category: 'VPL Language', order: 2 },
      { slug: 'language/types', title: 'Type System', category: 'VPL Language', order: 3 },
      { slug: 'language/operators', title: 'Operators', category: 'VPL Language', order: 4 },
      { slug: 'language/builtins', title: 'Built-in Functions', category: 'VPL Language', order: 5 },
      { slug: 'language/connectors', title: 'Connectors', category: 'VPL Language', order: 6 },
      { slug: 'language/keywords', title: 'Reserved Keywords', category: 'VPL Language', order: 7 },
      { slug: 'language/grammar', title: 'Formal Grammar (PEG)', category: 'VPL Language', order: 8 },
    ],
  },
  {
    name: 'Tutorials',
    icon: 'mdi-school',
    pages: [
      { slug: 'tutorials/cluster-tutorial', title: 'Distributed Cluster Mode', category: 'Tutorials', order: 1 },
      { slug: 'tutorials/contexts-tutorial', title: 'Parallel Processing with Contexts', category: 'Tutorials', order: 2 },
      { slug: 'tutorials/checkpointing-tutorial', title: 'Checkpointing & Watermarks', category: 'Tutorials', order: 3 },
      { slug: 'tutorials/trend-aggregation-tutorial', title: 'Trend Aggregation', category: 'Tutorials', order: 4 },
      { slug: 'tutorials/forecasting-tutorial', title: 'Pattern Forecasting', category: 'Tutorials', order: 5 },
      { slug: 'tutorials/nats-connector', title: 'NATS Connector', category: 'Tutorials', order: 6 },
      { slug: 'tutorials/postgres-cdc-tutorial', title: 'PostgreSQL CDC', category: 'Tutorials', order: 7 },
      { slug: 'tutorials/outer-joins-tutorial', title: 'Outer Joins', category: 'Tutorials', order: 8 },
      { slug: 'tutorials/encryption-at-rest-tutorial', title: 'Encryption at Rest', category: 'Tutorials', order: 9 },
      { slug: 'tutorials/sso-oidc-tutorial', title: 'SSO / OIDC', category: 'Tutorials', order: 10 },
    ],
  },
  {
    name: 'Architecture',
    icon: 'mdi-sitemap',
    pages: [
      { slug: 'architecture/system', title: 'System Architecture', category: 'Architecture', order: 1 },
      { slug: 'architecture/cluster', title: 'Distributed Execution', category: 'Architecture', order: 2 },
      { slug: 'architecture/state-management', title: 'State Management', category: 'Architecture', order: 3 },
      { slug: 'architecture/trend-aggregation', title: 'Trend Aggregation', category: 'Architecture', order: 4 },
      { slug: 'architecture/forecasting', title: 'Forecasting', category: 'Architecture', order: 5 },
      { slug: 'architecture/nats-transport', title: 'NATS Transport', category: 'Architecture', order: 6 },
      { slug: 'architecture/authentication', title: 'Authentication', category: 'Architecture', order: 7 },
      { slug: 'architecture/multi-tenancy', title: 'Multi-Tenancy', category: 'Architecture', order: 8 },
      { slug: 'architecture/observability', title: 'Observability', category: 'Architecture', order: 9 },
    ],
  },
  {
    name: 'Guides',
    icon: 'mdi-book-open-variant',
    pages: [
      { slug: 'guides/sase-patterns', title: 'SASE+ Pattern Matching', category: 'Guides', order: 1 },
      { slug: 'guides/contexts', title: 'Context-Based Execution', category: 'Guides', order: 2 },
      { slug: 'guides/performance-tuning', title: 'Performance Tuning', category: 'Guides', order: 3 },
      { slug: 'guides/troubleshooting', title: 'Troubleshooting', category: 'Guides', order: 4 },
    ],
  },
  {
    name: 'Reference',
    icon: 'mdi-file-document',
    pages: [
      { slug: 'reference/cli-reference', title: 'CLI Reference', category: 'Reference', order: 1 },
      { slug: 'reference/windows-aggregations', title: 'Windows & Aggregations', category: 'Reference', order: 2 },
      { slug: 'reference/trend-aggregation', title: 'Trend Aggregation', category: 'Reference', order: 3 },
      { slug: 'reference/joins', title: 'Joins', category: 'Reference', order: 4 },
      { slug: 'reference/enrichment', title: 'Enrichment', category: 'Reference', order: 5 },
      { slug: 'reference/mcp-integration', title: 'MCP Integration', category: 'Reference', order: 6 },
    ],
  },
  {
    name: 'Operations',
    icon: 'mdi-cog',
    pages: [
      { slug: 'operations/runbook', title: 'Operations Runbook', category: 'Operations', order: 1 },
      { slug: 'operations/alerting', title: 'Alerting Reference', category: 'Operations', order: 2 },
      { slug: 'operations/slo', title: 'SLO/SLI Definitions', category: 'Operations', order: 3 },
      { slug: 'operations/capacity-planning', title: 'Capacity Planning', category: 'Operations', order: 4 },
      { slug: 'PRODUCTION_DEPLOYMENT', title: 'Production Deployment', category: 'Operations', order: 5 },
    ],
  },
]
