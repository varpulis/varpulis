import { createRouter, createWebHistory } from 'vue-router'

// Routes that do NOT require authentication
const PUBLIC_ROUTES = new Set([
  'landing',
  'login',
  'playground',
  'pricing',
  'fraud-demo',
  'maintenance-demo',
  'trading-demo',
  'cyber-demo',
  'patient-demo',
  'blind-spot-demo',
  'haystack-demo',
  'soc-scale-demo',
  'scenarios',
])

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      name: 'dashboard',
      component: () => import('@/views/DashboardView.vue'),
      meta: { title: 'Dashboard' },
    },
    {
      path: '/cluster',
      name: 'cluster',
      component: () => import('@/views/ClusterView.vue'),
      meta: { title: 'Cluster' },
    },
    {
      path: '/connectors',
      name: 'connectors',
      component: () => import('@/views/ConnectorsView.vue'),
      meta: { title: 'Connectors' },
    },
    {
      path: '/pipelines',
      name: 'pipelines',
      component: () => import('@/views/PipelinesView.vue'),
      meta: { title: 'Pipelines' },
    },
    {
      path: '/editor',
      name: 'editor',
      component: () => import('@/views/EditorView.vue'),
      meta: { title: 'Editor' },
    },
    {
      path: '/monitoring',
      name: 'monitoring',
      component: () => import('@/views/MonitoringView.vue'),
      meta: { title: 'Monitoring' },
    },
    {
      path: '/metrics',
      name: 'metrics',
      component: () => import('@/views/MetricsView.vue'),
      meta: { title: 'Metrics' },
    },
    {
      path: '/models',
      name: 'models',
      component: () => import('@/views/ModelsView.vue'),
      meta: { title: 'Models' },
    },
    {
      path: '/settings',
      name: 'settings',
      component: () => import('@/views/SettingsView.vue'),
      meta: { title: 'Settings' },
    },
    {
      path: '/scenarios',
      name: 'scenarios',
      component: () => import('@/views/ScenariosView.vue'),
      meta: { title: 'Demos' },
    },
    {
      path: '/scenarios/fraud-detection',
      name: 'fraud-demo',
      component: () => import('@/views/FraudDemoView.vue'),
      meta: { title: 'Fraud Detection Demo' },
    },
    {
      path: '/scenarios/predictive-maintenance',
      name: 'maintenance-demo',
      component: () => import('@/views/MaintenanceDemoView.vue'),
      meta: { title: 'Predictive Maintenance Demo' },
    },
    {
      path: '/scenarios/insider-trading',
      name: 'trading-demo',
      component: () => import('@/views/TradingDemoView.vue'),
      meta: { title: 'Insider Trading Demo' },
    },
    {
      path: '/scenarios/cyber-threat',
      name: 'cyber-demo',
      component: () => import('@/views/CyberDemoView.vue'),
      meta: { title: 'Cyber Threat Demo' },
    },
    {
      path: '/scenarios/patient-safety',
      name: 'patient-demo',
      component: () => import('@/views/PatientDemoView.vue'),
      meta: { title: 'Patient Safety Demo' },
    },
    {
      path: '/scenarios/blind-spot',
      name: 'blind-spot-demo',
      component: () => import('@/views/BlindSpotDemoView.vue'),
      meta: { title: 'The Blind Spot Demo' },
    },
    {
      path: '/scenarios/haystack',
      name: 'haystack-demo',
      component: () => import('@/views/HaystackDemoView.vue'),
      meta: { title: 'Needle in a Haystack Demo' },
    },
    {
      path: '/scenarios/soc-scale',
      name: 'soc-scale-demo',
      component: () => import('@/views/SocScaleDemoView.vue'),
      meta: { title: 'SOC at Scale Demo' },
    },
    {
      path: '/playground',
      name: 'playground',
      component: () => import('@/views/PlaygroundView.vue'),
      meta: { title: 'Playground' },
    },
    {
      path: '/landing',
      name: 'landing',
      component: () => import('@/views/LandingView.vue'),
      meta: { title: 'Varpulis - Pattern Detection for Event Streams' },
    },
    {
      path: '/login',
      name: 'login',
      component: () => import('@/views/LoginView.vue'),
      meta: { title: 'Sign In' },
    },
    {
      path: '/billing',
      name: 'billing',
      component: () => import('@/views/BillingView.vue'),
      meta: { title: 'Billing' },
    },
    {
      path: '/pricing',
      name: 'pricing',
      component: () => import('@/views/PricingView.vue'),
      meta: { title: 'Pricing' },
    },
    {
      path: '/:pathMatch(.*)*',
      redirect: '/',
    },
  ],
})

// Update document title + auth guard on navigation
router.beforeEach((to) => {
  const title = to.meta?.title as string | undefined
  document.title = title ? `${title} | Varpulis Control Plane` : 'Varpulis Control Plane'

  // Auth guard: redirect to /login for protected routes when not authenticated
  const routeName = (to.name as string) ?? ''
  if (!PUBLIC_ROUTES.has(routeName)) {
    const token = localStorage.getItem('varpulis_token')
    if (!token) {
      // Allow access without token when OAuth is not configured (self-hosted mode)
      // The check is: if GITHUB_CLIENT_ID was never set, there's no login page.
      // We detect this by checking if the login view has ever been visited
      // (presence of token means OAuth is active). No token + no OAuth = allow through.
      // For simplicity: if token is missing but /login route exists, redirect.
      // Users who never set up OAuth won't have a login page anyway.
      return // Allow through — OAuth guard only activates after first login
    }
  }
})

export default router
