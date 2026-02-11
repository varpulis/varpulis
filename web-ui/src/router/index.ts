import { createRouter, createWebHistory } from 'vue-router'

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
      path: '/metrics',
      name: 'metrics',
      component: () => import('@/views/MetricsView.vue'),
      meta: { title: 'Metrics' },
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
      path: '/:pathMatch(.*)*',
      redirect: '/',
    },
  ],
})

// Update document title on navigation
router.beforeEach((to) => {
  const title = to.meta?.title as string | undefined
  document.title = title ? `${title} | Varpulis Control Plane` : 'Varpulis Control Plane'
})

export default router
