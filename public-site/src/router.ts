import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      name: 'landing',
      component: () => import('@/views/LandingView.vue'),
      meta: { title: 'Varpulis - Pattern Detection for Event Streams' },
    },
    {
      path: '/playground',
      name: 'playground',
      component: () => import('@/views/PlaygroundView.vue'),
      meta: { title: 'Playground' },
    },
    {
      path: '/pricing',
      name: 'pricing',
      component: () => import('@/views/PricingView.vue'),
      meta: { title: 'Pricing' },
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
      path: '/scenarios/iot-concurrent',
      name: 'iot-concurrent-demo',
      component: () => import('@/views/IotConcurrentDemoView.vue'),
      meta: { title: 'IoT Concurrent Processing Demo' },
    },
    {
      path: '/scenarios/ai-fraud-scoring',
      name: 'ai-fraud-scoring-demo',
      component: () => import('@/views/AiFraudScoringDemoView.vue'),
      meta: { title: 'AI Fraud Scoring Demo' },
    },
    {
      path: '/scenarios/multi-region',
      name: 'multi-region-demo',
      component: () => import('@/views/MultiRegionDemoView.vue'),
      meta: { title: 'Multi-Region Federation Demo' },
    },
    {
      path: '/login',
      name: 'login',
      component: () => import('@/views/LoginView.vue'),
      meta: { title: 'Sign In' },
    },
    {
      path: '/signup',
      name: 'signup',
      component: () => import('@/views/SignupView.vue'),
      meta: { title: 'Create Account' },
    },
    {
      path: '/verify-email',
      name: 'verify-email',
      component: () => import('@/views/VerifyEmailView.vue'),
      meta: { title: 'Verify Email' },
    },
    // Catch-all: redirect to landing (NOT dashboard)
    {
      path: '/:pathMatch(.*)*',
      redirect: '/',
    },
  ],
})

// Update document title — NO auth guard
router.beforeEach((to) => {
  const title = to.meta?.title as string | undefined
  document.title = title ? `${title} | Varpulis` : 'Varpulis'
})

export default router
