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
