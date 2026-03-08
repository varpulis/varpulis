import { createApp } from 'vue'
import { createPinia } from 'pinia'
import vuetify from '@/plugins/vuetify'
import '@/plugins/echarts'
import router from './router'
import { setApiKey } from '@/api'
import App from './App.vue'

const app = createApp(App)

app.use(createPinia())
app.use(router)
app.use(vuetify)

app.config.errorHandler = (err, _instance, info) => {
  console.error(`[Varpulis] Unhandled error in ${info}:`, err)
}

app.mount('#app')

// Auto-populate API key for public demos (set at build time)
if (import.meta.env.VITE_API_KEY) {
  setApiKey(import.meta.env.VITE_API_KEY)
}
