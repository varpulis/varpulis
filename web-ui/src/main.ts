import { createApp } from 'vue'
import { createPinia } from 'pinia'
import vuetify from './plugins/vuetify'
import './plugins/echarts'
import router from './router'
import { useAuthStore } from './stores/auth'
import App from './App.vue'

const app = createApp(App)

app.use(createPinia())
app.use(router)
app.use(vuetify)

app.mount('#app')

// Initialize auth state after mount (validate stored token/session)
const authStore = useAuthStore()
authStore.handleOAuthCallback()
