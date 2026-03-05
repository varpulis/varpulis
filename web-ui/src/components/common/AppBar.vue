<script setup lang="ts">
import { computed } from 'vue'
import { useAuthStore } from '@/stores/auth'
import { useOrgStore } from '@/stores/org'

const props = defineProps<{
  isDark: boolean
  wsConnected: boolean
}>()

const emit = defineEmits<{
  toggleTheme: []
  toggleDrawer: []
}>()

const authStore = useAuthStore()
const orgStore = useOrgStore()

const connectionColor = computed(() => (props.wsConnected ? 'success' : 'grey'))
const connectionText = computed(() => (props.wsConnected ? 'Live' : 'Offline'))

const userInitials = computed(() => {
  const name = authStore.user?.name || authStore.user?.login || '?'
  return name.slice(0, 2).toUpperCase()
})

const displayName = computed(() => authStore.user?.name || authStore.user?.login || 'Unknown')
const userRole = computed(() => authStore.user?.role || '')
const orgName = computed(() => orgStore.currentOrg?.name || '')
const orgTier = computed(() => orgStore.currentOrg?.tier || '')

async function handleLogout() {
  await authStore.logout()
  window.location.href = '/login'
}
</script>

<template>
  <v-app-bar elevation="1" density="comfortable">
    <v-app-bar-nav-icon @click="emit('toggleDrawer')" />

    <v-toolbar-title class="d-flex align-center">
      <v-icon class="mr-2" color="primary">mdi-lightning-bolt</v-icon>
      <span class="font-weight-bold">Varpulis</span>
      <span class="text-medium-emphasis ml-2">Control Plane</span>
    </v-toolbar-title>

    <v-spacer />

    <!-- Org Switcher -->
    <v-menu v-if="orgStore.organizations.length > 1" offset-y>
      <template #activator="{ props: orgMenuProps }">
        <v-chip
          v-bind="orgMenuProps"
          variant="tonal"
          size="small"
          class="mr-2"
          style="cursor: pointer"
        >
          <v-icon start size="small">mdi-domain</v-icon>
          {{ orgName }}
          <v-chip
            v-if="orgTier"
            size="x-small"
            variant="flat"
            color="primary"
            class="ml-1"
          >{{ orgTier }}</v-chip>
          <v-icon end size="small">mdi-chevron-down</v-icon>
        </v-chip>
      </template>
      <v-list density="compact" min-width="220">
        <v-list-subheader>Switch Organization</v-list-subheader>
        <v-list-item
          v-for="org in orgStore.organizations"
          :key="org.id"
          :active="org.id === orgStore.currentOrg?.id"
          @click="orgStore.switchOrg(org.id)"
        >
          <template #prepend>
            <v-icon size="small">mdi-domain</v-icon>
          </template>
          <v-list-item-title class="text-body-2">{{ org.name }}</v-list-item-title>
          <template #append>
            <v-chip size="x-small" variant="tonal">{{ org.tier }}</v-chip>
          </template>
        </v-list-item>
      </v-list>
    </v-menu>
    <!-- Single org display (no switcher needed) -->
    <v-chip
      v-else-if="orgName"
      variant="tonal"
      size="small"
      class="mr-2"
    >
      <v-icon start size="small">mdi-domain</v-icon>
      {{ orgName }}
      <v-chip
        v-if="orgTier"
        size="x-small"
        variant="flat"
        color="primary"
        class="ml-1"
      >{{ orgTier }}</v-chip>
    </v-chip>

    <!-- Connection Status -->
    <v-chip
      :color="connectionColor"
      variant="flat"
      size="small"
      class="mr-3"
    >
      <v-icon start size="small">
        {{ wsConnected ? 'mdi-wifi' : 'mdi-wifi-off' }}
      </v-icon>
      {{ connectionText }}
    </v-chip>

    <!-- Theme Toggle -->
    <v-btn
      icon
      variant="text"
      @click="emit('toggleTheme')"
    >
      <v-icon>{{ isDark ? 'mdi-weather-sunny' : 'mdi-weather-night' }}</v-icon>
      <v-tooltip activator="parent" location="bottom">
        {{ isDark ? 'Light mode' : 'Dark mode' }}
      </v-tooltip>
    </v-btn>

    <!-- Documentation Link -->
    <v-btn
      icon
      variant="text"
      href="https://github.com/your-org/varpulis"
      target="_blank"
    >
      <v-icon>mdi-github</v-icon>
      <v-tooltip activator="parent" location="bottom">
        Documentation
      </v-tooltip>
    </v-btn>

    <!-- User Menu -->
    <v-menu offset-y>
      <template #activator="{ props: menuProps }">
        <v-btn
          v-bind="menuProps"
          variant="text"
          class="ml-1"
          data-testid="user-menu-btn"
        >
          <v-avatar size="28" color="primary" class="mr-2">
            <span class="text-caption font-weight-bold">{{ userInitials }}</span>
          </v-avatar>
          <span class="d-none d-sm-inline text-body-2">{{ displayName }}</span>
          <v-icon end size="small">mdi-chevron-down</v-icon>
        </v-btn>
      </template>

      <v-card min-width="240">
        <v-card-text class="pb-2">
          <div class="d-flex align-center mb-2">
            <v-avatar size="36" color="primary" class="mr-3">
              <span class="text-body-2 font-weight-bold">{{ userInitials }}</span>
            </v-avatar>
            <div>
              <div class="text-body-2 font-weight-medium">{{ displayName }}</div>
              <div v-if="authStore.user?.email" class="text-caption text-medium-emphasis">{{ authStore.user.email }}</div>
            </div>
          </div>
          <v-chip v-if="userRole" size="x-small" variant="tonal" color="primary" class="mb-1">{{ userRole }}</v-chip>
        </v-card-text>

        <v-divider />

        <v-list density="compact">
          <v-list-item v-if="orgName" prepend-icon="mdi-domain" :subtitle="orgTier">
            <v-list-item-title class="text-body-2">{{ orgName }}</v-list-item-title>
          </v-list-item>
          <v-list-item
            v-for="org in orgStore.organizations.filter(o => o.id !== orgStore.currentOrg?.id)"
            :key="org.id"
            prepend-icon="mdi-swap-horizontal"
            @click="orgStore.switchOrg(org.id)"
          >
            <v-list-item-title class="text-body-2">Switch to {{ org.name }}</v-list-item-title>
          </v-list-item>
          <v-list-item prepend-icon="mdi-cog" to="/settings">
            <v-list-item-title class="text-body-2">Settings</v-list-item-title>
          </v-list-item>
        </v-list>

        <v-divider />

        <v-list density="compact">
          <v-list-item
            prepend-icon="mdi-logout"
            data-testid="logout-btn"
            @click="handleLogout"
          >
            <v-list-item-title class="text-body-2">Sign out</v-list-item-title>
          </v-list-item>
        </v-list>
      </v-card>
    </v-menu>
  </v-app-bar>
</template>
