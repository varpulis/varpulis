<template>
  <v-container>
    <v-row>
      <v-col cols="12">
        <h1 class="text-h4 mb-6">Billing</h1>
      </v-col>
    </v-row>

    <!-- Success banner after Stripe checkout -->
    <v-row v-if="showSuccess">
      <v-col cols="12">
        <v-alert type="success" closable @click:close="showSuccess = false">
          Your subscription is now active. Welcome to Varpulis Pro!
        </v-alert>
      </v-col>
    </v-row>

    <v-row>
      <!-- Current Plan -->
      <v-col cols="12" md="6">
        <v-card>
          <v-card-title>Current Plan</v-card-title>
          <v-card-text>
            <div class="text-h5 font-weight-bold mb-2">{{ plan.display_name }}</div>
            <div v-if="plan.event_limit" class="text-body-1 text-medium-emphasis">
              {{ formatNumber(plan.event_limit) }} events/month
            </div>
            <div v-else class="text-body-1 text-medium-emphasis">
              Unlimited events
            </div>
          </v-card-text>
          <v-card-actions>
            <v-btn
              v-if="plan.tier === 'free'"
              color="primary"
              variant="elevated"
              :loading="upgrading"
              @click="upgradeToTier('pro')"
            >
              Upgrade to Pro
            </v-btn>
            <v-btn
              v-if="plan.tier === 'pro'"
              color="primary"
              variant="elevated"
              :loading="upgrading"
              @click="upgradeToTier('business')"
            >
              Upgrade to Business
            </v-btn>
            <v-btn
              v-if="plan.tier !== 'free'"
              variant="outlined"
              :loading="loadingPortal"
              @click="openPortal"
            >
              Manage Billing
            </v-btn>
          </v-card-actions>
        </v-card>
      </v-col>

      <!-- Usage -->
      <v-col cols="12" md="6">
        <v-card>
          <v-card-title>Usage This Month</v-card-title>
          <v-card-text>
            <div class="text-h5 font-weight-bold mb-2">
              {{ formatNumber(totalEvents) }}
            </div>
            <div class="text-body-2 text-medium-emphasis mb-4">events processed</div>

            <v-progress-linear
              v-if="plan.event_limit"
              :model-value="usagePercent"
              :color="usagePercent > 80 ? 'error' : usagePercent > 60 ? 'warning' : 'primary'"
              height="8"
              rounded
            />
            <div v-if="plan.event_limit" class="text-caption text-medium-emphasis mt-1">
              {{ formatNumber(totalEvents) }} / {{ formatNumber(plan.event_limit) }}
              ({{ usagePercent.toFixed(1) }}%)
            </div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- Pricing Cards -->
    <v-row class="mt-6">
      <v-col cols="12">
        <h2 class="text-h5 mb-4">Plans</h2>
      </v-col>

      <v-col v-for="tier in tiers" :key="tier.name" cols="12" md="3">
        <v-card
          :variant="tier.name === plan.tier ? 'outlined' : 'elevated'"
          :color="tier.name === plan.tier ? 'primary' : undefined"
        >
          <v-card-title>{{ tier.display }}</v-card-title>
          <v-card-subtitle>{{ tier.price }}</v-card-subtitle>
          <v-card-text>
            <v-list density="compact">
              <v-list-item v-for="feature in tier.features" :key="feature">
                <template #prepend>
                  <v-icon size="small" color="success">mdi-check</v-icon>
                </template>
                {{ feature }}
              </v-list-item>
            </v-list>
          </v-card-text>
          <v-card-actions v-if="tier.name !== plan.tier">
            <v-btn
              v-if="tier.name === 'pro' && plan.tier === 'free'"
              color="primary"
              variant="elevated"
              block
              @click="upgradeToTier('pro')"
            >
              Upgrade
            </v-btn>
            <v-btn
              v-if="tier.name === 'business' && (plan.tier === 'free' || plan.tier === 'pro')"
              color="primary"
              variant="elevated"
              block
              @click="upgradeToTier('business')"
            >
              Upgrade
            </v-btn>
            <v-btn
              v-if="tier.name === 'enterprise'"
              variant="outlined"
              block
              href="mailto:sales@varpulis.com"
            >
              Contact Sales
            </v-btn>
          </v-card-actions>
          <v-card-actions v-else>
            <v-btn variant="text" block disabled>Current Plan</v-btn>
          </v-card-actions>
        </v-card>
      </v-col>
    </v-row>
  </v-container>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import axios from 'axios'

interface Plan {
  tier: string
  event_limit: number | null
  display_name: string
}

const plan = ref<Plan>({ tier: 'free', event_limit: 100000, display_name: 'Free' })
const totalEvents = ref(0)
const upgrading = ref(false)
const loadingPortal = ref(false)
const showSuccess = ref(false)

const usagePercent = computed(() => {
  if (!plan.value.event_limit) return 0
  return (totalEvents.value / plan.value.event_limit) * 100
})

const tiers = [
  {
    name: 'free',
    display: 'Free',
    price: '$0/month',
    features: ['100K events/month', '5 pipelines', '500 events/sec', 'Community support'],
  },
  {
    name: 'pro',
    display: 'Pro',
    price: '$49/month',
    features: ['10M events/month', '20 pipelines', '50K events/sec', 'All connectors'],
  },
  {
    name: 'business',
    display: 'Business',
    price: '$199/month',
    features: ['100M events/month', '100 pipelines', '200K events/sec', 'Advanced analytics'],
  },
  {
    name: 'enterprise',
    display: 'Enterprise',
    price: 'Custom',
    features: ['Unlimited events', '1000 pipelines', 'Dedicated cluster', 'SLA guarantee'],
  },
]

function formatNumber(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M'
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K'
  return n.toString()
}

async function fetchPlan() {
  try {
    const res = await axios.get('/api/v1/billing/plan')
    plan.value = res.data
  } catch {
    // Billing not configured, keep defaults
  }
}

async function fetchUsage() {
  try {
    const res = await axios.get('/api/v1/billing/usage')
    // Handle both new DB format and legacy in-memory format
    if (res.data.events_this_month !== undefined) {
      totalEvents.value = res.data.events_this_month
    } else if (res.data.usage && res.data.usage.length > 0) {
      totalEvents.value = res.data.usage[0].events_today
    }
  } catch {
    // Billing not configured
  }
}

async function upgradeToTier(tier: string) {
  upgrading.value = true
  try {
    const res = await axios.post('/api/v1/billing/checkout', {
      tier,
      success_url: window.location.origin + '/billing?success=true',
      cancel_url: window.location.origin + '/billing',
    })
    if (res.data.checkout_url) {
      window.location.href = res.data.checkout_url
    }
  } catch {
    // Handle error
  } finally {
    upgrading.value = false
  }
}

async function openPortal() {
  loadingPortal.value = true
  try {
    const res = await axios.post('/api/v1/billing/portal')
    if (res.data.portal_url) {
      window.location.href = res.data.portal_url
    }
  } catch {
    // Handle error
  } finally {
    loadingPortal.value = false
  }
}

onMounted(() => {
  // Check for success redirect from Stripe
  const params = new URLSearchParams(window.location.search)
  if (params.get('success') === 'true') {
    showSuccess.value = true
    // Clean URL
    const url = new URL(window.location.href)
    url.searchParams.delete('success')
    window.history.replaceState({}, '', url.pathname + url.search)
  }

  fetchPlan()
  fetchUsage()
})
</script>
