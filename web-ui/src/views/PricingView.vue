<template>
  <v-container>
    <v-row justify="center">
      <v-col cols="12" class="text-center">
        <h1 class="text-h3 font-weight-bold mb-2">Pricing</h1>
        <p class="text-body-1 text-medium-emphasis mb-8">
          Start free. Scale as you grow.
        </p>
      </v-col>
    </v-row>

    <v-row justify="center">
      <v-col v-for="tier in tiers" :key="tier.name" cols="12" md="3">
        <v-card :variant="tier.highlighted ? 'outlined' : 'elevated'" :color="tier.highlighted ? 'primary' : undefined">
          <v-card-title class="text-h5">{{ tier.display }}</v-card-title>
          <v-card-subtitle class="text-h6">{{ tier.price }}</v-card-subtitle>
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
          <v-card-actions>
            <v-btn
              v-if="tier.name === 'free'"
              color="primary"
              variant="outlined"
              block
              href="/login"
            >
              Get Started
            </v-btn>
            <v-btn
              v-if="tier.name === 'pro'"
              color="primary"
              variant="elevated"
              block
              href="/login"
            >
              Start Free Trial
            </v-btn>
            <v-btn
              v-if="tier.name === 'business'"
              color="primary"
              variant="elevated"
              block
              href="/login"
            >
              Start Free Trial
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
        </v-card>
      </v-col>
    </v-row>
  </v-container>
</template>

<script setup lang="ts">
const tiers = [
  {
    name: 'free',
    display: 'Free',
    price: '$0/month',
    highlighted: false,
    features: ['100K events/month', '5 pipelines', '500 events/sec', 'Community support', '30-day trial'],
  },
  {
    name: 'pro',
    display: 'Pro',
    price: '$49/month',
    highlighted: true,
    features: [
      '10M events/month',
      '20 pipelines',
      '50K events/sec',
      'Priority support',
      'All connectors',
      'API key management',
    ],
  },
  {
    name: 'business',
    display: 'Business',
    price: '$199/month',
    highlighted: false,
    features: [
      '100M events/month',
      '100 pipelines',
      '200K events/sec',
      'Priority support',
      'All connectors',
      'Advanced analytics',
    ],
  },
  {
    name: 'enterprise',
    display: 'Enterprise',
    price: 'Custom',
    highlighted: false,
    features: [
      'Unlimited events',
      '1000 pipelines',
      '500K events/sec',
      'Dedicated cluster',
      'SLA guarantee',
      '24/7 support',
      'SSO/SAML',
    ],
  },
]
</script>
