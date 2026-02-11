<script setup lang="ts">
import { ref } from 'vue'
import DemoShell from '@/components/demos/DemoShell.vue'
import NetworkMap from '@/components/demos/cyber/NetworkMap.vue'
import KillChainBar from '@/components/demos/cyber/KillChainBar.vue'
import { cyberThreatScenario } from '@/data/scenarios/cyber-threat'

const compromisedHosts = ref<string[]>([])
const attackPaths = ref<Array<{ from: string; to: string }>>([])
const activeStages = ref<string[]>([])

function addCompromised(host: string) {
  if (!compromisedHosts.value.includes(host)) {
    compromisedHosts.value.push(host)
  }
}

function addAttackPath(from: string, to: string) {
  if (!attackPaths.value.some((p) => p.from === from && p.to === to)) {
    attackPaths.value.push({ from, to })
  }
}

function activateStage(stageId: string) {
  if (!activeStages.value.includes(stageId)) {
    activeStages.value.push(stageId)
  }
}

function onAlerts(alerts: Array<Record<string, unknown>>) {
  for (const alert of alerts) {
    const alertType = String(alert.alert_type || '')

    if (alertType === 'brute_force_lateral') {
      addCompromised('web-server-01')
      addCompromised('file-server-02')
      addAttackPath('web-server-01', 'file-server-02')
      activateStage('recon')
      activateStage('access')
      activateStage('lateral')
    }

    if (alertType === 'dns_exfiltration') {
      addCompromised('workstation-15')
      activateStage('exfil')
    }

    if (alertType === 'privilege_escalation') {
      addCompromised('dev-box-03')
      activateStage('escalation')
    }
  }
}
</script>

<template>
  <DemoShell :scenario="cyberThreatScenario" @alerts="onAlerts">
    <template #hero>
      <div class="cyber-hero">
        <div class="cyber-hero__map">
          <NetworkMap
            :compromised-hosts="compromisedHosts"
            :attack-paths="attackPaths"
          />
        </div>
        <div class="cyber-hero__killchain">
          <KillChainBar :active-stages="activeStages" />
        </div>
      </div>
    </template>
  </DemoShell>
</template>

<style scoped>
.cyber-hero {
  display: flex;
  flex-direction: column;
  height: 100%;
  width: 100%;
}

.cyber-hero__map {
  flex: 1;
  min-height: 0;
}

.cyber-hero__killchain {
  flex-shrink: 0;
  height: 80px;
}
</style>
