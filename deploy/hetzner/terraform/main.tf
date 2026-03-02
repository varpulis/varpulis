terraform {
  required_version = ">= 1.5.0"

  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = ">= 1.45.0"
    }
  }
}

provider "hcloud" {
  token = var.hcloud_token
}

resource "hcloud_ssh_key" "default" {
  name       = "varpulis-k3s"
  public_key = file(var.ssh_public_key_path)
}

module "kube_hetzner" {
  source = "kube-hetzner/kube-hetzner/hcloud"

  hcloud_token = var.hcloud_token

  # ---------------------------------------------------------------------------
  # Cluster basics
  # ---------------------------------------------------------------------------
  cluster_name = var.cluster_name
  ssh_public_key = hcloud_ssh_key.default.id

  network_region = var.network_region

  # Use the lightweight MicroOS snapshot built by kube-hetzner's packer template.
  microos_x86_snapshot_id = var.microos_snapshot_id

  # ---------------------------------------------------------------------------
  # Control-plane / System pool  (1x CX32)
  # ---------------------------------------------------------------------------
  control_plane_nodepools = [
    {
      name        = "system"
      server_type = "cx32"
      location    = var.location
      labels      = ["pool=system"]
      taints      = []
      count       = 1
    }
  ]

  # ---------------------------------------------------------------------------
  # Agent (worker) node pools
  # ---------------------------------------------------------------------------
  agent_nodepools = [
    # -- Shared pool: Free / Pro tenant workloads --------------------------
    {
      name        = "shared"
      server_type = "cx42"
      location    = var.location
      labels      = ["pool=shared"]
      taints      = []
      count       = var.shared_pool_count
    },

    # -- Database pool: PostgreSQL + MinIO ---------------------------------
    {
      name        = "database"
      server_type = "cx32"
      location    = var.location
      labels      = ["pool=database"]
      taints      = ["dedicated=database:NoSchedule"]
      count       = 1
    },
  ]

  # ---------------------------------------------------------------------------
  # Autoscaler (Enterprise per-customer nodes spun up on demand)
  # ---------------------------------------------------------------------------
  autoscaler_nodepools = [
    {
      name        = "enterprise-cx42"
      server_type = "cx42"
      location    = var.location
      labels      = ["pool=enterprise"]
      taints      = ["dedicated=enterprise:NoSchedule"]
      min_nodes   = 0
      max_nodes   = var.enterprise_cx42_max
    },
    {
      name        = "enterprise-cx52"
      server_type = "cx52"
      location    = var.location
      labels      = ["pool=enterprise"]
      taints      = ["dedicated=enterprise:NoSchedule"]
      min_nodes   = 0
      max_nodes   = var.enterprise_cx52_max
    },
  ]

  # ---------------------------------------------------------------------------
  # Load balancer
  # ---------------------------------------------------------------------------
  load_balancer_type     = "lb11"
  load_balancer_location = var.location

  # ---------------------------------------------------------------------------
  # Networking & firewall
  # ---------------------------------------------------------------------------
  firewall_kube_api_source = var.kube_api_allowed_cidrs
  enable_kube_proxy_ipvs   = true

  # ---------------------------------------------------------------------------
  # Storage
  # ---------------------------------------------------------------------------
  enable_longhorn = false  # We use Hetzner CSI volumes directly.

  # ---------------------------------------------------------------------------
  # Ingress & DNS
  # ---------------------------------------------------------------------------
  ingress_controller = "traefik"

  traefik_additional_options = [
    "--entryPoints.websecure.http.tls=true",
  ]

  # ---------------------------------------------------------------------------
  # k3s configuration
  # ---------------------------------------------------------------------------
  allow_scheduling_on_control_plane = false

  additional_k3s_environment = {
    "K3S_RESOLV_CONF" = "/run/systemd/resolve/resolv.conf"
  }

  # ---------------------------------------------------------------------------
  # Labels applied to every node (overridden per-pool above)
  # ---------------------------------------------------------------------------
  automatically_upgrade_k3s = var.auto_upgrade_k3s

  # ---------------------------------------------------------------------------
  # DNS
  # ---------------------------------------------------------------------------
  dns_servers = [
    "185.12.64.1",   # Hetzner DNS
    "185.12.64.2",
  ]
}

# ---------------------------------------------------------------------------
# Enterprise tenant nodes (static, label-per-tenant)
#
# For customers that need a dedicated, pre-provisioned node rather than
# autoscaler-managed capacity, create instances of this resource via
# var.enterprise_dedicated_tenants.
# ---------------------------------------------------------------------------
resource "hcloud_server" "enterprise_tenant" {
  for_each = { for t in var.enterprise_dedicated_tenants : t.tenant_id => t }

  name        = "varpulis-ent-${each.value.tenant_id}"
  server_type = each.value.server_type
  location    = var.location
  image       = "ubuntu-22.04"
  ssh_keys    = [hcloud_ssh_key.default.id]

  labels = {
    pool   = "enterprise"
    tenant = each.value.tenant_id
  }

  lifecycle {
    ignore_changes = [image]
  }
}
