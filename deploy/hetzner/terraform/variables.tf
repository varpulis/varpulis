# =============================================================================
# Hetzner Cloud credentials & SSH
# =============================================================================

variable "hcloud_token" {
  description = "Hetzner Cloud API token (read/write)."
  type        = string
  sensitive   = true
}

variable "ssh_public_key_path" {
  description = "Path to the SSH public key uploaded to Hetzner."
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}

# =============================================================================
# Cluster settings
# =============================================================================

variable "cluster_name" {
  description = "Name of the k3s cluster."
  type        = string
  default     = "varpulis-saas"
}

variable "location" {
  description = "Hetzner datacenter location."
  type        = string
  default     = "nbg1"
}

variable "network_region" {
  description = "Hetzner network region (eu-central or us-east)."
  type        = string
  default     = "eu-central"
}

variable "microos_snapshot_id" {
  description = "ID of the openSUSE MicroOS snapshot built with kube-hetzner's Packer template."
  type        = string
}

# =============================================================================
# Node pool sizing
# =============================================================================

variable "shared_pool_count" {
  description = "Number of CX42 nodes in the shared (Free/Pro) pool."
  type        = number
  default     = 2

  validation {
    condition     = var.shared_pool_count >= 2 && var.shared_pool_count <= 3
    error_message = "shared_pool_count must be between 2 and 3."
  }
}

variable "enterprise_cx42_max" {
  description = "Max autoscaler nodes for Enterprise CX42 pool."
  type        = number
  default     = 5
}

variable "enterprise_cx52_max" {
  description = "Max autoscaler nodes for Enterprise CX52 pool."
  type        = number
  default     = 3
}

# =============================================================================
# Enterprise dedicated tenants (static, per-customer nodes)
# =============================================================================

variable "enterprise_dedicated_tenants" {
  description = "List of enterprise customers that get a dedicated, pre-provisioned node."
  type = list(object({
    tenant_id   = string
    server_type = string
  }))
  default = []

  # Example:
  # enterprise_dedicated_tenants = [
  #   { tenant_id = "acme",   server_type = "cx42" },
  #   { tenant_id = "globex", server_type = "cx52" },
  # ]
}

# =============================================================================
# Security
# =============================================================================

variable "kube_api_allowed_cidrs" {
  description = "CIDRs allowed to reach the Kubernetes API server."
  type        = list(string)
  default     = ["0.0.0.0/0", "::/0"]
}

# =============================================================================
# Upgrades
# =============================================================================

variable "auto_upgrade_k3s" {
  description = "Whether to enable automatic k3s upgrades via system-upgrade-controller."
  type        = bool
  default     = false
}
