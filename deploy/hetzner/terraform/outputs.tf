# =============================================================================
# Cluster access
# =============================================================================

output "kubeconfig" {
  description = "Kubeconfig for the k3s cluster (write to ~/.kube/config or pass to kubectl --kubeconfig)."
  value       = module.kube_hetzner.kubeconfig
  sensitive   = true
}

output "kubeconfig_file" {
  description = "Path hint: save kubeconfig with  terraform output -raw kubeconfig > kubeconfig.yaml"
  value       = "Run: terraform output -raw kubeconfig > kubeconfig.yaml"
}

# =============================================================================
# Networking
# =============================================================================

output "load_balancer_ip" {
  description = "Public IPv4 of the Hetzner load balancer (point DNS A records here)."
  value       = module.kube_hetzner.ingress_public_ipv4
}

output "load_balancer_ipv6" {
  description = "Public IPv6 of the Hetzner load balancer."
  value       = module.kube_hetzner.ingress_public_ipv6
}

# =============================================================================
# Node information
# =============================================================================

output "control_plane_ips" {
  description = "Public IPs of control-plane nodes."
  value       = module.kube_hetzner.control_planes_public_ipv4
}

output "agent_ips" {
  description = "Map of agent node pool name to public IPs."
  value       = module.kube_hetzner.agents_public_ipv4
}

output "enterprise_tenant_ips" {
  description = "Map of tenant_id to public IPv4 for dedicated enterprise nodes."
  value = {
    for id, server in hcloud_server.enterprise_tenant :
    id => server.ipv4_address
  }
}

# =============================================================================
# SSH key
# =============================================================================

output "ssh_key_id" {
  description = "Hetzner SSH key ID used by cluster nodes."
  value       = hcloud_ssh_key.default.id
}
