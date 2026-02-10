#!/bin/bash
# Varpulis Demo - Server Bootstrap (run once with sudo)
set -e

echo "=== Varpulis Demo Server Bootstrap ==="

# Install Docker
apt-get update
apt-get install -y docker.io docker-compose

# Enable and start Docker
systemctl enable --now docker

# Add current user to docker group
usermod -aG docker "${SUDO_USER:-cpo}"

# Firewall: only HTTP, HTTPS, SSH
ufw allow 22/tcp
ufw allow 80/tcp
ufw allow 443/tcp
ufw --force enable

echo ""
echo "=== Bootstrap complete ==="
echo "Log out and back in for docker group to take effect."
