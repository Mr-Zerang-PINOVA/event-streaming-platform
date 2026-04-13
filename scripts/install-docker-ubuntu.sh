#!/usr/bin/env bash
set -euo pipefail

echo "[1/6] Updating apt packages..."
sudo apt update

echo "[2/6] Installing base dependencies..."
sudo apt install -y ca-certificates curl gnupg git lsb-release

echo "[3/6] Adding Docker GPG key..."
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo "[4/6] Adding Docker apt repository..."
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

echo "[5/6] Installing Docker Engine and Compose plugin..."
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

echo "[6/6] Enabling Docker..."
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -aG docker "$USER"

echo
echo "Docker installation completed."
echo "Log out and log back in, then verify with:"
echo "  docker --version"
echo "  docker compose version"
