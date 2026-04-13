#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <git-repo-url> [project-dir]"
  exit 1
fi

REPO_URL="$1"
PROJECT_DIR="${2:-event-streaming-platform}"

echo "[1/7] Cloning project..."
if [[ -d "$PROJECT_DIR/.git" ]]; then
  echo "Project already exists at $PROJECT_DIR, skipping clone."
else
  git clone "$REPO_URL" "$PROJECT_DIR"
fi

cd "$PROJECT_DIR"

echo "[2/7] Making init script executable..."
chmod +x init/create-topics.sh

echo "[3/7] Preparing environment file..."
if [[ ! -f .env && -f .env.example ]]; then
  cp .env.example .env
fi

echo "[4/7] Building Docker images..."
docker compose build

echo "[5/7] Starting containers..."
docker compose up -d

echo "[6/7] Current container status:"
docker compose ps

echo "[7/7] Helpful follow-up commands:"
echo "  cd $PROJECT_DIR"
echo "  docker compose logs -f pipeline"
echo "  docker compose logs -f processor"
echo "  docker compose logs init-topics --tail=100"
