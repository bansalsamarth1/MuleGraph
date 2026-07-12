#!/bin/bash
set -e

echo "=== MuleGraph Phase 0 Verification ==="

export PATH="/opt/homebrew/opt/openjdk@21/bin:$PATH"

echo "1. Git Version:"
git --version

echo "2. Java Version:"
java -version

echo "3. Docker Version:"
docker --version

echo "4. Docker Compose Version:"
docker compose version

echo "5. Python Version:"
python3 --version

echo "6. Docker Accessibility Test:"
docker info > /dev/null && echo "Docker is running and accessible." || echo "Docker is NOT accessible."

echo "=== End Verification ==="
