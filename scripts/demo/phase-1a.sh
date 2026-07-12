#!/bin/bash
set -e

echo "=== MuleGraph Phase 1A Verification ==="

export PATH="/opt/homebrew/opt/openjdk@21/bin:$PATH"

# Run tests
echo "1. Running unit tests..."
./mvnw clean test

# Start the application in the background
echo "2. Starting application..."
./mvnw spring-boot:run &
APP_PID=$!

# Wait for application to be healthy
echo "Waiting for application to become healthy..."
sleep 15

# Check endpoints
echo "3. Testing /actuator/health"
curl -s http://localhost:8080/actuator/health | grep -q '"status":"UP"' && echo "Health OK" || echo "Health FAILED"

echo "4. Testing /actuator/health/liveness"
curl -s http://localhost:8080/actuator/health/liveness | grep -q '"status":"UP"' && echo "Liveness OK" || echo "Liveness FAILED"

echo "5. Testing /actuator/health/readiness"
curl -s http://localhost:8080/actuator/health/readiness | grep -q '"status":"UP"' && echo "Readiness OK" || echo "Readiness FAILED"

# Stop the application
echo "6. Stopping application..."
kill $APP_PID
wait $APP_PID 2>/dev/null || true

echo "=== End Verification ==="
