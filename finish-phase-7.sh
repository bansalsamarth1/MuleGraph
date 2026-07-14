#!/bin/bash
set -e

echo "Loading image into kind..."
kind load docker-image mulegraph:v1 --name mulegraph-cluster

# 6. Install MuleGraph components
echo "6. Deploying MuleGraph application profiles..."

# Create API
helm install mulegraph-api ./k8s/charts/mulegraph \
  --set spring.profiles="api" \
  --set service.type=NodePort \
  --set nodePort=30080 \
  --set autoscaling.enabled=true \
  --set autoscaling.minReplicas=1 \
  --set autoscaling.maxReplicas=3 \
  --set createSecrets=true

# Create Ledger Projector
helm install mulegraph-ledger-projector ./k8s/charts/mulegraph \
  --set spring.profiles="ledger-projector"

# Create Stream Worker
helm install mulegraph-stream-worker ./k8s/charts/mulegraph \
  --set spring.profiles="stream-worker\,normalizer"

# Create Alert Projector
helm install mulegraph-alert-projector ./k8s/charts/mulegraph \
  --set spring.profiles="alert-projector"

# Create Graph Projector
helm install mulegraph-graph-projector ./k8s/charts/mulegraph \
  --set spring.profiles="graph-projector"

echo "Waiting for all MuleGraph pods to become ready..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=mulegraph --timeout=300s

# 7. End to End test
echo "7. Testing End-to-End processing..."
echo "Sending a transaction..."
curl -s -X POST http://localhost:8080/api/v1/transactions \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-local-api-key" \
  -d '{"account_id": "ACC-K8S-01", "amount_minor": 5000, "currency": "USD", "device_id": "DEV-K8S-01", "ip_hash": "IP-K8S-01"}' | jq .
sleep 10
kubectl logs -l app.kubernetes.io/instance=mulegraph-graph-projector --tail=20

# 8. Test Self Healing (API Pod Deletion)
echo "8. Testing Self-Healing..."
API_POD=$(kubectl get pods -l app.kubernetes.io/instance=mulegraph-api -o jsonpath='{.items[0].metadata.name}')
echo "Killing API Pod: $API_POD"
kubectl delete pod $API_POD
echo "Waiting for new API Pod to be ready..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/instance=mulegraph-api --timeout=120s

# 9. Test HPA
echo "9. Verifying HPA configuration..."
kubectl get hpa

# 10. Test StatefulSet Resiliency (Kill Neo4j)
echo "10. Testing StatefulSet Resiliency (Neo4j)..."
kubectl scale statefulset mulegraph-neo4j --replicas=0
sleep 5
echo "Neo4j stopped. Sending transaction..."
curl -s -X POST http://localhost:8080/api/v1/transactions \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-local-api-key" \
  -d '{"account_id": "ACC-K8S-02", "amount_minor": 7500, "currency": "USD", "device_id": "DEV-K8S-02", "ip_hash": "IP-K8S-02"}' | jq .
echo "Recovering Neo4j..."
kubectl scale statefulset mulegraph-neo4j --replicas=1
kubectl rollout status statefulset/mulegraph-neo4j --timeout=300s
sleep 15
echo "Checking graph projector recovery logs..."
kubectl logs -l app.kubernetes.io/instance=mulegraph-graph-projector --tail=50

# 11. Collect Evidence
echo "11. Collecting Evidence..."
mkdir -p docs/evidence/phase-7
kubectl get all > docs/evidence/phase-7/k8s-state.txt
kubectl get hpa >> docs/evidence/phase-7/k8s-state.txt

echo "Phase 7 Showcase Complete!"
echo "Run 'kind delete cluster --name mulegraph-cluster' to clean up."
