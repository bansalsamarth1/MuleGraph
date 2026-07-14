#!/usr/bin/env bash
set -e

echo "===================================================="
echo " MuleGraph Phase 7: Kubernetes Deployment Showcase "
echo "===================================================="

# 1. Install dependencies
echo "1. Checking dependencies (kind, helm, kubectl)..."
if ! command -v brew &> /dev/null; then
    echo "Homebrew is required. Please install it."
    exit 1
fi

if ! command -v kind &> /dev/null; then
    echo "Installing kind..."
    brew install kind
fi

if ! command -v helm &> /dev/null; then
    echo "Installing helm..."
    brew install helm
fi

if ! command -v kubectl &> /dev/null; then
    echo "Installing kubectl..."
    brew install kubectl
fi

# 2. Create Cluster
echo "2. Creating kind cluster..."
if kind get clusters | grep -q mulegraph-cluster; then
    echo "Cluster mulegraph-cluster already exists. Deleting it..."
    kind delete cluster --name mulegraph-cluster
fi
kind create cluster --config k8s/kind-config.yaml --name mulegraph-cluster

# 3. Install metrics server (for HPA)
echo "3. Installing metrics-server..."
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
kubectl patch deployment metrics-server -n kube-system -p '{"spec":{"template":{"spec":{"containers":[{"name":"metrics-server","args":["--cert-dir=/tmp","--secure-port=4443","--kubelet-insecure-tls","--kubelet-preferred-address-types=InternalIP,ExternalIP,Hostname","--kubelet-use-node-status-port","--metric-resolution=15s"]}]}}}}'

# 4. Install Stateful Dependencies
echo "4. Installing stateful dependencies (Postgres, Kafka, Neo4j)..."
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add neo4j https://helm.neo4j.com/neo4j
helm repo update

# Install Postgres
helm install mulegraph-postgresql bitnami/postgresql \
  --set auth.postgresPassword=mulegraph \
  --set auth.database=mulegraph \
  --set primary.persistence.size=1Gi \
  --wait --timeout 5m

# Install Kafka using Strimzi Operator (Verified Kubernetes-native approach)
echo "Installing Strimzi Kafka Operator..."
helm repo add strimzi https://strimzi.io/charts/
helm repo update
helm install strimzi-kafka-operator strimzi/strimzi-kafka-operator --namespace default --wait --timeout 5m

echo "Waiting for Strimzi CRDs to be established..."
sleep 10
kubectl wait crd/kafkas.kafka.strimzi.io --for=condition=Established --timeout=300s
kubectl wait crd/kafkanodepools.kafka.strimzi.io --for=condition=Established --timeout=300s

echo "Deploying Kafka Cluster (KRaft mode via Strimzi)..."
cat <<EOF | kubectl apply -f -
apiVersion: kafka.strimzi.io/v1
kind: KafkaNodePool
metadata:
  name: dual-role
  labels:
    strimzi.io/cluster: mulegraph-cluster
spec:
  replicas: 1
  roles:
    - controller
    - broker
  storage:
    type: jbod
    volumes:
      - id: 0
        type: persistent-claim
        size: 1Gi
        deleteClaim: false
---
apiVersion: kafka.strimzi.io/v1
kind: Kafka
metadata:
  name: mulegraph-cluster
  annotations:
    strimzi.io/node-pools: enabled
    strimzi.io/kraft: enabled
spec:
  kafka:
    version: 4.3.0
    metadataVersion: 4.3-IV0
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
  entityOperator:
    topicOperator: {}
    userOperator: {}
EOF

echo "Waiting for Kafka cluster to become ready..."
kubectl wait kafka/mulegraph-cluster --for=condition=Ready --timeout=300s -n default

# Install Neo4j
helm install mulegraph-neo4j neo4j/neo4j \
  --set neo4j.name=mulegraph-neo4j \
  --set neo4j.password=mulegraph \
  --set volumes.data.mode=defaultStorageClass \
  --set volumes.data.defaultStorageClass.requests.storage=1Gi \
  --wait --timeout 5m

# 5. Build and Load Docker Image
echo "5. Building MuleGraph Docker image..."
export JAVA_HOME=$(brew --prefix openjdk@21)/libexec/openjdk.jdk/Contents/Home
./mvnw clean package -DskipTests
docker build -t mulegraph:v1 .

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
  --set spring.profiles="stream-worker\\,normalizer"

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

# 10. Test DB Outage (Neo4j)
echo "10. Simulating Neo4j Outage..."
kubectl scale statefulset mulegraph-neo4j --replicas=0
sleep 5
echo "Neo4j stopped. Sending transaction..."
curl -s -X POST http://localhost:8080/api/v1/transactions \
  -H "Content-Type: application/json" \
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
