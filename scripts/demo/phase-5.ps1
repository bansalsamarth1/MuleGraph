$ErrorActionPreference = "Stop"

Write-Host "MuleGraph Phase 5 - Metric Evaluation Demo"
Write-Host "=========================================="

Write-Host "1. Starting infrastructure (Kafka, PostgreSQL, Neo4j)..."
docker compose up -d

Write-Host "2. Waiting for services to be ready..."
Start-Sleep -Seconds 15

Write-Host "3. Starting MuleGraph Evaluation Harness (all profiles + evaluator)..."
$env:SPRING_PROFILES_ACTIVE="api,normalizer,ledger-projector,stream-worker,alert-projector,graph-projector,evaluator"
Start-Process -NoNewWindow -Wait -FilePath "./mvnw.cmd" -ArgumentList "spring-boot:run"

Write-Host "Evaluation Harness finished. Check docs/evidence/phase-5/evaluation_report.md for results."

Write-Host "Demo complete! Shutting down infrastructure."
docker compose down
