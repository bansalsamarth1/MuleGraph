$ErrorActionPreference = "Stop"

Write-Host "MuleGraph Phase 4 - Circular Flow Confirmation Demo"
Write-Host "==================================================="

Write-Host "1. Starting infrastructure (Kafka, PostgreSQL, Neo4j)..."
docker compose up -d

Write-Host "2. Waiting for services to be ready..."
Start-Sleep -Seconds 15

Write-Host "3. Starting MuleGraph application (graph-projector profile is active)..."
$env:SPRING_PROFILES_ACTIVE="api,normalizer,ledger-projector,stream-worker,alert-projector,graph-projector"
Start-Process -NoNewWindow -FilePath "./mvnw.cmd" -ArgumentList "spring-boot:run" -PassThru | Set-Variable appProcess

Start-Sleep -Seconds 30

Write-Host "4. Seeding a circular flow: A -> B -> C"
$API_KEY = "dev-local-api-key"
$ACCOUNT_A = "a0000000-0000-0000-0000-00000000000a"
$ACCOUNT_B = "b0000000-0000-0000-0000-00000000000b"
$ACCOUNT_C = "c0000000-0000-0000-0000-00000000000c"

# A -> B
$bodyAB = @{
    transaction_id = "11111111-1111-1111-1111-111111111111"
    source_account_id = $ACCOUNT_A
    destination_account_id = $ACCOUNT_B
    amount_minor = 1000
    currency = "USD"
    device_id = "dev1"
    ip_hash = "ip1"
    occurred_at = "2026-07-14T10:00:00Z"
} | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8080/api/v1/transactions" -Method Post -Headers @{ "X-API-Key" = $API_KEY } -Body $bodyAB -ContentType "application/json"
Write-Host " (A -> B sent)"
Start-Sleep -Seconds 2

# B -> C
$bodyBC = @{
    transaction_id = "22222222-2222-2222-2222-222222222222"
    source_account_id = $ACCOUNT_B
    destination_account_id = $ACCOUNT_C
    amount_minor = 1000
    currency = "USD"
    device_id = "dev2"
    ip_hash = "ip2"
    occurred_at = "2026-07-14T10:05:00Z"
} | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8080/api/v1/transactions" -Method Post -Headers @{ "X-API-Key" = $API_KEY } -Body $bodyBC -ContentType "application/json"
Write-Host " (B -> C sent)"
Start-Sleep -Seconds 5

Write-Host "5. Sending candidate closing transaction: C -> A"
$bodyCA = @{
    transaction_id = "33333333-3333-3333-3333-333333333333"
    source_account_id = $ACCOUNT_C
    destination_account_id = $ACCOUNT_A
    amount_minor = 1000
    currency = "USD"
    device_id = "dev3"
    ip_hash = "ip3"
    occurred_at = "2026-07-14T10:10:00Z"
} | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8080/api/v1/transactions" -Method Post -Headers @{ "X-API-Key" = $API_KEY } -Body $bodyCA -ContentType "application/json"
Write-Host " (C -> A sent)"

Write-Host "Waiting for Circular Flow Confirmation in Neo4j and Alert projection..."
Start-Sleep -Seconds 15

Write-Host "6. Checking generated alerts in PostgreSQL..."
docker compose exec postgres psql -U muleuser -d mulegraph -c "SELECT deduplication_key, rule_type, detected_at, primary_account_id FROM alerts WHERE rule_type='CIRCULAR_FLOW';"

Write-Host "Demo complete! Shutting down."
Stop-Process -Id $appProcess.Id -Force
docker compose down
