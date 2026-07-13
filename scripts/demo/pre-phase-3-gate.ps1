$ErrorActionPreference = "Stop"

Write-Host "=== MuleGraph Final Pre-Phase-3 Gate Demo ==="

Write-Host "1. Start Infrastructure"
docker compose down -v
docker compose up -d
Start-Sleep -Seconds 15

$Date = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$TxId = "55555555-5555-5555-5555-555555555555"

Write-Host "`n2. Exact Transaction Replay"
$Body = @"
{
  "schema_version": 1,
  "transaction_id": "$TxId",
  "source_system": "WEB",
  "source_account_id": "11111111-1111-1111-1111-111111111111",
  "destination_account_id": "22222222-2222-2222-2222-222222222222",
  "amount_minor": 1000,
  "currency": "USD",
  "occurred_at": "$Date"
}
"@
Invoke-RestMethod -Uri "http://localhost:8080/v1/transactions" -Method Post -Headers @{"X-API-KEY"="dev-local-api-key"; "Content-Type"="application/json"} -Body $Body | Out-Null
Write-Host "First insert complete."

Invoke-RestMethod -Uri "http://localhost:8080/v1/transactions" -Method Post -Headers @{"X-API-KEY"="dev-local-api-key"; "Content-Type"="application/json"} -Body $Body | Out-Null
Write-Host "Replay insert complete. Check DB for duplicates (should be 1)."

Write-Host "`n3. Conflicting Transaction Identity"
$ConflictBody = @"
{
  "schema_version": 1,
  "transaction_id": "$TxId",
  "source_system": "WEB",
  "source_account_id": "11111111-1111-1111-1111-111111111111",
  "destination_account_id": "33333333-3333-3333-3333-333333333333",
  "amount_minor": 9000,
  "currency": "USD",
  "occurred_at": "$Date"
}
"@
Invoke-RestMethod -Uri "http://localhost:8080/v1/transactions" -Method Post -Headers @{"X-API-KEY"="dev-local-api-key"; "Content-Type"="application/json"} -Body $ConflictBody | Out-Null
Write-Host "Conflict insert sent. Should be routed to dead-letter."

Write-Host "`nGate demo commands executed."
