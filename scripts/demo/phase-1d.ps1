$ErrorActionPreference = "Stop"

Write-Host "=== MuleGraph Phase 1D Validation Demo ==="

$Date = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

Write-Host "1. Valid Transaction (API to Validated)"
Invoke-RestMethod -Uri "http://localhost:8080/v1/transactions" -Method Post -Headers @{"X-API-KEY"="dev-local-api-key"; "Content-Type"="application/json"} -Body @"
{
    "schema_version": 1,
    "transaction_id": "00000000-0000-0000-0000-000000000001",
    "source_system": "WEB_UI",
    "source_account_id": "11111111-1111-1111-1111-111111111111",
    "destination_account_id": "22222222-2222-2222-2222-222222222222",
    "amount_minor": 1000,
    "currency": "USD",
    "device_id": "dev1",
    "ip_hash": "ip1",
    "occurred_at": "$Date"
}
"@ | Out-Null

Write-Host "2. Invalid Transaction (Invalid Schema, to transactions.invalid)"
Invoke-RestMethod -Uri "http://localhost:8080/v1/transactions" -Method Post -Headers @{"X-API-KEY"="dev-local-api-key"; "Content-Type"="application/json"} -Body @"
{
    "schema_version": 999,
    "transaction_id": "00000000-0000-0000-0000-000000000002",
    "source_system": "WEB_UI",
    "source_account_id": "11111111-1111-1111-1111-111111111111",
    "destination_account_id": "22222222-2222-2222-2222-222222222222",
    "amount_minor": -500,
    "currency": "USD",
    "occurred_at": "$Date"
}
"@ | Out-Null

Write-Host "3. Malformed JSON (to Dead Letter Topic)"
# Using Invoke-WebRequest as Invoke-RestMethod fails on malformed JSON server response sometimes
Try {
    Invoke-WebRequest -Uri "http://localhost:8080/v1/transactions" -Method Post -Headers @{"X-API-KEY"="dev-local-api-key"; "Content-Type"="application/json"} -Body "{ `"schema_version`": 1, `"transaction_id`": "
} Catch {
    # Expect 400 Bad Request
}

Write-Host "Done."
