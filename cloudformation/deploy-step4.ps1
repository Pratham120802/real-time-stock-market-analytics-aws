$ErrorActionPreference = "Stop"

$StackName = "stock-market-athena"
$Region = "us-east-1"
$TemplateFile = Join-Path $PSScriptRoot "04-athena-analytics.yaml"

Write-Host "Deploying Step 4: Glue Data Catalog and Athena" -ForegroundColor Cyan

aws sts get-caller-identity --region $Region | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "AWS credentials are unavailable or invalid."
}

aws cloudformation deploy `
    --stack-name $StackName `
    --template-file $TemplateFile `
    --parameter-overrides `
        ProjectName=stock-market-analytics `
        Environment=development `
        ProjectionStartYear=2026 `
        ProjectionEndYear=2035 `
    --region $Region `
    --no-fail-on-empty-changeset

if ($LASTEXITCODE -ne 0) {
    throw "Athena stack deployment failed."
}

$Outputs = aws cloudformation describe-stacks `
    --stack-name $StackName `
    --region $Region `
    --query "Stacks[0].Outputs" `
    --output json | ConvertFrom-Json

$DatabaseName = ($Outputs | Where-Object OutputKey -eq "AthenaDatabaseName").OutputValue
$WorkGroupName = ($Outputs | Where-Object OutputKey -eq "AthenaWorkGroupName").OutputValue
$ResultsBucketName = ($Outputs | Where-Object OutputKey -eq "AthenaResultsBucketName").OutputValue
$RawBucketName = aws cloudformation describe-stacks `
    --stack-name "stock-market-storage" `
    --region $Region `
    --query "Stacks[0].Outputs[?OutputKey=='RawBucketName'].OutputValue | [0]" `
    --output text

if (-not $DatabaseName -or -not $WorkGroupName -or -not $ResultsBucketName -or -not $RawBucketName) {
    throw "One or more Athena stack outputs are missing."
}

$UtcNow = [DateTime]::UtcNow
$LatestRawKey = aws s3api list-objects-v2 `
    --bucket $RawBucketName `
    --prefix "raw/" `
    --region $Region `
    --query "reverse(sort_by(Contents,&LastModified))[0].Key" `
    --output text

if ($LatestRawKey -match '^raw/year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})/') {
    $Year = $Matches[1]
    $Month = $Matches[2]
    $Day = $Matches[3]
    $Hour = $Matches[4]
} else {
    $Year = $UtcNow.ToString("yyyy")
    $Month = $UtcNow.ToString("MM")
    $Day = $UtcNow.ToString("dd")
    $Hour = $UtcNow.ToString("HH")
    Write-Warning "No existing raw object was found; the test query will use the current UTC partition."
}
$Query = @"
SELECT
  payload.symbol AS symbol,
  payload.price AS price,
  payload.volume AS volume,
  payload.timestamp AS event_timestamp,
  payload.source AS source
FROM raw_stock_events
WHERE year = '$Year'
  AND month = '$Month'
  AND day = '$Day'
  AND hour = '$Hour'
ORDER BY payload.timestamp DESC
LIMIT 20
"@

Write-Host "Running a partition-filtered test query for $Year-$Month-$Day hour $Hour UTC..." -ForegroundColor Yellow
$QueryExecutionId = aws athena start-query-execution `
    --query-string $Query `
    --query-execution-context "Database=$DatabaseName,Catalog=AwsDataCatalog" `
    --work-group $WorkGroupName `
    --region $Region `
    --query "QueryExecutionId" `
    --output text

if ($LASTEXITCODE -ne 0 -or -not $QueryExecutionId) {
    throw "Athena test query could not be started."
}

$State = "QUEUED"
for ($Attempt = 1; $Attempt -le 30; $Attempt++) {
    $Execution = aws athena get-query-execution `
        --query-execution-id $QueryExecutionId `
        --region $Region `
        --output json | ConvertFrom-Json
    $State = $Execution.QueryExecution.Status.State
    if ($State -in @("SUCCEEDED", "FAILED", "CANCELLED")) {
        break
    }
    Start-Sleep -Seconds 2
}

if ($State -ne "SUCCEEDED") {
    $Reason = $Execution.QueryExecution.Status.StateChangeReason
    throw "Athena test query ended in state $State. $Reason"
}

aws athena get-query-results `
    --query-execution-id $QueryExecutionId `
    --region $Region `
    --query "ResultSet.Rows[].Data[].VarCharValue" `
    --output table

$ScannedBytes = $Execution.QueryExecution.Statistics.DataScannedInBytes
Write-Host "Athena query succeeded. Query ID: $QueryExecutionId" -ForegroundColor Green
Write-Host "Actual bytes scanned: $ScannedBytes" -ForegroundColor Green
Write-Host "Results bucket: s3://$ResultsBucketName/results/" -ForegroundColor Green
Write-Host "Step 4 is deployed and verified." -ForegroundColor Green
