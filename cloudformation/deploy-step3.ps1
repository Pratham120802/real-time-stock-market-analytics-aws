$ErrorActionPreference = "Stop"

$StackName = "stock-market-storage"
$Region = "us-east-1"
$TemplateFile = Join-Path $PSScriptRoot "03-storage.yaml"
$Step2Script = Join-Path $PSScriptRoot "deploy-step2.ps1"

Write-Host "Deploying Step 3: S3 and DynamoDB storage" -ForegroundColor Cyan

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
        DataRetentionDays=7 `
    --region $Region `
    --no-fail-on-empty-changeset

if ($LASTEXITCODE -ne 0) {
    throw "Storage stack deployment failed."
}

Write-Host "Updating the Step 2 processor with storage permissions..." -ForegroundColor Yellow
& $Step2Script

if ($LASTEXITCODE -ne 0) {
    throw "Processor update failed."
}

aws cloudformation describe-stacks `
    --stack-name $StackName `
    --region $Region `
    --query "Stacks[0].Outputs[?OutputKey=='RawBucketName' || OutputKey=='CleanedTableName'].[OutputKey,OutputValue]" `
    --output table

Write-Host "Step 3 storage is deployed and connected." -ForegroundColor Green
