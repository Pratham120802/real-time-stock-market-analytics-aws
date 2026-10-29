param(
    [string]$AlertEmail = ""
)

$ErrorActionPreference = "Stop"

$StackName = "stock-market-alerts"
$Region = "us-east-1"
$TemplateFile = Join-Path $PSScriptRoot "05-trend-alerts.yaml"
$ProjectRoot = Split-Path $PSScriptRoot -Parent
$SourceFile = Join-Path $ProjectRoot "lambda\trend\index.py"
$BuildDirectory = Join-Path $ProjectRoot "build\trend"
$ZipFile = Join-Path $ProjectRoot "build\trend.zip"

Write-Host "Deploying the trend-analysis Lambda and SNS topic" -ForegroundColor Cyan

aws sts get-caller-identity --region $Region | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "AWS credentials are unavailable or invalid."
}

New-Item -ItemType Directory -Path $BuildDirectory -Force | Out-Null
Copy-Item -LiteralPath $SourceFile -Destination (Join-Path $BuildDirectory "index.py") -Force
if (Test-Path -LiteralPath $ZipFile) {
    Remove-Item -LiteralPath $ZipFile -Force
}
Compress-Archive -LiteralPath (Join-Path $BuildDirectory "index.py") -DestinationPath $ZipFile

$ParameterOverrides = @(
    "ProjectName=stock-market-analytics",
    "Environment=development"
)
if ($AlertEmail) {
    $ParameterOverrides += "AlertEmail=$AlertEmail"
}

aws cloudformation deploy `
    --stack-name $StackName `
    --template-file $TemplateFile `
    --parameter-overrides $ParameterOverrides `
    --capabilities CAPABILITY_NAMED_IAM `
    --region $Region `
    --no-fail-on-empty-changeset

if ($LASTEXITCODE -ne 0) {
    throw "Trend alert stack deployment failed."
}

$FunctionName = aws cloudformation describe-stacks `
    --stack-name $StackName `
    --region $Region `
    --query "Stacks[0].Outputs[?OutputKey=='TrendFunctionName'].OutputValue" `
    --output text

$ZipUri = "fileb://" + ($ZipFile -replace '\\', '/')
aws lambda update-function-code `
    --function-name $FunctionName `
    --zip-file $ZipUri `
    --region $Region `
    --output text | Out-Null

if ($LASTEXITCODE -ne 0) {
    throw "Trend Lambda code upload failed."
}

aws lambda wait function-updated-v2 --function-name $FunctionName --region $Region
if ($LASTEXITCODE -ne 0) {
    throw "Trend Lambda did not finish updating."
}

aws cloudformation describe-stacks `
    --stack-name $StackName `
    --region $Region `
    --query "Stacks[0].Outputs[?OutputKey=='TrendFunctionName' || OutputKey=='AlertsTopicArn' || OutputKey=='EmailSubscriptionRequested'].[OutputKey,OutputValue]" `
    --output table

Write-Host "Trend analysis and SNS resources are deployed." -ForegroundColor Green
if ($AlertEmail) {
    Write-Host "Check $AlertEmail and confirm the AWS SNS subscription." -ForegroundColor Yellow
}
