$ErrorActionPreference = "Stop"

$StackName = "stock-market-kinesis"
$Region = "us-east-1"
$TemplateFile = Join-Path $PSScriptRoot "01-kinesis-stream.yaml"

Write-Host "Deploying Step 1: Amazon Kinesis Data Stream" -ForegroundColor Cyan
Write-Host "Stack:  $StackName"
Write-Host "Region: $Region"

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
        RetentionPeriodHours=24 `
        ShardCount=1 `
    --region $Region `
    --no-fail-on-empty-changeset

if ($LASTEXITCODE -ne 0) {
    throw "CloudFormation deployment failed."
}

$StreamName = aws cloudformation describe-stacks `
    --stack-name $StackName `
    --region $Region `
    --query "Stacks[0].Outputs[?OutputKey=='StreamName'].OutputValue" `
    --output text

aws kinesis wait stream-exists --stream-name $StreamName --region $Region
if ($LASTEXITCODE -ne 0) {
    throw "The Kinesis stream did not become active."
}

Write-Host "Kinesis stream is active: $StreamName" -ForegroundColor Green
aws kinesis describe-stream-summary `
    --stream-name $StreamName `
    --region $Region `
    --query "StreamDescriptionSummary.{Name:StreamName,Status:StreamStatus,Mode:StreamModeDetails.StreamMode,OpenShards:OpenShardCount,RetentionHours:RetentionPeriodHours,Encryption:EncryptionType,ARN:StreamARN}" `
    --output table
