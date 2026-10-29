$ErrorActionPreference = "Stop"

$StackName = "stock-market-processor"
$Region = "us-east-1"
$TemplateFile = Join-Path $PSScriptRoot "02-processing-lambda.yaml"
$ProjectRoot = Split-Path $PSScriptRoot -Parent
$SourceFile = Join-Path $ProjectRoot "lambda\processor\index.py"
$BuildDirectory = Join-Path $ProjectRoot "build\processor"
$ZipFile = Join-Path $ProjectRoot "build\processor.zip"

Write-Host "Deploying Step 2: Stock Processing Lambda" -ForegroundColor Cyan

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

aws cloudformation deploy `
    --stack-name $StackName `
    --template-file $TemplateFile `
    --parameter-overrides ProjectName=stock-market-analytics Environment=development `
    --capabilities CAPABILITY_NAMED_IAM `
    --region $Region `
    --no-fail-on-empty-changeset

if ($LASTEXITCODE -ne 0) {
    throw "CloudFormation deployment failed."
}

$FunctionName = aws cloudformation describe-stacks `
    --stack-name $StackName `
    --region $Region `
    --query "Stacks[0].Outputs[?OutputKey=='FunctionName'].OutputValue" `
    --output text

$ZipUri = "fileb://" + ($ZipFile -replace '\\', '/')
aws lambda update-function-code `
    --function-name $FunctionName `
    --zip-file $ZipUri `
    --region $Region `
    --output text | Out-Null

if ($LASTEXITCODE -ne 0) {
    throw "Lambda code upload failed."
}

aws lambda wait function-updated-v2 --function-name $FunctionName --region $Region
if ($LASTEXITCODE -ne 0) {
    throw "Lambda function did not finish updating."
}

Write-Host "Lambda processor deployed: $FunctionName" -ForegroundColor Green
aws lambda get-function-configuration `
    --function-name $FunctionName `
    --region $Region `
    --query "{Name:FunctionName,State:State,Runtime:Runtime,Architecture:Architectures[0],MemoryMB:MemorySize,TimeoutSeconds:Timeout}" `
    --output table
