# Step 1: Amazon Kinesis Data Streams

This step creates the ingestion layer for the real-time stock market analytics pipeline.

The Python stock producer added in a later step will publish one JSON stock event at a time to this stream. AWS Lambda will then consume those events for processing.

## Resource created

- Kinesis stream: `stock-market-analytics-stream-development`
- Capacity mode: provisioned with one shard
- Record retention: 24 hours
- Server-side encryption: AWS managed Kinesis KMS key
- AWS Region: `us-east-1`

## Deploy

From PowerShell:

```powershell
cd C:\path\to\aws_4\cloudformation
.\deploy-step1.ps1
```

## Verify

```powershell
aws kinesis describe-stream-summary `
  --stream-name stock-market-analytics-stream-development `
  --region us-east-1
```

The expected stream status is `ACTIVE`.

## Cost control

Kinesis Data Streams is not included in the AWS Free Tier. One provisioned shard is the least expensive practical configuration and costs approximately $0.36 for each full day it remains active in `us-east-1`, before the very small PUT usage charge.

To keep this project within a $5-$6 budget, delete the stack within 14 days. Delete it sooner whenever the project is not actively being demonstrated:

```powershell
aws cloudformation delete-stack --stack-name stock-market-kinesis --region us-east-1
```

Kinesis streams cannot be paused. Deleting the stack is the only way to stop its shard-hour charges.
