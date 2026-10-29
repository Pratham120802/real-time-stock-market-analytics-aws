# Step 2: Process and clean stock data with AWS Lambda

This step creates `stock-market-analytics-processor-development`, a Python 3.14 Lambda function connected to the Kinesis stream from Step 1.

For each JSON stock record, the function:

- validates and normalizes the stock symbol;
- validates price and volume values;
- normalizes timestamps to UTC;
- calculates the percentage change from the previous close when available;
- flags price changes of 5% or more;
- flags volume at least three times the supplied average volume;
- reports individual bad records without retrying an entire successful batch.

Processed records are written as structured CloudWatch logs for verification. Later steps will add S3 and DynamoDB destinations.

## Cost controls

- 128 MB Lambda memory
- ARM64 execution environment
- one Kinesis shard and a parallelization factor of 1, limiting stream-processing concurrency
- batches of up to 100 Kinesis records
- one-day CloudWatch log retention
- no provisioned concurrency
- no paid Lambda event-source provisioned polling

This small demo workload should remain inside Lambda's monthly free usage allowance.

## Local test

```powershell
python -m unittest discover -s tests -v
```

## Deploy

```powershell
cd C:\path\to\aws_4\cloudformation
.\deploy-step2.ps1
```

## View in AWS

Open AWS Lambda in `us-east-1` and select:

`stock-market-analytics-processor-development`
