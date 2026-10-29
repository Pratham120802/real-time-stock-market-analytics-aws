# Step 3: Store cleaned data in DynamoDB and raw data in S3

This step adds both storage paths from the project architecture:

- Raw input events are batched as newline-delimited JSON, compressed with gzip, encrypted by S3, and stored under hourly date partitions.
- Cleaned events are stored in DynamoDB using `symbol` as the partition key and the UTC `timestamp` as the sort key.

## Cost controls

- DynamoDB Standard table with 1 provisioned RCU and 1 provisioned WCU
- DynamoDB item TTL after seven days
- no point-in-time recovery or paid backups
- DynamoDB Streams enabled for the later trend Lambda; Lambda reads from DynamoDB Streams are not charged
- gzip-compressed S3 batches instead of one object per event
- S3 lifecycle deletes raw demo data after seven days
- no S3 versioning

The DynamoDB configuration is within the AWS Free Tier allowance of 25 RCUs, 25 WCUs, and 25 GB for Standard tables. S3 usage for this demo remains tiny, though S3 eligibility depends on the account's Free Tier or credits.

## Deploy

```powershell
cd C:\path\to\aws_4\cloudformation
.\deploy-step3.ps1
```

## Resources

- S3 bucket: `stock-market-analytics-raw-<account-id>-us-east-1`
- DynamoDB table: `stock-market-analytics-cleaned-development`
- Retention: seven days for both raw and cleaned demo data
