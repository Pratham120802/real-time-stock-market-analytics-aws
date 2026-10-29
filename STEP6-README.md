# Step 6: Detect stock trends and publish SNS alerts

This step completes the lower branch of the architecture:

`DynamoDB Streams → trend Lambda → Amazon SNS → email`

The Lambda reads the latest 21 prices for each newly inserted symbol, calculates SMA-5 and SMA-20, detects BUY/SELL crossovers, and publishes an SNS notification. Each crossover is claimed on the newest DynamoDB item so retries do not send the same alert repeatedly.

## Cost controls

- Event-driven DynamoDB Streams trigger; no polling server or EventBridge schedule
- 30-second batching window and batch size 100
- 128 MB ARM Lambda with a 10-second timeout
- CloudWatch logs retained for one day
- Standard SNS topic with no KMS request charges
- no SMS subscription, because SMS pricing varies; email is used instead
- DynamoDB Streams reads performed by Lambda are not charged

SNS includes the first one million requests and first 1,000 email deliveries per month in its Free Tier. Lambda and DynamoDB usage for this small demonstration should also remain within their existing low-cost configuration.

## Deploy without an email subscription

```powershell
cd C:\path\to\aws_4\cloudformation
.\deploy-step6.ps1
```

## Add an email subscription

```powershell
.\deploy-step6.ps1 -AlertEmail "<email-address>"
```

AWS sends a confirmation message to that address. Alerts cannot be delivered until the recipient clicks **Confirm subscription**.

The deployed topic is `stock-market-analytics-alerts-development`. The AWS publish path has been verified with a controlled BUY crossover; an email endpoint still must be added and confirmed before a person can receive those messages.

## Controlled crossover test

From the project root:

```powershell
.\.venv\Scripts\python.exe producer\stream_stock_data.py `
    --test-crossover `
    --test-symbol TRND `
    --interval 1.1
```

The 20 prices at `100` followed by one price at `110` create a controlled BUY crossover. The final DynamoDB item receives `alert_signal`, `sma_5`, `sma_20`, and `alerted_at` attributes, and the trend Lambda publishes the corresponding message to SNS.
