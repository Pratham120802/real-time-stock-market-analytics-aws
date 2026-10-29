# Real-Time Stock Market Analytics Pipeline on AWS

This project implements a streaming stock-data pipeline using Amazon Kinesis, AWS Lambda, Amazon S3, AWS Glue, Amazon Athena, Amazon DynamoDB, and Amazon SNS.

## Current progress

- Step 1: Amazon Kinesis Data Stream — complete
- Step 2: AWS Lambda processing, cleaning, and anomaly detection — complete
- Step 3: S3 raw archive and DynamoDB cleaned-data storage — complete
- Step 4: Glue Data Catalog and Amazon Athena historical queries — complete
- Step 5: Local yfinance/Kinesis stock producer — complete and live-tested
- Step 6: DynamoDB trend Lambda and SNS publishing — complete and live-tested
- Final notification endpoint: email subscription — awaiting an address and recipient confirmation

See [STEP1-README.md](STEP1-README.md) for the first deployment step.
See [STEP2-README.md](STEP2-README.md) for the processing Lambda.
See [STEP3-README.md](STEP3-README.md) for the storage layer.
See [STEP4-README.md](STEP4-README.md) for historical SQL analytics.
See [STEP5-README.md](STEP5-README.md) for the local stock producer.
See [STEP6-README.md](STEP6-README.md) for SMA crossover alerts.
