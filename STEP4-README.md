# Step 4: Query historical stock data with Amazon Athena

This step makes the compressed raw history in S3 queryable with SQL:

- an AWS Glue Data Catalog database and external table define the NDJSON schema;
- partition projection maps `year/month/day/hour` filters directly to the existing S3 folders;
- an Athena workgroup stores encrypted results in a dedicated S3 bucket;
- a sample named query and deployment-time live query demonstrate historical analysis.

## Cost controls

- no Glue crawler, ETL job, or scheduled process
- only two Glue catalog objects, within the first one million free catalog objects
- gzip-compressed source data and mandatory date/hour partition filters in the sample query
- Athena per-query scan cutoff set to the minimum allowed value: 10 MB
- CloudWatch workgroup metrics disabled
- query results expire from S3 after one day; versioning is not enabled

Athena has no idle charge. SQL queries are billed at $5 per TB scanned, rounded up with a 10 MB minimum per query. A query at that minimum is approximately $0.00005, plus tiny S3 request and result-storage charges.

## Deploy and verify

```powershell
cd C:\path\to\aws_4\cloudformation
.\deploy-step4.ps1
```

The script deploys the catalog, table, results bucket, and workgroup. It then queries only the current UTC hour and prints the returned rows, query ID, and actual bytes scanned.

## Resources

- Glue database: `stock_market_analytics_development`
- Glue table: `raw_stock_events`
- Athena workgroup: `stock-market-analytics-development`
- results bucket: `stock-market-analytics-athena-<account-id>-us-east-1`

In the AWS console, open **Athena**, choose the `stock-market-analytics-development` workgroup, select the `stock_market_analytics_development` database, and query `raw_stock_events`.
