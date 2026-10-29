# Step 5: Run the stock-data producer

The producer runs locally and sends stock events to the existing Kinesis stream. Keeping it local avoids EC2, container, and scheduler charges.

## Install

```powershell
cd C:\path\to\aws_4
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r producer\requirements.txt
```

The script uses the AWS credentials already configured on this computer. No access keys are stored in the project.

## Send one safe test record

```powershell
.\.venv\Scripts\python.exe producer\stream_stock_data.py --mock --iterations 1
```

## Stream live prices

```powershell
.\.venv\Scripts\python.exe producer\stream_stock_data.py --symbols AAPL,MSFT --interval 60
```

Press `Ctrl+C` when the demonstration is finished. The AWS pipeline remains deployed, but no new market records are generated after the local producer stops.

## Cost control

- The producer uses this computer instead of a paid always-on AWS compute service.
- The default interval is 60 seconds.
- The script enforces a minimum interval of one second to protect the 1-WCU DynamoDB table.
- Use one or two symbols for demonstrations and stop the process afterward.
