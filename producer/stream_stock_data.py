import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone


DEFAULT_STREAM_NAME = "stock-market-analytics-stream-development"
DEFAULT_REGION = "us-east-1"


def utc_timestamp(offset_seconds=0):
    return (datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)).isoformat().replace("+00:00", "Z")


def mock_record(symbol="DEMO", price=205.5, offset_seconds=0, source="mock-producer"):
    return {
        "symbol": symbol.upper(),
        "open": round(price - 1.0, 4),
        "high": round(price + 1.0, 4),
        "low": round(price - 2.0, 4),
        "price": round(price, 4),
        "previous_close": 200.0,
        "volume": 1500000,
        "average_volume": 1000000,
        "timestamp": utc_timestamp(offset_seconds),
        "source": source,
    }


def live_record(symbol):
    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError(
            "yfinance is not installed. Run: pip install -r producer/requirements.txt"
        ) from exc

    history = yf.Ticker(symbol).history(period="5d", interval="1d", auto_adjust=False)
    if len(history.index) < 2:
        raise RuntimeError(f"Not enough yfinance history was returned for {symbol}")

    latest = history.iloc[-1]
    previous = history.iloc[-2]
    average_volume = float(history["Volume"].iloc[:-1].mean())
    return {
        "symbol": symbol.upper(),
        "open": round(float(latest["Open"]), 4),
        "high": round(float(latest["High"]), 4),
        "low": round(float(latest["Low"]), 4),
        "price": round(float(latest["Close"]), 4),
        "previous_close": round(float(previous["Close"]), 4),
        "volume": int(latest["Volume"]),
        "average_volume": round(average_volume, 4),
        "timestamp": utc_timestamp(),
        "source": "yfinance",
    }


def send_record(kinesis_client, stream_name, record):
    response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(record, separators=(",", ":")).encode("utf-8"),
        PartitionKey=record["symbol"],
    )
    return {
        "symbol": record["symbol"],
        "price": record["price"],
        "timestamp": record["timestamp"],
        "shard_id": response["ShardId"],
        "sequence_number": response["SequenceNumber"],
    }


def send_crossover_test(kinesis_client, stream_name, symbol, delay_seconds):
    prices = ([100.0] * 20) + [110.0]
    print(f"Sending {len(prices)} controlled records for a BUY crossover test...")
    for index, price in enumerate(prices):
        record = mock_record(
            symbol=symbol,
            price=price,
            offset_seconds=index,
            source="sma-crossover-test",
        )
        result = send_record(kinesis_client, stream_name, record)
        print(json.dumps(result))
        if index < len(prices) - 1:
            time.sleep(delay_seconds)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Send live or controlled stock events to Amazon Kinesis."
    )
    parser.add_argument(
        "--stream-name",
        default=os.getenv("KINESIS_STREAM_NAME", DEFAULT_STREAM_NAME),
    )
    parser.add_argument("--region", default=os.getenv("AWS_REGION", DEFAULT_REGION))
    parser.add_argument("--symbols", default="AAPL", help="Comma-separated ticker symbols")
    parser.add_argument("--interval", type=float, default=60.0, help="Seconds between sends")
    parser.add_argument(
        "--iterations",
        type=int,
        default=0,
        help="Number of cycles; 0 runs continuously until Ctrl+C",
    )
    parser.add_argument("--mock", action="store_true", help="Send controlled demo data without yfinance")
    parser.add_argument(
        "--test-crossover",
        action="store_true",
        help="Send 20 flat prices followed by a higher price to test the trend alert",
    )
    parser.add_argument("--test-symbol", default="TRND")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.interval < 1:
        raise ValueError("--interval must be at least one second to protect the low-cost table capacity")

    import boto3

    kinesis_client = boto3.client("kinesis", region_name=args.region)
    if args.test_crossover:
        send_crossover_test(kinesis_client, args.stream_name, args.test_symbol.upper(), args.interval)
        return

    symbols = [symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()]
    if not symbols:
        raise ValueError("At least one symbol is required")

    cycle = 0
    try:
        while args.iterations == 0 or cycle < args.iterations:
            for symbol in symbols:
                record = mock_record(symbol=symbol) if args.mock else live_record(symbol)
                print(json.dumps(send_record(kinesis_client, args.stream_name, record)))
            cycle += 1
            if args.iterations == 0 or cycle < args.iterations:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Producer stopped. No local process remains running.")


if __name__ == "__main__":
    main()
