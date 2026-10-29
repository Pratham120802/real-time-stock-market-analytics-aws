import base64
import gzip
import hashlib
import json
import logging
import math
import os
import re
import time
from decimal import Decimal
from datetime import datetime, timezone


logger = logging.getLogger()
logger.setLevel(logging.INFO)

PRICE_CHANGE_THRESHOLD_PCT = float(os.getenv("PRICE_CHANGE_THRESHOLD_PCT", "5"))
VOLUME_SPIKE_MULTIPLIER = float(os.getenv("VOLUME_SPIKE_MULTIPLIER", "3"))
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9.-]{1,12}$")
RAW_BUCKET_NAME = os.getenv("RAW_BUCKET_NAME")
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME")
DATA_TTL_DAYS = int(os.getenv("DATA_TTL_DAYS", "7"))


def _finite_number(value, field_name):
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be numeric") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field_name} must be finite")
    return number


def _timestamp(value):
    if value is None:
        return datetime.now(timezone.utc).isoformat()
    if isinstance(value, (int, float)):
        seconds = value / 1000 if value > 10_000_000_000 else value
        return datetime.fromtimestamp(seconds, timezone.utc).isoformat()
    text = str(value).strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def clean_and_analyze(payload):
    if not isinstance(payload, dict):
        raise ValueError("record must be a JSON object")

    symbol = str(payload.get("symbol", "")).strip().upper()
    if not SYMBOL_PATTERN.fullmatch(symbol):
        raise ValueError("symbol is missing or invalid")

    price = _finite_number(payload.get("price"), "price")
    if price <= 0:
        raise ValueError("price must be greater than zero")

    volume = int(_finite_number(payload.get("volume", 0), "volume"))
    if volume < 0:
        raise ValueError("volume cannot be negative")

    anomalies = []
    price_change_pct = None
    previous_close = payload.get("previous_close")
    if previous_close is not None:
        previous_close = _finite_number(previous_close, "previous_close")
        if previous_close <= 0:
            raise ValueError("previous_close must be greater than zero")
        price_change_pct = round(((price - previous_close) / previous_close) * 100, 4)
        if abs(price_change_pct) >= PRICE_CHANGE_THRESHOLD_PCT:
            anomalies.append("PRICE_CHANGE")

    average_volume = payload.get("average_volume")
    if average_volume is not None:
        average_volume = _finite_number(average_volume, "average_volume")
        if average_volume <= 0:
            raise ValueError("average_volume must be greater than zero")
        if volume >= average_volume * VOLUME_SPIKE_MULTIPLIER:
            anomalies.append("VOLUME_SPIKE")

    now = datetime.now(timezone.utc).isoformat()
    return {
        "symbol": symbol,
        "price": round(price, 4),
        "volume": volume,
        "timestamp": _timestamp(payload.get("timestamp")),
        "source": str(payload.get("source", "stock-api")),
        "previous_close": previous_close,
        "average_volume": average_volume,
        "price_change_pct": price_change_pct,
        "is_anomaly": bool(anomalies),
        "anomaly_types": anomalies,
        "processed_at": now,
    }


def archive_raw_records(raw_records):
    if not RAW_BUCKET_NAME or not raw_records:
        return None

    import boto3

    now = datetime.now(timezone.utc)
    sequence_numbers = "|".join(record["sequence_number"] for record in raw_records)
    batch_id = hashlib.sha256(sequence_numbers.encode("utf-8")).hexdigest()[:20]
    object_key = (
        f"raw/year={now:%Y}/month={now:%m}/day={now:%d}/hour={now:%H}/"
        f"batch-{batch_id}.json.gz"
    )
    ndjson = "\n".join(
        json.dumps(record, separators=(",", ":"), sort_keys=True)
        for record in raw_records
    ).encode("utf-8")

    boto3.client("s3").put_object(
        Bucket=RAW_BUCKET_NAME,
        Key=object_key,
        Body=gzip.compress(ndjson),
        ContentType="application/x-ndjson",
        ContentEncoding="gzip",
    )
    return object_key


def store_clean_records(cleaned_records):
    if not DYNAMODB_TABLE_NAME or not cleaned_records:
        return 0

    import boto3

    expires_at = int(time.time()) + (DATA_TTL_DAYS * 86400)
    table = boto3.resource("dynamodb").Table(DYNAMODB_TABLE_NAME)
    with table.batch_writer(overwrite_by_pkeys=["symbol", "timestamp"]) as batch:
        for record in cleaned_records:
            item = dict(record)
            item["expires_at"] = expires_at
            item = json.loads(json.dumps(item), parse_float=Decimal)
            batch.put_item(Item=item)
    return len(cleaned_records)


def handler(event, context):
    failures = []
    processed_count = 0
    raw_records = []
    cleaned_records = []
    received_sequence_numbers = []

    for record in event.get("Records", []):
        sequence_number = record.get("kinesis", {}).get("sequenceNumber", "unknown")
        received_sequence_numbers.append(sequence_number)
        try:
            encoded = record["kinesis"]["data"]
            payload = json.loads(base64.b64decode(encoded).decode("utf-8"))
            raw_records.append({"sequence_number": sequence_number, "payload": payload})
            cleaned = clean_and_analyze(payload)
            cleaned["kinesis_sequence_number"] = sequence_number
            cleaned_records.append(cleaned)
            logger.info(json.dumps({"event": "stock_record_processed", "data": cleaned}))
            processed_count += 1
        except Exception as exc:
            logger.error(json.dumps({
                "event": "stock_record_failed",
                "sequence_number": sequence_number,
                "error": str(exc),
            }))
            failures.append({"itemIdentifier": sequence_number})

    try:
        raw_object_key = archive_raw_records(raw_records)
        stored_count = store_clean_records(cleaned_records)
        logger.info(json.dumps({
            "event": "storage_complete",
            "raw_object_key": raw_object_key,
            "dynamodb_items": stored_count,
        }))
    except Exception as exc:
        logger.error(json.dumps({"event": "storage_failed", "error": str(exc)}))
        failed_ids = {failure["itemIdentifier"] for failure in failures}
        for sequence_number in received_sequence_numbers:
            if sequence_number not in failed_ids:
                failures.append({"itemIdentifier": sequence_number})

    logger.info(json.dumps({
        "event": "batch_complete",
        "received": len(event.get("Records", [])),
        "processed": processed_count,
        "failed": len(failures),
    }))
    return {"batchItemFailures": failures}
