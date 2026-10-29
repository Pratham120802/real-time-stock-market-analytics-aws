import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal


logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")
SHORT_WINDOW = int(os.getenv("SHORT_WINDOW", "5"))
LONG_WINDOW = int(os.getenv("LONG_WINDOW", "20"))


def moving_average(prices, window):
    if len(prices) < window:
        return None
    return sum(prices[-window:]) / Decimal(window)


def detect_crossover(prices):
    if len(prices) < LONG_WINDOW + 1:
        return None

    previous_prices = prices[:-1]
    previous_short = moving_average(previous_prices, SHORT_WINDOW)
    previous_long = moving_average(previous_prices, LONG_WINDOW)
    current_short = moving_average(prices, SHORT_WINDOW)
    current_long = moving_average(prices, LONG_WINDOW)

    signal = None
    if previous_short <= previous_long and current_short > current_long:
        signal = "BUY"
    elif previous_short >= previous_long and current_short < current_long:
        signal = "SELL"

    if not signal:
        return None
    return {
        "signal": signal,
        "sma_5": current_short,
        "sma_20": current_long,
        "previous_sma_5": previous_short,
        "previous_sma_20": previous_long,
    }


def stream_symbols(event):
    symbols = {}
    for record in event.get("Records", []):
        if record.get("eventName") != "INSERT":
            continue
        image = record.get("dynamodb", {}).get("NewImage", {})
        symbol = image.get("symbol", {}).get("S")
        if symbol:
            symbols.setdefault(symbol, []).append(record.get("eventID", "unknown"))
    return symbols


def recent_items(table, symbol):
    from boto3.dynamodb.conditions import Key

    response = table.query(
        KeyConditionExpression=Key("symbol").eq(symbol),
        ProjectionExpression="#ts, price",
        ExpressionAttributeNames={"#ts": "timestamp"},
        ScanIndexForward=False,
        Limit=LONG_WINDOW + 1,
        ConsistentRead=False,
    )
    return list(reversed(response.get("Items", [])))


def claim_alert(table, symbol, item, analysis):
    from botocore.exceptions import ClientError

    try:
        table.update_item(
            Key={"symbol": symbol, "timestamp": item["timestamp"]},
            UpdateExpression=(
                "SET alert_signal = :signal, sma_5 = :short, sma_20 = :long, "
                "alerted_at = :alerted"
            ),
            ConditionExpression="attribute_not_exists(alert_signal)",
            ExpressionAttributeValues={
                ":signal": analysis["signal"],
                ":short": analysis["sma_5"],
                ":long": analysis["sma_20"],
                ":alerted": datetime.now(timezone.utc).isoformat(),
            },
        )
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise


def publish_alert(sns_client, symbol, item, analysis):
    direction = "upward" if analysis["signal"] == "BUY" else "downward"
    message = {
        "symbol": symbol,
        "signal": analysis["signal"],
        "price": str(item["price"]),
        "timestamp": item["timestamp"],
        "sma_5": str(analysis["sma_5"].quantize(Decimal("0.0001"))),
        "sma_20": str(analysis["sma_20"].quantize(Decimal("0.0001"))),
        "summary": f"{symbol} produced an {direction} SMA-5/SMA-20 crossover.",
    }
    response = sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"Stock trend alert: {analysis['signal']} {symbol}",
        Message=json.dumps(message, indent=2),
    )
    return response["MessageId"], message


def handler(event, context):
    if not TABLE_NAME or not SNS_TOPIC_ARN:
        raise RuntimeError("DYNAMODB_TABLE_NAME and SNS_TOPIC_ARN must be configured")

    import boto3

    table = boto3.resource("dynamodb").Table(TABLE_NAME)
    sns_client = boto3.client("sns")
    failures = []

    for symbol, event_ids in stream_symbols(event).items():
        try:
            items = recent_items(table, symbol)
            prices = [Decimal(str(item["price"])) for item in items]
            analysis = detect_crossover(prices)
            if not analysis:
                logger.info(json.dumps({
                    "event": "trend_checked",
                    "symbol": symbol,
                    "records_available": len(items),
                    "signal": None,
                }))
                continue

            latest_item = items[-1]
            if not claim_alert(table, symbol, latest_item, analysis):
                logger.info(json.dumps({
                    "event": "duplicate_alert_skipped",
                    "symbol": symbol,
                    "timestamp": latest_item["timestamp"],
                }))
                continue

            message_id, message = publish_alert(sns_client, symbol, latest_item, analysis)
            logger.info(json.dumps({
                "event": "trend_alert_published",
                "message_id": message_id,
                **message,
            }))
        except Exception as exc:
            logger.error(json.dumps({
                "event": "trend_analysis_failed",
                "symbol": symbol,
                "error": str(exc),
            }))
            failures.extend({"itemIdentifier": event_id} for event_id in event_ids)

    return {"batchItemFailures": failures}
