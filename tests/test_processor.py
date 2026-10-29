import base64
import json
import os
import sys
import unittest


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "lambda", "processor"))

import index


def kinesis_event(payload, sequence_number="1"):
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    return {
        "Records": [{
            "kinesis": {
                "sequenceNumber": sequence_number,
                "data": encoded,
            }
        }]
    }


class ProcessorTests(unittest.TestCase):
    def test_cleans_record_and_detects_price_and_volume_anomalies(self):
        result = index.clean_and_analyze({
            "symbol": "aapl",
            "price": 210,
            "previous_close": 190,
            "volume": 4000,
            "average_volume": 1000,
            "timestamp": "2026-10-28T15:30:00Z",
        })

        self.assertEqual(result["symbol"], "AAPL")
        self.assertTrue(result["is_anomaly"])
        self.assertEqual(result["anomaly_types"], ["PRICE_CHANGE", "VOLUME_SPIKE"])

    def test_reports_only_the_invalid_record_as_failed(self):
        event = kinesis_event({"symbol": "AAPL", "price": -1}, "bad-1")
        response = index.handler(event, None)
        self.assertEqual(response, {"batchItemFailures": [{"itemIdentifier": "bad-1"}]})

    def test_accepts_a_normal_record(self):
        event = kinesis_event({
            "symbol": "MSFT",
            "price": 410,
            "previous_close": 409,
            "volume": 100,
            "average_volume": 1000,
        })
        response = index.handler(event, None)
        self.assertEqual(response, {"batchItemFailures": []})


if __name__ == "__main__":
    unittest.main()
