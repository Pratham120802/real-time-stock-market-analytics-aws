import json
import os
import sys
import unittest


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "producer"))

import stream_stock_data


class FakeKinesisClient:
    def __init__(self):
        self.request = None

    def put_record(self, **kwargs):
        self.request = kwargs
        return {"ShardId": "shardId-000", "SequenceNumber": "123"}


class ProducerTests(unittest.TestCase):
    def test_mock_record_matches_processor_contract(self):
        record = stream_stock_data.mock_record("demo", 101.25)
        self.assertEqual(record["symbol"], "DEMO")
        self.assertEqual(record["price"], 101.25)
        self.assertIn("previous_close", record)
        self.assertTrue(record["timestamp"].endswith("Z"))

    def test_send_record_uses_symbol_as_partition_key(self):
        client = FakeKinesisClient()
        record = stream_stock_data.mock_record("AAPL", 210.0)
        result = stream_stock_data.send_record(client, "test-stream", record)

        self.assertEqual(client.request["StreamName"], "test-stream")
        self.assertEqual(client.request["PartitionKey"], "AAPL")
        self.assertEqual(json.loads(client.request["Data"])["price"], 210.0)
        self.assertEqual(result["sequence_number"], "123")


if __name__ == "__main__":
    unittest.main()
