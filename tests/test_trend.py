import importlib.util
import os
import unittest
from decimal import Decimal


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TREND_SOURCE = os.path.join(PROJECT_ROOT, "lambda", "trend", "index.py")
SPEC = importlib.util.spec_from_file_location("trend_index", TREND_SOURCE)
trend_index = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(trend_index)


class TrendTests(unittest.TestCase):
    def test_detects_buy_crossover(self):
        prices = [Decimal("100")] * 20 + [Decimal("110")]
        result = trend_index.detect_crossover(prices)
        self.assertEqual(result["signal"], "BUY")
        self.assertGreater(result["sma_5"], result["sma_20"])

    def test_detects_sell_crossover(self):
        prices = [Decimal("100")] * 20 + [Decimal("90")]
        result = trend_index.detect_crossover(prices)
        self.assertEqual(result["signal"], "SELL")
        self.assertLess(result["sma_5"], result["sma_20"])

    def test_requires_twenty_one_prices(self):
        result = trend_index.detect_crossover([Decimal("100")] * 20)
        self.assertIsNone(result)

    def test_extracts_only_insert_symbols(self):
        event = {
            "Records": [
                {
                    "eventID": "one",
                    "eventName": "INSERT",
                    "dynamodb": {"NewImage": {"symbol": {"S": "AAPL"}}},
                },
                {
                    "eventID": "two",
                    "eventName": "MODIFY",
                    "dynamodb": {"NewImage": {"symbol": {"S": "AAPL"}}},
                },
            ]
        }
        self.assertEqual(trend_index.stream_symbols(event), {"AAPL": ["one"]})


if __name__ == "__main__":
    unittest.main()
