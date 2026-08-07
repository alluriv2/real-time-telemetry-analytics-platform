import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from weather_platform.consumer import (
    kafka_offsets_after,
    write_weather_batch,
)


class FakeMessage:
    def __init__(self, topic, partition, offset):
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


class ConsumerBatchTests(unittest.TestCase):
    def test_kafka_offsets_after_uses_next_highest_offset_per_partition(self):
        offsets = kafka_offsets_after([
            FakeMessage("weather", 0, 7),
            FakeMessage("weather", 1, 3),
            FakeMessage("weather", 0, 9),
        ])

        actual = {
            (item.topic, item.partition): item.offset
            for item in offsets
        }

        self.assertEqual(actual, {
            ("weather", 0): 10,
            ("weather", 1): 4,
        })

    @patch("weather_platform.consumer.record_consumer_watermark")
    def test_write_weather_batch_commits_all_rows_once(self, watermark):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        records = [
            {
                "station": "station-a",
                "dt": datetime(2026, 8, 7, 12, 0),
            },
            {
                "station": "station-b",
                "dt": datetime(2026, 8, 7, 12, 1),
            },
        ]
        batch = [
            {
                "record": records[0],
                "partition": 0,
                "offset": 20,
            },
            {
                "record": records[1],
                "partition": 0,
                "offset": 21,
            },
        ]

        write_weather_batch(conn, batch)

        self.assertEqual(cur.executemany.call_count, 2)
        self.assertEqual(cur.executemany.call_args_list[0].args[1], records)
        self.assertEqual(cur.executemany.call_args_list[1].args[1], records)
        self.assertEqual(watermark.call_count, 2)
        conn.commit.assert_called_once_with()
        conn.rollback.assert_not_called()

    @patch("weather_platform.consumer.record_consumer_watermark")
    def test_write_weather_batch_rolls_back_on_error(self, watermark):
        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.executemany.side_effect = RuntimeError("database unavailable")

        with self.assertRaises(RuntimeError):
            write_weather_batch(
                conn,
                [{
                    "record": {
                        "station": "station-a",
                        "dt": datetime(2026, 8, 7, 12, 0),
                    },
                    "partition": 0,
                    "offset": 20,
                }],
            )

        conn.rollback.assert_called_once_with()
        conn.commit.assert_not_called()
        watermark.assert_not_called()


if __name__ == "__main__":
    unittest.main()
