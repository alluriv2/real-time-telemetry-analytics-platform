import json
import unittest
import urllib.error
from unittest.mock import MagicMock, patch

from fastapi import HTTPException

from weather_platform import api
from weather_platform.consumer import StallWatchdog
from weather_platform.dashboard.app import server


def fake_connection(row):
    cursor = MagicMock()
    cursor.fetchone.return_value = row
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.__enter__.return_value = conn
    return conn


class StallWatchdogTests(unittest.TestCase):
    def test_not_stalled_while_assigned(self):
        watchdog = StallWatchdog(300, now=0)
        self.assertFalse(watchdog.is_stalled(True, now=10_000))

    def test_not_stalled_within_grace_period(self):
        watchdog = StallWatchdog(300, now=0)
        self.assertFalse(watchdog.is_stalled(False, now=299))

    def test_stalled_after_timeout_without_assignment(self):
        watchdog = StallWatchdog(300, now=0)
        self.assertTrue(watchdog.is_stalled(False, now=301))

    def test_assignment_resets_the_clock(self):
        watchdog = StallWatchdog(300, now=0)
        self.assertFalse(watchdog.is_stalled(True, now=250))
        self.assertFalse(watchdog.is_stalled(False, now=500))
        self.assertTrue(watchdog.is_stalled(False, now=551))


class ApiDataHealthTests(unittest.TestCase):
    def test_fresh_data_is_healthy(self):
        with patch.object(
            api, "get_connection",
            return_value=fake_connection({"age_seconds": 42.4}),
        ):
            result = api.health_data()
        self.assertEqual(result["status"], "fresh")
        self.assertEqual(result["newest_observation_age_seconds"], 42)

    def test_stale_data_returns_503(self):
        with patch.object(
            api, "get_connection",
            return_value=fake_connection(
                {"age_seconds": api.DATA_STALE_SECONDS + 1}
            ),
        ):
            with self.assertRaises(HTTPException) as ctx:
                api.health_data()
        self.assertEqual(ctx.exception.status_code, 503)

    def test_empty_table_returns_503(self):
        with patch.object(
            api, "get_connection",
            return_value=fake_connection({"age_seconds": None}),
        ):
            with self.assertRaises(HTTPException) as ctx:
                api.health_data()
        self.assertEqual(ctx.exception.status_code, 503)

    def test_database_error_returns_503(self):
        with patch.object(
            api, "get_connection", side_effect=api.psycopg.Error("down"),
        ):
            with self.assertRaises(HTTPException) as ctx:
                api.health_data()
        self.assertEqual(ctx.exception.status_code, 503)


class DashboardDataHealthTests(unittest.TestCase):
    def setUp(self):
        self.client = server.test_client()

    def test_relays_fresh_status(self):
        response = MagicMock()
        response.read.return_value = b'{"status": "fresh"}'
        response.status = 200
        response.__enter__.return_value = response
        with patch("urllib.request.urlopen", return_value=response):
            result = self.client.get("/health/data")
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.get_json(), {"status": "fresh"})

    def test_relays_stale_status(self):
        error = urllib.error.HTTPError(
            "url", 503, "stale", {}, MagicMock(
                read=lambda: b'{"detail": "stale"}'
            ),
        )
        with patch("urllib.request.urlopen", side_effect=error):
            result = self.client.get("/health/data")
        self.assertEqual(result.status_code, 503)

    def test_api_unreachable_returns_503(self):
        with patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.URLError("refused"),
        ):
            result = self.client.get("/health/data")
        self.assertEqual(result.status_code, 503)
        self.assertIn("unreachable", json.loads(result.data)["detail"])


if __name__ == "__main__":
    unittest.main()
