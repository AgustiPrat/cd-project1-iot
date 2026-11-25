"""Unit tests for the ingestor routing (no MQTT broker and no Kafka needed).

Run from this folder:  python -m unittest -v
"""

import sys
import types
import unittest

# The routing logic does not need Kafka: stub the library so the tests
# also run on a machine without confluent-kafka installed.
if "confluent_kafka" not in sys.modules:
    stub = types.ModuleType("confluent_kafka")
    stub.Producer = object
    sys.modules["confluent_kafka"] = stub

from ingestor import RAW_TOPIC, SETTINGS_TOPIC, is_valid_settings, route  # noqa: E402


class RouteReadingsTest(unittest.TestCase):
    def test_reading_goes_to_raw_with_user_room_key(self):
        payload = b'{"timestamp": "t", "value": 21.5, "user": "user1", "room": "room1"}'
        self.assertEqual(route("user1/room1/temperature", payload),
                         (RAW_TOPIC, "user1/room1", payload))

    def test_malformed_reading_is_still_ingested_as_is(self):
        self.assertEqual(route("user2/office/temperature", b"garbage{"),
                         (RAW_TOPIC, "user2/office", b"garbage{"))

    def test_unknown_topics_are_dropped(self):
        self.assertIsNone(route("user1/room1/humidity", b"{}"))
        self.assertIsNone(route("room1/temperature", b"{}"))
        self.assertIsNone(route("user1//temperature", b"{}"))
        self.assertIsNone(route("user1/room1/heatpump", b'{"status": 1}'))


class RouteSettingsTest(unittest.TestCase):
    def test_valid_settings_go_to_settings_topic_keyed_by_user(self):
        payload = b'{"mode": "comfort", "min": 21, "max": 24}'
        self.assertEqual(route("user1/settings", payload), (SETTINGS_TOPIC, "user1", payload))

    def test_invalid_settings_are_dropped(self):
        self.assertIsNone(route("user1/settings", b'{"mode": "party"}'))
        self.assertIsNone(route("user1/settings", b"not json"))


class SettingsValidationTest(unittest.TestCase):
    def test_valid(self):
        self.assertTrue(is_valid_settings(b'{"mode": "eco"}'))
        self.assertTrue(is_valid_settings(b'{"mode": "away"}'))
        self.assertTrue(is_valid_settings(b'{"mode": "comfort", "min": 20.5, "max": 23}'))

    def test_invalid(self):
        self.assertFalse(is_valid_settings(b'{"mode": "comfort", "min": 25, "max": 20}'))
        self.assertFalse(is_valid_settings(b'{"mode": "comfort", "min": "21"}'))
        self.assertFalse(is_valid_settings(b'{"mode": "comfort", "min": true}'))
        self.assertFalse(is_valid_settings(b'["comfort"]'))
        self.assertFalse(is_valid_settings(b'{"mode": "comfort", "min": NaN, "max": 24}'))


if __name__ == "__main__":
    unittest.main()
