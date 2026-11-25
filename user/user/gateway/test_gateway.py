"""Unit tests for the gateway translation functions (no broker needed).

Run from this folder:  python -m unittest -v
"""

import json
import unittest

from gateway import is_valid_command, to_cloud, to_home


class ToCloudTest(unittest.TestCase):
    def test_valid_reading_is_enriched(self):
        payload = b'{"timestamp": "2026-11-25T10:00:00.000", "value": 21.5}'
        topic, out = to_cloud("user1", "room1/temperature", payload)
        self.assertEqual(topic, "user1/room1/temperature")
        data = json.loads(out)
        self.assertEqual(data["user"], "user1")
        self.assertEqual(data["room"], "room1")
        # Original sensor fields are kept untouched
        self.assertEqual(data["timestamp"], "2026-11-25T10:00:00.000")
        self.assertEqual(data["value"], 21.5)

    def test_sensor_error_value_is_forwarded(self):
        # 100 is a sensor error, but filtering is P2's job, not the gateway's
        topic, out = to_cloud("user1", "office/temperature", b'{"timestamp": "t", "value": 100}')
        self.assertEqual(json.loads(out)["value"], 100)

    def test_malformed_json_is_forwarded_raw(self):
        topic, out = to_cloud("user1", "room1/temperature", b"not json {")
        self.assertEqual(topic, "user1/room1/temperature")
        self.assertEqual(out, b"not json {")

    def test_json_that_is_not_an_object_is_forwarded_raw(self):
        topic, out = to_cloud("user1", "room1/temperature", b"42")
        self.assertEqual(out, b"42")

    def test_invalid_utf8_is_forwarded_raw(self):
        topic, out = to_cloud("user1", "room1/temperature", b"\xff\xfe")
        self.assertEqual(out, b"\xff\xfe")

    def test_settings_are_forwarded_to_user_topic(self):
        payload = b'{"mode": "comfort", "min": 21, "max": 24}'
        self.assertEqual(to_cloud("user1", "settings", payload), ("user1/settings", payload))

    def test_unexpected_topic_is_ignored(self):
        self.assertIsNone(to_cloud("user1", "room1/humidity", b"{}"))
        self.assertIsNone(to_cloud("user1", "a/b/temperature", b"{}"))
        self.assertIsNone(to_cloud("user1", "/temperature", b"{}"))


class ToHomeTest(unittest.TestCase):
    def test_command_for_this_user(self):
        self.assertEqual(to_home("user1", "user1/room2/heatpump"), "room2/heatpump")

    def test_command_for_another_user_is_ignored(self):
        self.assertIsNone(to_home("user1", "user2/room2/heatpump"))

    def test_other_topics_are_ignored(self):
        self.assertIsNone(to_home("user1", "user1/room2/temperature"))
        self.assertIsNone(to_home("user1", "user1//heatpump"))


class CommandValidationTest(unittest.TestCase):
    def test_valid_commands(self):
        self.assertTrue(is_valid_command(b'{"status": 1}'))
        self.assertTrue(is_valid_command(b'{"status": 0}'))

    def test_invalid_commands(self):
        self.assertFalse(is_valid_command(b'{"status": 2}'))
        self.assertFalse(is_valid_command(b'{"state": 1}'))
        self.assertFalse(is_valid_command(b"on"))
        self.assertFalse(is_valid_command(b"[1]"))
        self.assertFalse(is_valid_command(b'{"status": true}'))
        self.assertFalse(is_valid_command(b'{"status": 1.0}'))


if __name__ == "__main__":
    unittest.main()
