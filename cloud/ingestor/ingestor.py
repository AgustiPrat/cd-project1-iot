"""
Ingestor: cloud MQTT broker -> Kafka.

Routes:
  <user>/<room>/temperature  ->  topic raw-readings,  key "<user>/<room>"
                                 value = payload exactly as received
                                 (malformed ones too: P1 stores them, P2 counts them)
  <user>/settings            ->  topic user-settings, key "<user>"
                                 only valid settings: user-settings is a compacted
                                 topic read as a KTable (P3), so a broken value
                                 would become that user's "current" settings.

Why the key "<user>/<room>":
  Kafka keeps order only inside a partition, and the key decides the partition.
  All readings of one room go to the same partition, in order, so the per-room
  state of the Streams app (heat pump on/off, error counts, dead sensor) is
  correct, while different rooms are spread over partitions to scale.
"""

import json
import logging
import math
import os
import signal
import socket
import time

import paho.mqtt.client as mqtt
from confluent_kafka import Producer

# Configuration from environment variables (defaults are for local testing)
CLOUD_MQTT_HOST = os.getenv("CLOUD_MQTT_HOST", "localhost")
CLOUD_MQTT_PORT = int(os.getenv("CLOUD_MQTT_PORT", "1883"))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "raw-readings")
SETTINGS_TOPIC = os.getenv("SETTINGS_TOPIC", "user-settings")
# EXTRA (beyond the course material): shared subscriptions and a persistent
# MQTT session, added for fault tolerance and horizontal scaling.
# Unique per instance: two MQTT clients with the same id kick each other out
INGESTOR_ID = os.getenv("INGESTOR_ID", f"ingestor-{socket.gethostname()}")
# Shared subscription group: with several ingestors, each message goes to only one of them
SHARE_GROUP = os.getenv("SHARE_GROUP", "ingestors")

VALID_MODES = {"comfort", "eco", "away"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ingestor] %(levelname)s %(message)s")
log = logging.getLogger("ingestor")


# --------------------------------------------------------------------------
# Pure routing functions (no network: easy to unit test)
# --------------------------------------------------------------------------

def is_valid_settings(payload):
    """Settings must be a JSON object with a known mode; min/max are optional numbers, min < max."""
    try:
        data = json.loads(payload)
    except ValueError:
        return False
    if not isinstance(data, dict) or data.get("mode") not in VALID_MODES:
        return False
    low, high = data.get("min"), data.get("max")
    for bound in (low, high):
        if bound is None:
            continue
        if isinstance(bound, bool) or not isinstance(bound, (int, float)) or not math.isfinite(bound):
            return False
    if low is not None and high is not None and low >= high:
        return False
    return True


def route(mqtt_topic, payload):
    """Decide where an MQTT message goes in Kafka.

    Returns (kafka_topic, key, value) or None if the message must be dropped.
    """
    parts = mqtt_topic.split("/")

    if len(parts) == 3 and parts[0] and parts[1] and parts[2] == "temperature":
        user, room = parts[0], parts[1]
        return RAW_TOPIC, f"{user}/{room}", payload

    if len(parts) == 2 and parts[0] and parts[1] == "settings":
        if not is_valid_settings(payload):
            return None
        return SETTINGS_TOPIC, parts[0], payload

    return None


# --------------------------------------------------------------------------
# MQTT + Kafka plumbing
# --------------------------------------------------------------------------

def on_delivery(err, msg):
    """Called by the Kafka producer once a message is stored (or failed)."""
    if err is not None:
        log.error("Kafka delivery failed for key %s: %s", msg.key(), err)


def connect_with_retry(client, host, port):
    """Keep trying until the broker accepts the connection (it may start later)."""
    while True:
        try:
            client.connect(host, port, keepalive=60)
            log.info("Connected to cloud MQTT broker at %s:%s", host, port)
            return
        except OSError as exc:
            log.warning("MQTT broker %s:%s not reachable (%s), retrying in 5 s", host, port, exc)
            time.sleep(5)


def main():
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "ingestor",
        "acks": "all",                # wait until the broker has really stored it
        "enable.idempotence": True,   # retries never create duplicates
        "linger.ms": 20,              # small batches: better throughput, tiny delay
    })

    def on_connect(client, userdata, flags, rc):
        if rc != 0:
            log.error("Cloud broker refused the connection (rc=%s)", rc)
            return
        # Subscribing here restores the subscriptions after a reconnect.
        # "$share/<group>/" = shared subscription (supported by Mosquitto 1.6):
        # several ingestors split the messages instead of each getting a copy.
        client.subscribe([
            (f"$share/{SHARE_GROUP}/+/+/temperature", 1),
            (f"$share/{SHARE_GROUP}/+/settings", 1),
        ])

    def produce(kafka_topic, key, value):
        """Hand a message to the Kafka producer, waiting while its local queue is full."""
        while True:
            try:
                producer.produce(kafka_topic, key=key.encode("utf-8"), value=value, on_delivery=on_delivery)
                return
            except BufferError:
                # Queue full (Kafka slow or down): serve callbacks to free space, then retry
                log.warning("Kafka producer queue full, waiting...")
                producer.poll(1)

    def on_message(client, userdata, msg):
        target = route(msg.topic, msg.payload)
        if target is None:
            log.warning("Dropping message on %s: %r", msg.topic, msg.payload)
            return
        kafka_topic, key, value = target
        produce(kafka_topic, key, value)
        producer.poll(0)  # serve delivery callbacks without blocking
        log.info("%s  key=%s  %r", kafka_topic, key, value)

    # clean_session=False: the broker keeps a queue of QoS 1 messages for this
    # client id while the ingestor is down, and delivers them when it comes back
    client = mqtt.Client(client_id=INGESTOR_ID, clean_session=False)
    client.on_connect = on_connect
    client.on_message = on_message

    # docker stop sends SIGTERM: stop the MQTT loop so we can flush Kafka cleanly
    signal.signal(signal.SIGTERM, lambda *_: client.disconnect())

    connect_with_retry(client, CLOUD_MQTT_HOST, CLOUD_MQTT_PORT)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        pass
    finally:
        log.info("Flushing pending messages to Kafka...")
        producer.flush(10)


if __name__ == "__main__":
    main()
