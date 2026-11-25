"""
Home gateway.

Bridges the home MQTT broker (where the sensors live) and the cloud MQTT
broker (where the platform lives). All communication is MQTT.

Directions:
  home -> cloud   <room>/temperature        ->  <user>/<room>/temperature
                  Valid JSON readings are enriched with "user" and "room".
                  Malformed messages are forwarded untouched, so the cloud
                  can still store them (P1) and count them as errors (P2).

  home -> cloud   settings                  ->  <user>/settings
                  Comfort preferences the user publishes at home, e.g.
                  mosquitto_pub -t settings -m '{"mode": "eco"}'.
                  Forwarded as-is; the ingestor validates them.

  cloud -> home   <user>/<room>/heatpump    ->  <room>/heatpump
                  Heat pump commands from the commander service. Only valid
                  commands ({"status": 0|1}) are passed on: on bad JSON the
                  sensor (which we are not allowed to modify) raises inside
                  its MQTT thread and silently stops working.
"""

import json
import logging
import os
import time

import paho.mqtt.client as mqtt

# Configuration from environment variables (defaults are for local testing)
USER_ID = os.getenv("USER_ID", "user1")
LOCAL_MQTT_HOST = os.getenv("LOCAL_MQTT_HOST", "localhost")
LOCAL_MQTT_PORT = int(os.getenv("LOCAL_MQTT_PORT", "1883"))
CLOUD_MQTT_HOST = os.getenv("CLOUD_MQTT_HOST", "localhost")
CLOUD_MQTT_PORT = int(os.getenv("CLOUD_MQTT_PORT", "1883"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [gateway] %(levelname)s %(message)s")
log = logging.getLogger("gateway")


# --------------------------------------------------------------------------
# Pure translation functions (no network: easy to unit test)
# --------------------------------------------------------------------------

def to_cloud(user, local_topic, payload):
    """Translate a home message into (cloud_topic, cloud_payload).

    Handles "<room>/temperature" readings and the "settings" topic.
    Returns None for any other local topic.
    `payload` and the returned payload are bytes.
    """
    if local_topic == "settings":
        return f"{user}/settings", payload

    parts = local_topic.split("/")
    if len(parts) != 2 or not parts[0] or parts[1] != "temperature":
        return None
    room = parts[0]
    cloud_topic = f"{user}/{room}/temperature"

    try:
        data = json.loads(payload)
        if not isinstance(data, dict):
            raise ValueError("reading is not a JSON object")
    except ValueError:
        # JSONDecodeError and UnicodeDecodeError are both ValueError.
        # Forward the raw bytes: the identity is still in the topic.
        return cloud_topic, payload

    data["user"] = user
    data["room"] = room
    return cloud_topic, json.dumps(data).encode("utf-8")


def to_home(user, cloud_topic):
    """Translate "<user>/<room>/heatpump" into the local topic "<room>/heatpump".

    Returns None if the topic is not a heat pump command for this user.
    """
    parts = cloud_topic.split("/")
    if len(parts) != 3 or parts[0] != user or not parts[1] or parts[2] != "heatpump":
        return None
    return f"{parts[1]}/heatpump"


def is_valid_command(payload):
    """A command is valid only if it is exactly the format the sensor expects."""
    try:
        data = json.loads(payload)
    except ValueError:
        return False
    if not isinstance(data, dict):
        return False
    status = data.get("status")
    # type() check: in Python True == 1, but {"status": true} is not a valid command
    return type(status) is int and status in (0, 1)


# --------------------------------------------------------------------------
# MQTT plumbing
# --------------------------------------------------------------------------

def connect_with_retry(client, host, port, name):
    """Keep trying until the broker accepts the connection (it may start later)."""
    while True:
        try:
            client.connect(host, port, keepalive=60)
            log.info("Connected to %s broker at %s:%s", name, host, port)
            return
        except OSError as exc:
            log.warning("%s broker %s:%s not reachable (%s), retrying in 5 s", name, host, port, exc)
            time.sleep(5)


def main():
    # Unique client ids: two clients with the same id on one broker kick each other out
    cloud = mqtt.Client(client_id=f"gateway-{USER_ID}-cloud")
    local = mqtt.Client(client_id=f"gateway-{USER_ID}-local")

    # --- cloud side: receives heat pump commands for this user ---
    def on_cloud_connect(client, userdata, flags, rc):
        if rc != 0:
            log.error("Cloud broker refused the connection (rc=%s)", rc)
            return
        # Subscribing here (not once at start) restores the subscription after a reconnect
        client.subscribe(f"{USER_ID}/+/heatpump", qos=1)

    def on_cloud_message(client, userdata, msg):
        local_topic = to_home(USER_ID, msg.topic)
        if local_topic is None:
            return
        if not is_valid_command(msg.payload):
            log.warning("Ignoring invalid command on %s: %r", msg.topic, msg.payload)
            return
        local.publish(local_topic, msg.payload)
        log.info("cloud -> home  %s %s", local_topic, msg.payload.decode("utf-8"))

    # --- home side: receives the readings of every room ---
    def on_local_connect(client, userdata, flags, rc):
        if rc != 0:
            log.error("Home broker refused the connection (rc=%s)", rc)
            return
        # "+" matches any room, so new sensors need no gateway configuration
        client.subscribe([("+/temperature", 0), ("settings", 1)])

    def on_local_message(client, userdata, msg):
        result = to_cloud(USER_ID, msg.topic, msg.payload)
        if result is None:
            log.warning("Ignoring message on unexpected topic %s", msg.topic)
            return
        cloud_topic, cloud_payload = result
        cloud.publish(cloud_topic, cloud_payload, qos=1)
        log.info("home -> cloud  %s %r", cloud_topic, cloud_payload)

    cloud.on_connect = on_cloud_connect
    cloud.on_message = on_cloud_message
    local.on_connect = on_local_connect
    local.on_message = on_local_message

    connect_with_retry(cloud, CLOUD_MQTT_HOST, CLOUD_MQTT_PORT, "cloud")
    cloud.loop_start()  # background thread for the cloud client

    connect_with_retry(local, LOCAL_MQTT_HOST, LOCAL_MQTT_PORT, "local")
    local.loop_forever()  # blocks: keeps the gateway alive (paho reconnects automatically)


if __name__ == "__main__":
    main()
