import paho.mqtt.client as mqtt
import time
import threading
import json
from datetime import datetime, timedelta

# Configuration
BROKER = "localhost"  # Change to your MQTT broker address
PORT = 1883
KEEPALIVE = 60
TOPIC_STATUS = "tele/tasmota_XXXXXX/LWT"
TOPIC_COMMAND = "cmnd/tasmota_XXXXXX/POWER"
TOPIC_STATE = "tele/tasmota_XXXXXX/STATE"
TOPIC_RESULT = "stat/tasmota_XXXXXX/RESULT"
CLIENT_ID = "tasmota_emulator"
POWER_STATE = "OFF"
TELE_PERIOD = 60  # Default telemetry period in seconds
start_time = time.time()  # Track when the program started

"""
Tasmota MQTT Emulator — Message Protocol
=========================================

1. Startup
   On connect, publishes:
     tele/tasmota_XXXXXX/LWT  →  "Online"  (retained)

2. Periodic Telemetry (TelePeriod)
   Every TELE_PERIOD seconds (default: 60), publishes:
     tele/tasmota_XXXXXX/STATE  →  JSON state payload:
       {"Time":"<ISO8601>","Uptime":"<D>T<HH:MM:SS>","UptimeSec":<int>,
        "Heap":23,"SleepMode":"Dynamic","Sleep":50,"LoadAvg":19,"MqttCount":1,
        "POWER":"<ON|OFF>","Wifi":{"AP":1,"SSId":"YourSSID",
        "BSSId":"AA:BB:CC:DD:EE:FF","Channel":6,"RSSI":100}}
   Time and Uptime/UptimeSec are live; all other fields are fixed fake values.

   Subscribes to: cmnd/tasmota_XXXXXX/TelePeriod
     - Empty payload  →  query; responds on stat/tasmota_XXXXXX/RESULT:
         {"TelePeriod": <current>}
     - Numeric payload greater or equal to 10 →  sets new period, responds as above, and immediately
         wakes the main loop to publish a fresh STATE message.
     - Non-numeric payload or numeric payload less than 10 →  treated as query (same response as empty payload)

3. Power ON/OFF
   Subscribes to: cmnd/tasmota_XXXXXX/POWER
     Accepted payloads: "ON", "1", "OFF", "0"  (case-insensitive)
   On receipt, in order:
     a) tele/tasmota_XXXXXX/STATE      →  full JSON state (POWER updated);
                                           does NOT reset the TelePeriod timer
     b) stat/tasmota_XXXXXX/RESULT     →  {"POWER":"<ON|OFF>"}
     c) stat/tasmota_XXXXXX/POWER      →  ON  or  OFF  (plain string, no quotes)

4. Shutdown
   On KeyboardInterrupt, publishes:
     tele/tasmota_XXXXXX/LWT  →  "Offline"  (retained)
"""

# Callback when the client receives a CONNACK response from the server
def on_connect(client, userdata, flags, rc, properties=None):
    print("Connected with result code " + str(rc))
    client.subscribe(TOPIC_COMMAND)
    client.subscribe("cmnd/tasmota_XXXXXX/TelePeriod")
    client.publish(TOPIC_STATUS, "Online", retain=True)

# Event to wake up the main loop
wake_up_event = threading.Event()

def get_state_payload():
    """Generate the state payload with current time and uptime"""
    elapsed_seconds = int(time.time() - start_time)
    uptime_days = elapsed_seconds // 86400
    uptime_hours = (elapsed_seconds % 86400) // 3600
    uptime_minutes = (elapsed_seconds % 3600) // 60
    uptime_secs = elapsed_seconds % 60
    uptime_str = f"{uptime_days}T{uptime_hours:02d}:{uptime_minutes:02d}:{uptime_secs:02d}"

    state = {
        "Time": datetime.now().isoformat(timespec='seconds'),
        "Uptime": uptime_str,
        "UptimeSec": elapsed_seconds,
        "Heap": 23,
        "SleepMode": "Dynamic",
        "Sleep": 50,
        "LoadAvg": 19,
        "MqttCount": 1,
        "POWER": POWER_STATE,
        "Wifi": {
            "AP": 1,
            "SSId": "YourSSID",
            "BSSId": "AA:BB:CC:DD:EE:FF",
            "Channel": 6,
            "RSSI": 100
        }
    }
    return json.dumps(state)

import requests

def on_message(client, userdata, msg):
    global POWER_STATE, TELE_PERIOD
    print(f"Received message: {msg.topic} {msg.payload.decode()}")
    if msg.topic == TOPIC_COMMAND:
        payload = msg.payload.decode().strip()
        if payload.upper() == "ON" or payload == "1":
            POWER_STATE = "ON"
        elif payload.upper() == "OFF" or payload == "0":
            POWER_STATE = "OFF"
        # Notify the REST API of the state change
        requests.post('http://localhost:8082/v1/set-state', json={"power": POWER_STATE})
    global POWER_STATE, TELE_PERIOD
    print(f"Received message: {msg.topic} {msg.payload.decode()}")
    if msg.topic == TOPIC_COMMAND:
        payload = msg.payload.decode().strip()
        if payload.upper() == "ON" or payload == "1":
            POWER_STATE = "ON"
        elif payload.upper() == "OFF" or payload == "0":
            POWER_STATE = "OFF"
        # Publish state immediately without resetting the TelePeriod timer
        client.publish(TOPIC_STATE, get_state_payload())
        # Publish result topic with JSON
        client.publish(TOPIC_RESULT, json.dumps({"POWER": POWER_STATE}))
        # Publish plain ON/OFF to stat/tasmota_XXXXXX/POWER (no JSON, no quotes)
        client.publish("stat/tasmota_XXXXXX/POWER", POWER_STATE)
    elif msg.topic == "cmnd/tasmota_XXXXXX/TelePeriod":
        payload = msg.payload.decode().strip()
        if payload:  # Command with a number payload
            try:
                TELE_PERIOD = max(10, int(payload))
                result = {"TelePeriod": TELE_PERIOD}
                client.publish(TOPIC_RESULT, json.dumps(result))
                wake_up_event.set()  # Wake up main loop to send telemetry immediately
            except ValueError:
                # Invalid payload behaves like a TelePeriod query
                result = {"TelePeriod": TELE_PERIOD}
                client.publish(TOPIC_RESULT, json.dumps(result))
        else:  # Command with no payload - just query current value
            result = {"TelePeriod": TELE_PERIOD}
            client.publish(TOPIC_RESULT, json.dumps(result))

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=CLIENT_ID)
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT, KEEPALIVE)

# Start the loop
client.loop_start()

try:
    while True:
        client.publish(TOPIC_STATE, get_state_payload())
        wake_up_event.wait(timeout=TELE_PERIOD)
        wake_up_event.clear()
except KeyboardInterrupt:
    client.publish(TOPIC_STATUS, "Offline", retain=True)
    client.disconnect()
