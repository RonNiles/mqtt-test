import paho.mqtt.client as mqtt
import time

# Configuration
BROKER = "localhost"  # Change to your MQTT broker address
PORT = 1883
KEEPALIVE = 60
TOPIC_STATUS = "tele/tasmota_XXXXXX/LWT"
TOPIC_COMMAND = "cmnd/tasmota_XXXXXX/power"
TOPIC_STATE = "stat/tasmota_XXXXXX/POWER"
CLIENT_ID = "tasmota_emulator"
POWER_STATE = "OFF"

# Callback when the client receives a CONNACK response from the server
def on_connect(client, userdata, flags, rc, properties=None):
    print("Connected with result code " + str(rc))
    client.subscribe(TOPIC_COMMAND)
    client.publish(TOPIC_STATUS, "Online", retain=True)

# Callback when a PUBLISH message is received from the server
def on_message(client, userdata, msg):
    global POWER_STATE
    print(f"Received message: {msg.topic} {msg.payload.decode()}")
    if msg.topic == TOPIC_COMMAND:
        if msg.payload.decode().upper() == "ON":
            POWER_STATE = "ON"
        elif msg.payload.decode().upper() == "OFF":
            POWER_STATE = "OFF"
        client.publish(TOPIC_STATE, POWER_STATE)

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=CLIENT_ID)
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT, KEEPALIVE)

# Start the loop
client.loop_start()

try:
    while True:
        client.publish(TOPIC_STATE, POWER_STATE)
        time.sleep(30)
except KeyboardInterrupt:
    client.publish(TOPIC_STATUS, "Offline", retain=True)
    client.disconnect()
