from flask import Flask, request, jsonify
import threading
import http.server
import socketserver
import argparse
import os
import paho.mqtt.client as mqtt
import requests

PORT = 8082
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "cmnd/tasmota_XXXXXX/POWER"
WEBPAGE_FILE = os.path.join(os.path.dirname(__file__), "webserver.html")

mqtt_client = mqtt.Client()
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)

app = Flask(__name__)

state_lock = threading.Lock()
device_state = {"power": "OFF"}

@app.route('/v1/set-state', methods=['POST'])
def set_state():
    global device_state
    data = request.json
    with state_lock:
        device_state['power'] = data.get('power', 'OFF')
    mqtt_client.publish(MQTT_TOPIC, device_state['power'])
    return jsonify(device_state)

@app.route('/v1/get-state', methods=['GET'])
def get_state():
    with state_lock:
        return jsonify(device_state)

@app.route('/v1/wait-state-change', methods=['GET'])
def wait_state_change():
    # This is a placeholder for a more complex implementation
    # that would block until the state changes.
    return jsonify(device_state)

class SimpleHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    _webpage_cache = None
    _webpage_mtime = None

    def do_GET(self):
        if self.path == '/':
            try:
                stat_result = os.stat(WEBPAGE_FILE)
                current_mtime = stat_result.st_mtime

                if (
                    self.__class__._webpage_cache is None
                    or self.__class__._webpage_mtime != current_mtime
                ):
                    with open(WEBPAGE_FILE, 'rb') as html_file:
                        self.__class__._webpage_cache = html_file.read()
                    self.__class__._webpage_mtime = current_mtime
            except OSError as exc:
                self.send_error(500, f"Could not load {WEBPAGE_FILE}: {exc}")
                return

            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(self.__class__._webpage_cache)
        else:
            self.send_error(404)
            self.end_headers()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Minimalist Web Server')
    parser.add_argument('--port', type=int, default=PORT, help='Port to run the web server on')
    args = parser.parse_args()

    threading.Thread(target=lambda: app.run(port=args.port)).start()
    with socketserver.TCPServer(("", args.port + 1), SimpleHTTPRequestHandler) as httpd:
        print(f"Serving on port {args.port + 1}")
        httpd.serve_forever()
