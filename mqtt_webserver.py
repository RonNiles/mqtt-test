from flask import Flask, request, jsonify
import threading
import http.server
import socketserver
import argparse
import paho.mqtt.client as mqtt
import requests

PORT = 8082
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "cmnd/tasmota_XXXXXX/POWER"

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
    def do_POST(self):
        if self.path == '/v1/set-state':
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            response = requests.post(f'http://localhost:{PORT}/v1/set-state', data=post_data, headers={'Content-Type': 'application/json'})
            self.send_response(response.status_code)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(response.content)
        else:
            self.send_error(404)
            self.end_headers()
        if self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"""
                <!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>On-Off Slider</title>
                </head>
                <body>
                    <h1>On-Off Slider</h1>
                    <label class="switch">
                        <input type="checkbox" id="slider">
                        <span class="slider"></span>
                    </label>
                    <script>
                        const slider = document.getElementById('slider');
                        slider.addEventListener('change', function() {
                            const state = this.checked ? 'ON' : 'OFF';
                            fetch('/v1/set-state', {
                                method: 'POST',
                                headers: {
                                    'Content-Type': 'application/json'
                                },
                                body: JSON.stringify({ power: state })
                            })
                            .then(response => response.json())
                            .then(data => {
                                alert('Slider is ' + data.power);
                            });
                        });
                    </script>
                    <style>
                        .switch {
                            position: relative;
                            display: inline-block;
                            width: 60px;
                            height: 34px;
                        }
                        .switch input {
                            opacity: 0;
                            width: 0;
                            height: 0;
                        }
                        .slider {
                            position: absolute;
                            cursor: pointer;
                            top: 0;
                            left: 0;
                            right: 0;
                            bottom: 0;
                            background-color: #ccc;
                            transition: .4s;
                        }
                        .slider:before {
                            position: absolute;
                            content: "";
                            height: 26px;
                            width: 26px;
                            left: 4px;
                            bottom: 4px;
                            background-color: white;
                            transition: .4s;
                        }
                        input:checked + .slider {
                            background-color: #2196F3;
                        }
                        input:checked + .slider:before {
                            transform: translateX(26px);
                        }
                        .slider.round {
                            border-radius: 34px;
                        }
                        .slider.round:before {
                            border-radius: 50%;
                        }
                    </style>
                </body>
                </html>
            """)
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
