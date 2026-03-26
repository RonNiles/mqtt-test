import http.server
import socketserver
import argparse
import paho.mqtt.client as mqtt

PORT = 8082
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "cmnd/tasmota_XXXXXX/POWER"

mqtt_client = mqtt.Client()
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)

class SimpleHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
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
                            mqtt_client.publish(MQTT_TOPIC, state);
                            alert('Slider is ' + state);
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

    with socketserver.TCPServer(("", args.port), SimpleHTTPRequestHandler) as httpd:
        print(f"Serving on port {args.port}")
        httpd.serve_forever()
