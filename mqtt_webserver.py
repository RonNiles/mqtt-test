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
<label class="switch" for="pref" data-state="off">
  <input type="checkbox" id="pref" name="pref" />
  <div class="toggle">
    <div class="spinner"></div>
  </div>
</label>
                    <script>
const toggleSwitches = [...document.querySelectorAll('.switch')];
const stateOrder = ['off', 'on', 'disconnected'];

function applyState(toggleSwitch, state) {
  const checkbox = toggleSwitch.querySelector('input[type="checkbox"]');
  toggleSwitch.dataset.state = state;
  checkbox.checked = state === 'on';
  checkbox.indeterminate = state === 'disconnected';
}

toggleSwitches.forEach((toggleSwitch) => {
  const checkbox = toggleSwitch.querySelector('input[type="checkbox"]');
  const initialState = checkbox.checked ? 'on' : (toggleSwitch.dataset.state || 'off');
  applyState(toggleSwitch, initialState);

  toggleSwitch.addEventListener('click', (e) => {
    e.preventDefault();
    if (toggleSwitch.classList.contains('loading')) {
      return;
    }

    const currentState = toggleSwitch.dataset.state || 'off';
    const currentIndex = stateOrder.indexOf(currentState);
    const nextState = stateOrder[(currentIndex + 1) % stateOrder.length];

    toggleSwitch.classList.add('loading');
    setTimeout(() => {
      toggleSwitch.classList.remove('loading');
      applyState(toggleSwitch, nextState);
    }, 1000);
  });
});
                    </script>
                    <style>
:root {
  --blue: #007aff;
  --green: #4bd964;
  --disabled: #9ba2b5;
}
* {
  box-sizing: border-box;
}

html, body {
  min-height: 100vh;
}

body {
  margin: 0;
  display: flex;
  flex-direction: row;
  justify-content: center;
  align-items: center;
}

.switch {
  position: relative;
  display: flex;
  align-items: center;
  // outline: 1px solid #e4e6ea;
  width: 4rem;
  height: 2rem;
  border-radius: 1.5rem;
  background-color: #f4f5f8;
  box-shadow: inset 0px 0px 8px rgba(180, 185, 195, 0.4);
  cursor: pointer;
  
  input[type="checkbox"] {
    position: absolute;
    opacity: 0;
    top: -20px;
    pointer-events: none;
  }
  
  &.loading {
    opacity: 0.5;
    pointer-events: none;
  }
}

.toggle {
  position: absolute;
  display: flex;
  justify-content: center;
  align-items: center;
  width: 2rem;
  height: 2rem;
  border-radius: 50%;
  transition: width 250ms ease-out, left 250ms ease-out;
}
.switch input[type="checkbox"]:focus ~ .toggle {
  outline-offset: 0.125rem;
}
.switch.loading input[type="checkbox"]:not(:checked) ~ .toggle {
  width: 2rem;
  background: var(--disabled);
}
.switch.loading input[type="checkbox"]:checked:focus ~ .toggle,
.switch.loading input[type="checkbox"]:not(:checked):focus ~ .toggle {
  outline: 0.25rem solid var(--disabled);
}
.switch.loading input[type="checkbox"]:checked ~ .toggle {
  width: 2rem;
  left: calc(100% - 2rem);
  background: var(--disabled);
}

.switch[data-state="off"] .toggle {
  left: 0;
  background: var(--blue);
}

.switch[data-state="on"] .toggle {
  left: calc(100% - 2rem);
  background: var(--green);
}

.switch[data-state="disconnected"] .toggle {
  left: calc(50% - 1rem);
  background: var(--disabled);
}

.switch[data-state="off"]:focus-within .toggle {
  outline: 0.25rem solid var(--blue);
}

.switch[data-state="on"]:focus-within .toggle {
  outline: 0.25rem solid var(--green);
}

.switch[data-state="disconnected"]:focus-within .toggle {
  outline: 0.25rem solid var(--disabled);
}

.switch.loading[data-state="disconnected"] .toggle {
  width: 2rem;
  left: calc(50% - 1rem);
  background: var(--disabled);
}

.spinner {
  opacity: 0;
  width: 1.25rem;
  height: 1.25rem;
  border: 0.25rem solid #FFF;
  border-bottom-color: transparent;
  border-radius: 50%;
  display: inline-block;
  box-sizing: border-box;
  animation: rotate 1s linear infinite;
}

.switch.loading .spinner {
  opacity: 1;
}

@keyframes rotate {
  0% {
    transform: rotate(0deg);
  }
  100% {
    transform: rotate(360deg);
  }
} 

@keyframes turnOn {
  0% { left: 0; width: 3rem; background: var(--blue); }
  50% { left: -0.5rem; width: 2rem; background: var(--blue); }
  100% { left: calc(100% - 3rem); width: 3rem; background: var(--green); }
}

@keyframes turnOff {
  0% { left: calc(100% - 3rem); width: 3rem; background: var(--green); }
  50% { left: calc(100% - 1.5rem); width: 2rem; background: var(--green); }
  100% { left: 0; width: 3rem; background: var(--blue); }
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
