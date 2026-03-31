# MQTT Tasmota Emulator

This project is an MQTT emulator for Tasmota devices. It simulates a Tasmota device by connecting to an MQTT broker and responding to commands.

It also includes a Go port of the web server and power-state manager:

- `mqtt_webserver.go`
- `power_state.go`

## Features

- Connects to an MQTT broker
- Subscribes to command topics
- Publishes status and state updates
- Uses threading to handle message reception and state updates

## Requirements

- Python 3.x
- `paho-mqtt` library
- Go 1.24+

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Install the required Python package:
   ```bash
   pip install paho-mqtt
   ```

3. Download Go dependencies:
   ```bash
   go mod tidy
   ```

## Usage

1. Update the configuration in `mqtt_tasmota_emulator.py` with your MQTT broker details.

2. Run the emulator:
   ```bash
   python mqtt_tasmota_emulator.py
   ```

3. Use an MQTT client to publish commands to the emulator:
   ```bash
   mosquitto_pub -t cmnd/tasmota_XXXXXX/power -m ON
   ```

## Go Web Server Usage

The Go server provides:

- `GET /` serves `webserver.html`
- `GET /api/events` provides server-sent events (SSE)
- `POST /api/power` requests power state changes

Set environment variables as needed:

```bash
export MQTT_HOST=localhost
export MQTT_PORT=1883
export TASMOTA_ID=XXXXXX
```

Build and run:

```bash
go build ./...
go run mqtt_webserver.go power_state.go --port 8082
```

Example request:

```bash
curl -X POST http://127.0.0.1:8082/api/power \
   -H 'Content-Type: application/json' \
   -d '{"value":"on"}'
```

## License

This project is licensed under the MIT License.
