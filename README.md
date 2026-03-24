# MQTT Tasmota Emulator

This project is an MQTT emulator for Tasmota devices. It simulates a Tasmota device by connecting to an MQTT broker and responding to commands.

## Features

- Connects to an MQTT broker
- Subscribes to command topics
- Publishes status and state updates
- Uses threading to handle message reception and state updates

## Requirements

- Python 3.x
- `paho-mqtt` library

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

## License

This project is licensed under the MIT License.
