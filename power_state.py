from __future__ import annotations

import os
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

class PowerStateBase(ABC):
    """Abstract base class defining the public interface for PowerState.
    
    This class defines all externally-used methods. Subclasses provide the
    implementation, while internal helper methods (prefixed with _) remain
    private to the implementation.
    """

    @abstractmethod
    def wait_for_change(self, from_state: str, timeout: int) -> str:
        """Wait for the state to change and return the new state."""
        pass

    @abstractmethod
    def request_state_change(self, new_state: str) -> None:
        """Request a change to a new state: "on", "off", or "disconnected"."""
        pass

class PowerStateEmulator(PowerStateBase):
    """Emulates power state changes for testing purposes."""
    def __init__(self) -> None:
        """Initialize instance state and start the connection watchdog thread."""
        self._state: str = "disconnected"
        self._condition = threading.Condition()
        self._watchdog_thread = threading.Thread(target=self.connection_watchdog, daemon=True)
        self._watchdog_thread.start()

    def wait_for_change(self, from_state: str, timeout: int) -> str:
        with self._condition:
            if self._state == from_state:
                self._condition.wait(timeout=timeout)
            return self._state

    def request_state_change(self, new_state: str) -> None:
        with self._condition:
            if self._state == "loading":
                    return  # Ignore state change requests while loading
            if new_state != self._state:
                self._state = "loading"
                self._condition.notify_all()  # Notify that we're now loading
                # Simulate loading time
                self._condition.wait(timeout = 2)
                if self._state == "loading":
                    self._state = new_state  # Transition to the new state after loading    
                    self._condition.notify_all()

    def connection_watchdog(self) -> None:
        """Simulate a connection watchdog that reconnects the disconnected state."""
        while True:
            with self._condition:
                if self._state != "disconnected":
                    self._condition.wait(timeout=10)  # Wait for state change or timeout
                    continue
                print("connection_watchdog: Detected disconnected state, waiting for reconnection...")   
                self._condition.wait(timeout=10)
                if self._state != "disconnected":
                    print("connection_watchdog: Reconnection detected, state is now", self._state)
                    continue  # State changed, check again immediately
                print("connection_watchdog: No reconnection detected after 10 seconds, simulating reconnection...")
                self._state = "loading"
                self._condition.notify_all()
                self._condition.wait(timeout=1)  # Simulate loading time
                print("connection_watchdog: Transitioning to 'on' state")
                self._state = "on"
                self._condition.notify_all()

import paho.mqtt.client as mqtt
import json
BROKER = "localhost"  # Change to your MQTT broker address
PORT = 1883
KEEPALIVE = 60
CLIENT_ID = "on-off controller"

class PowerStateMQTT(PowerStateBase):
    """power state driven by MQTT messages."""
    def __init__(self) -> None:
        self._host = os.getenv("MQTT_HOST", "localhost")
        self._port = int(os.getenv("MQTT_PORT", "1883"))
        self._tasmota_id = os.getenv("TASMOTA_ID", "XXXXXX")
        self._state = "initializing"
        self._condition: threading.Condition = threading.Condition()
        self._mqtt_client: Any = None  # Placeholder for MQTT client
        self._power_state: str = "unknown"
        self._power_state_update_time: float = 0.0
        """Start the connection watchdog thread after initialization."""
        self.watchdog_thread = threading.Thread(target=self.connection_watchdog, daemon=True)
        self.watchdog_thread.start()

    def wait_for_change(self, from_state: str, timeout: int) -> str:
        with self._condition:
            if self._state == from_state:
                self._condition.wait(timeout=timeout)
            return self._state

    def request_state_change(self, new_state: str) -> None:
        with self._condition:
            if self._state == "loading":
                    return  # Ignore state change requests while loading
            if new_state != self._state:
                self._state = "loading"
                self._condition.notify_all()  # Notify that we're now loading
                # Simulate loading time
                self._condition.wait(timeout = 2)
                if self._state == "loading":
                    self._state = new_state  # Transition to the new state after loading
                    self._condition.notify_all()

    def connection_watchdog(self) -> None:
        """Connection watchdog to reconnect the disconnected state."""
        while True:
            with self._condition:
                if self._state == "initializing":
                    print("connection_watchdog: Initializing MQTT connection... ")
                    self._state = "connecting"
                    self._condition.notify_all()
                    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=CLIENT_ID)
                    client.on_connect = self._on_connect
                    client.on_message = self._on_message
                    self._mqtt_client = client
                    try:
                        connect_start_time = time.time()
                        client.connect(self._host, self._port, KEEPALIVE)
                        client.loop_start()
                    except Exception as e:
                        elapsed_time = time.time() - connect_start_time
                        print(f"connection_watchdog: MQTT connection failed after {elapsed_time:.2f} seconds:", e)
                        self._state = "disconnected"
                        self._condition.notify_all()
                        continue

                    self._condition.wait(timeout=2)  # Simulate initialization time
                    self._state = "on"
                    self._condition.notify_all()
                if self._state != "disconnected":
                    self._condition.wait(timeout=10)  # Wait for state change or timeout
                    continue
                print("connection_watchdog: Detected disconnected state, waiting for reconnection...")
                self._condition.wait(timeout=10)
                if self._state != "disconnected":
                    print("connection_watchdog: Reconnection detected, state is now", self._state)
                    continue  # State changed, check again immediately
                print("connection_watchdog: No reconnection detected after 10 seconds, simulating reconnection...")
                self._state = "loading"
                self._condition.notify_all()
                self._condition.wait(timeout=1)  # Simulate loading time
                print("connection_watchdog: Transitioning to 'on' state")
                self._state = "on"
                self._condition.notify_all()

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        lwt_topic = f"tele/tasmota_{self._tasmota_id}/LWT"
        power_topic = f"stat/tasmota_{self._tasmota_id}/POWER"
        power_command_topic = f"cmnd/tasmota_{self._tasmota_id}/POWER"
        with self._condition:
            print("MQTT connected with result code "+str(rc))
            if rc != 0:
                print("Failed to connect, return code:", rc)
                self._state = "disconnected"
                self._condition.notify_all()
                return
        # Subscribe to command topic
        client.subscribe(lwt_topic)
        client.subscribe(power_topic)
        print(f"Subscribed to MQTT topics: {lwt_topic}, {power_topic}")
        client.publish(power_command_topic)  # Request current state immediately

    def _on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode().strip()
        print(f"Received MQTT message: {topic} {payload}")
        with self._condition:
            if topic.endswith("/LWT"):
                if payload == "Online":
                    print("MQTT LWT indicates device is online")
                    self._state = "on"
                else:
                    print("MQTT LWT indicates device is offline")
                    self._state = "disconnected"
                self._condition.notify_all()
            elif topic.endswith("/POWER"):
                if payload in {"ON", "OFF"}:
                    print(f"MQTT POWER state changed to {payload}")
                    self._state = payload.lower()
                    self._condition.notify_all()
