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
                self._condition.wait(timeout = 1)
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
        self._state = "disconnected"
        self._disconnect_count = 0
        self._loading_count = 0
        self._condition: threading.Condition = threading.Condition()
        self._mqtt_client: Any = None  # Placeholder for MQTT client
        self._power_state: str = "unknown"
        self._power_state_update_time: float = 0.0

        self._lwt_topic = f"tele/tasmota_{self._tasmota_id}/LWT"
        self._power_topic = f"stat/tasmota_{self._tasmota_id}/POWER"
        self._power_command_topic = f"cmnd/tasmota_{self._tasmota_id}/POWER"

        """Start the MQTT manager thread after initialization."""
        self.mqtt_thread = threading.Thread(target=self.mqtt_manager, daemon=True)
        self.mqtt_thread.start()

    def wait_for_change(self, from_state: str, timeout: int) -> str:
        with self._condition:
            if self._state == from_state:
                self._condition.wait(timeout=timeout)
            return self._state

    def request_state_change(self, new_state: str) -> None:
        with self._condition:
            if self._state not in {"on", "off"}:
                print(f"request_state_change: Ignoring state change request to '{new_state}' because current state is '{self._state}'")
                return  # Ignore state change requests while loading or connecting

            self._state = "loading"
            self._loading_count += 1
            self._condition.notify_all()  # Notify that we're now loading

            if new_state in {"on", "off"}:
                if self._mqtt_client is not None:
                    try:
                        self._mqtt_client.publish(self._power_command_topic, new_state.upper())
                        print(f"request_state_change: Published MQTT message to change state to '{new_state}'")
                        return # Assume state will change based on MQTT message, wait for confirmation in mqtt_manager
                    except Exception as e:
                        print("Failed to publish MQTT message:", e)

            self._cleanup_client()  # If we can't publish, treat it as a disconnection
            with self._condition:
                self._state = "disconnected"
                self._disconnect_count += 1
                print(f"request_state_change: Failed to publish MQTT message, setting state to 'disconnected'")
                self._condition.notify_all()

    def mqtt_manager(self) -> None:
        time.sleep(2)  # Brief delay to allow main thread to start
        """MQTT manager to handle connection and messages."""
        with self._condition:
            self._state = "loading"
            self._loading_count += 1
            print ("mqtt_manager: state set to 'loading', count: ", self._loading_count)
            self._condition.notify_all()  # Notify that we're now loading

        need_reconnect = True
        need_disconnect = False
        while True:
            if need_reconnect:
                print("mqtt_manager: Attempting to start MQTT client...")
                self._start_client()
                need_reconnect = False

            if need_disconnect:
                print(f"mqtt_manager: Attempting to clean up MQTT client at address {hex(id(self._mqtt_client))}...")
                self._cleanup_client()
                with self._condition:
                    self._state = "disconnected"
                    self._disconnect_count += 1
                    self._condition.notify_all()
                need_disconnect = False

            with self._condition:
                previous_state = self._state
                previous_disconnect_count = self._disconnect_count
                previous_loading_count = self._loading_count
                self._condition.wait(timeout=10)  # Wait for state change or timeout
                if self._state == "disconnected" and previous_state == "disconnected" and self._disconnect_count == previous_disconnect_count:
                    print(f"mqtt_manager: Detected prolonged disconnected state (count {self._disconnect_count}), will attempt to reconnect...")
                    self._state = "loading"
                    self._loading_count += 1
                    self._condition.notify_all()
                    need_reconnect = True
                    continue
                if self._state == "loading" and previous_state == "loading" and self._loading_count == previous_loading_count:
                    print(f"mqtt_manager: Detected prolonged loading state (count {self._loading_count}), disconnecting")
                    need_disconnect = True
                    continue

    def _start_client(self) -> None:
        """Start the MQTT client and connect to the broker."""
        self._mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=CLIENT_ID, reconnect_on_failure=False)
        print (f"_start_client: Created MQTT client with id {hex(id(self._mqtt_client))}")
        self._mqtt_client.on_connect = self._on_connect
        self._mqtt_client.on_disconnect = self._on_disconnect
        self._mqtt_client.on_message = self._on_message
        try:
            print(f"Attempting to connect to MQTT broker at {self._host}:{self._port}...")
            connect_start_time = time.time()
            self._mqtt_client.connect(self._host, self._port, KEEPALIVE)
            print("MQTT client connected successfully after {:.2f} seconds".format(time.time() - connect_start_time))
            self._mqtt_client.loop_start()
        except Exception as e:
            elapsed_time = time.time() - connect_start_time
            print(f"_start_client: MQTT connection failed after {elapsed_time:.2f} seconds:", e)
            with self._condition:
                self._state = "disconnected"
                self._disconnect_count += 1
                print(f"state set to 'disconnected' for client {hex(id(self._mqtt_client))} in _start_client due to connection failure")
                self._condition.notify_all()

    def _cleanup_client(self) -> None:
        client = self._mqtt_client
        self._mqtt_client = None
        if client is None:
            return
        try:
            client.disconnect()
        except Exception:
            pass
        try:
            client.loop_stop()
        except Exception:
            pass

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        # check if connection was successful
        if rc != 0:
            print("MQTT connection failed with result code "+str(rc))
            # Destroy the client to trigger a clean reconnect.
            self._cleanup_client()
            with self._condition:
                self._state = "disconnected"
                self._disconnect_count += 1
                print(f"state set to 'disconnected' for client {hex(id(self._mqtt_client))} in _on_connect due to failed connection")
                self._condition.notify_all()
            return
        # Subscribe to command topic
        client.subscribe(self._lwt_topic)
        client.subscribe(self._power_topic)
        print(f"Subscribed to MQTT topics: {self._lwt_topic}, {self._power_topic}")
        client.publish(self._power_command_topic)  # Request current state immediately

    def _on_disconnect(self, client, userdata, flags, rc, properties=None):
        if not client is self._mqtt_client:
            print(f"Received on_disconnect for an old client {hex(id(client))}, ignoring")
            return
        with self._condition:
            self._state = "disconnected"
            self._disconnect_count += 1
            print(f"state set to 'disconnected' for client {hex(id(self._mqtt_client))} in _on_disconnect")
            self._condition.notify_all()

    def _on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode().strip()
        print(f"Received MQTT message: {topic} {payload}")
        cleanup_needed = False
        with self._condition:
            if topic.endswith("/LWT"):
                if payload == "Online":
                    print("MQTT LWT indicates device is online")
                else:
                    print("MQTT LWT indicates device is offline")
                    cleanup_needed = True
            elif topic.endswith("/POWER"):
                if payload in {"ON", "OFF"}:
                    print(f"MQTT POWER state changed to {payload}")
                    self._state = payload.lower()
                    self._condition.notify_all()
                return

        if cleanup_needed:
            with self._condition:
                if self._state != "loading":  # Only set to loading if we're not already in the middle of a state change
                    self._state = "loading"
                    self._loading_count += 1
                    print("mqtt_manager: state set to 'loading', count: ", self._loading_count)

                    self._condition.notify_all()
            self._cleanup_client()  # on_disconnect will be called which will set state to disconnected and trigger reconnect logic in mqtt_manager
