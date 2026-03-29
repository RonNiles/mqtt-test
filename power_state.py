from __future__ import annotations

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

@dataclass
class PowerStateEmulator(PowerStateBase):
    """Emulates power state changes for testing purposes."""
    _state: str = field(default="disconnected", init=False)
    _condition: threading.Condition = field(default_factory=threading.Condition, init=False)

    def __post_init__(self) -> None:
        """Start the connection watchdog thread after initialization."""
        watchdog_thread = threading.Thread(target=self.connection_watchdog, daemon=True)
        watchdog_thread.start()

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
