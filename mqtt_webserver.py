from http import HTTPStatus
import json
import threading
import http.server
import socketserver
import argparse
import os
import time
from urllib.parse import parse_qs, urlparse
from typing import Any
from power_state import PowerStateEmulator, PowerStateBase

PORT = 8082
WEBPAGE_FILE = os.path.join(os.path.dirname(__file__), "webserver.html")

class PowerRequestHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    _webpage_cache = None
    _webpage_mtime = None
    _power_state: PowerStateBase | None = None

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == '/':
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
                self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, f"Could not load {WEBPAGE_FILE}: {exc}")
                return

            self.send_response(HTTPStatus.OK)
            self.send_header('Content-type', 'text/html')
            self.send_header('Content-Length', str(len(self.__class__._webpage_cache)))
            self.end_headers()
            self.wfile.write(self.__class__._webpage_cache)
            return

        if parsed.path == "/api/events":
            self.stream_events()
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def parse_int(self, value: str, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def wait_for_change(self, from_state: str, timeout: int) -> dict[str, Any]:
        remote_port = self.client_address[1]
        state = self.__class__._power_state.wait_for_change(from_state, timeout)
        changed = state != from_state
        if changed:
            print(f"[{remote_port}] State changed from {from_state} to {state}")
        else:
            print(f"[{remote_port}] Waited for change from {from_state}, current state is {state}, changed: {changed}")
        return {"state": state, "changed": changed}

    def stream_events(self) -> None:
        remote_port = self.client_address[1]
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        print(f"[{remote_port}] Opened event stream")
        last_state = None
        try:
            while True:
                state = self.__class__._power_state.wait_for_change(last_state if last_state else "__initial__", timeout=25)

                payload = json.dumps({"state": state})
                self.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
                self.wfile.flush()
                last_state = state
                print(f"[{remote_port}] Sent event: {payload}")
        except (BrokenPipeError, ConnectionResetError):
            print(f"[{remote_port}] Closed event stream")

    def do_POST(self) -> None:
        """Handle POST requests for power-state updates."""
        parsed = urlparse(self.path)
        if parsed.path != "/api/power":
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except (TypeError, ValueError):
            length = 0
        body = self.rfile.read(length) if length > 0 else b"{}"

        try:
            data = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            self.write_json({"error": "Invalid JSON body"}, status=HTTPStatus.BAD_REQUEST)
            return

        value = data.get("value", "").strip().lower()
        if value not in {"on", "off", "disconnected", "loading"}:
            self.write_json({"error": "value must be on, off, disconnected, or loading"}, status=HTTPStatus.BAD_REQUEST)
            return

        print(f"Setting state to {value}")
        self.__class__._power_state.request_state_change(value)
        # Wait briefly for state transition to complete
        time.sleep(0.1)
        self.write_json({"state": value})

    def write_json(self, data: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        response = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

class PowerServer(http.server.ThreadingHTTPServer):
    def __init__(
        self,
        server_address: tuple[str, int],
        power_state: str = "loading",
    ) -> None:
        super().__init__(server_address, PowerRequestHandler)
        self.power_state = power_state
        self.allow_reuse_address = True  

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Minimalist Web Server')
    parser.add_argument('--port', type=int, default=PORT, help='Port to run the web server on')
    args = parser.parse_args()

    # Create PowerStateEmulator and inject it into the handler
    power_state = PowerStateEmulator()
    PowerRequestHandler._power_state = power_state

    server = PowerServer(("127.0.0.1", args.port), PowerRequestHandler)
    print(f"Serving on port {args.port}")
    threading.Thread(target=server.serve_forever, daemon=True).start()

    try:
        while True:
            threading.Event().wait(1)
    except KeyboardInterrupt:
        print("Shutting down server...")
        server.shutdown()
        server.server_close()