# mcp_server.py
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

class MCPRequestHandler(BaseHTTPRequestHandler):
    def _send_json(self, obj, status=200):
        data = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        if self.path != "/execute":
            self._send_json({"error": "Unknown endpoint"}, status=404)
            return

        content_length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(content_length).decode("utf-8")
        try:
            request_json = json.loads(raw)
        except Exception as e:
            self._send_json({"error": f"invalid json: {e}"}, status=400)
            return

        # The server object carries the agent/handler as attribute 'handler'
        handler = getattr(self.server, "handler", None)
        if handler is None:
            self._send_json({"error": "no handler configured on server"}, status=500)
            return

        try:
            response = handler.execute(request_json)
            self._send_json(response)
        except Exception as e:
            self._send_json({"error": f"handler exception: {e}"}, status=500)

class MCPServerThread:
    def __init__(self, host="127.0.0.1", port=8000, handler=None, name="mcp_server"):
        self.host = host
        self.port = port
        self.handler = handler
        self.name = name
        self._httpd = HTTPServer((self.host, self.port), MCPRequestHandler)
        # attach handler to server instance for request handlers to access
        setattr(self._httpd, "handler", self.handler)

    def start(self):
        t = threading.Thread(target=self._serve_forever, daemon=True)
        t.start()
        print(f"[{self.name}] HTTP MCP Server running on http://{self.host}:{self.port}")

    def _serve_forever(self):
        try:
            self._httpd.serve_forever()
        except KeyboardInterrupt:
            pass

    def shutdown(self):
        self._httpd.shutdown()
        print(f"[{self.name}] Server shut down.")
