import socketserver
import json

class MCPRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(4096).strip()
        request = json.loads(data.decode("utf-8"))
        print(f"[{self.server.name}] Received:", request)
        response = self.server.agent.execute(request)
        self.request.sendall(json.dumps(response).encode("utf-8"))

class MCPServer(socketserver.TCPServer):
    allow_reuse_address = True

    def __init__(self, name, agent, host, port):
        super().__init__((host, port), MCPRequestHandler)
        self.name = name
        self.agent = agent

    def start(self):
        print(f"[{self.name}] MCP Server running on {self.server_address}")
        self.serve_forever()
