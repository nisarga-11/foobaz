import os
from flask import Flask, request, jsonify
from agents.backup_restore_agent2 import BackupRestoreAgent2

# Disable LLM requirements
os.environ["CREWAI_DISABLE_LLM"] = "true"
os.environ["OPENAI_API_KEY"] = "DUMMY_KEY"

app = Flask(__name__)

class MCPServer2:
    def __init__(self):
        self.agent = BackupRestoreAgent2()
        self.name = "mcp_server2"
        self.port = 5002  # ✅ different port

    def start(self):
        print(f"🚀 {self.name} HTTP server starting on port {self.port} ...")
        app.run(host="127.0.0.1", port=self.port, threaded=True)

mcp_server2 = MCPServer2()

@app.route("/execute", methods=["POST"])
def execute_action():
    data = request.get_json(force=True)
    result = mcp_server2.agent.execute(data)
    return jsonify({"result": result})
