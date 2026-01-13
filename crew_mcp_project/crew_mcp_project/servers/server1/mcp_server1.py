import os
from flask import Flask, request, jsonify
from agents.backup_restore_agent1 import BackupRestoreAgent1

# Disable LLM requirements
os.environ["CREWAI_DISABLE_LLM"] = "true"
os.environ["OPENAI_API_KEY"] = "DUMMY_KEY"

app = Flask(__name__)

class MCPServer1:
    def __init__(self):
        self.agent = BackupRestoreAgent1()
        self.name = "mcp_server1"
        self.port = 5001  # ✅ different port

    def start(self):
        print(f"🚀 {self.name} HTTP server starting on port {self.port} ...")
        app.run(host="127.0.0.1", port=self.port, threaded=True)

mcp_server1 = MCPServer1()

@app.route("/execute", methods=["POST"])
def execute_action():
    data = request.get_json(force=True)
    result = mcp_server1.agent.execute(data)
    return jsonify({"result": result})
