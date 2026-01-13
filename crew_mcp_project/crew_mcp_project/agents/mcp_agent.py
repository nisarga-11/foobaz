import os
import argparse
from crewai import Crew, Agent, Process
from crewai.tools import tool  # ✅ Correct import for CrewAI 1.3.0+
from agents.backup_restore_agent import PgbackrestBackupTool, PgbackrestRestoreTool, get_backup_restore_agent

# ----------------------------
# Parse environment file
# ----------------------------
parser = argparse.ArgumentParser(description='Run a CrewAI MCP agent.')
parser.add_argument('--env_file', type=str, required=True, help='Path to the .env file for this agent.')
args = parser.parse_args()

env_vars = {}
try:
    with open(args.env_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            key, value = line.split('=', 1)
            env_vars[key.strip()] = value.strip().strip('"').strip("'")
except FileNotFoundError:
    print(f"❌ Error: The .env file '{args.env_file}' was not found.")
    exit(1)

# ----------------------------
# Extract environment variables
# ----------------------------
SERVER_HOST = env_vars.get("SERVER_HOST")
STANZA_NAME = env_vars.get("STANZA_NAME")
PG_PATH = env_vars.get("PG_PATH")
AGENT_ROLE = env_vars.get("AGENT_ROLE")
mcp_port_str = env_vars.get("MCP_PORT")

if mcp_port_str is None:
    print("❌ ERROR: MCP_PORT is not set in the .env file. Please check the file.")
    exit()

MCP_PORT = int(mcp_port_str)

# ----------------------------
# Define tools with CrewAI decorator
# ----------------------------

@tool("Backup Tool for PostgreSQL")
def backup_wrapper(type: str = 'incremental') -> str:
    """Performs a pgBackRest backup for the configured stanza."""
    return PgbackrestBackupTool()._run(host=SERVER_HOST, stanza=STANZA_NAME, type=type)


@tool("Restore Tool for PostgreSQL")
def restore_wrapper(set_id: str, pg_path: str) -> str:
    """Restores a pgBackRest backup for the configured stanza."""
    return PgbackrestRestoreTool()._run(host=SERVER_HOST, stanza=STANZA_NAME, set_id=set_id, pg_path=pg_path)


# ----------------------------
# Create Agent with tools
# ----------------------------
agent = get_backup_restore_agent(AGENT_ROLE, SERVER_HOST, [backup_wrapper, restore_wrapper])

# ----------------------------
# Create Crew and start MCP host
# ----------------------------
crew = Crew(
    agents=[agent],
    tasks=[],
    process=Process.sequential
)

print(f"🚀 Starting MCP agent for stanza '{STANZA_NAME}' on port {MCP_PORT} ...")

crew.mcp_host(
    host="0.0.0.0",
    port=MCP_PORT
)
