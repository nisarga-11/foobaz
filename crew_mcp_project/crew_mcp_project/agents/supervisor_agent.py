import threading
import time
import requests
import yaml
import re
import json
import os
from crewai import Agent, Crew, Process
from servers.server1.mcp_server1 import mcp_server1
from servers.server2.mcp_server2 import mcp_server2
from ollama_api import ask_ollama  # Proper Ollama API logic

SERVER_HOST = "127.0.0.1"

# ----------------------------
# Cluster → databases mapping
# ----------------------------
CLUSTER_DATABASES = {
    "pg1": ["db1", "db2"],
    "pg2": ["db3", "db4"]
}

CONSISTENCY_LOG = "consistency_groups.json"


# ----------------------------
# Start MCP servers
# ----------------------------
def start_server(server):
    server.start()

print("🚀 Starting MCP servers...")
threading.Thread(target=start_server, args=(mcp_server1,), daemon=True).start()
threading.Thread(target=start_server, args=(mcp_server2,), daemon=True).start()
time.sleep(2)
print("✅ Both MCP servers running with pgBackRest backend.\n")

# ----------------------------
# Load YAML Configs
# ----------------------------
with open("agents.yaml", "r") as f:
    agents_data = yaml.safe_load(f).get("agents", [])

with open("task.yaml", "r") as f:
    tasks_data = yaml.safe_load(f).get("tasks", [])

# Build a mapping: agent_id -> port
AGENT_PORTS = {}
for agent in agents_data:
    agent_id = agent["id"]
    for tool in agent.get("tools", []):
        if tool.get("type") == "mcp":
            AGENT_PORTS[agent_id] = tool["port"]

# Build a mapping: task_id -> task actions
TASKS = {task["id"].lower(): task for task in tasks_data}

# ----------------------------
# Helper: Execute agent commands
# ----------------------------
def execute_agent(agent_id, actions):
    port = AGENT_PORTS.get(agent_id)
    if not port:
        return f"❌ Unknown agent '{agent_id}'"

    try:
        response = requests.post(
            f"http://{SERVER_HOST}:{port}/execute",
            json={"actions": actions},
            timeout=60
        )
        return response.json().get("result", "No result returned")
    except Exception as e:
        return f"❌ [{agent_id}] Request failed: {e}"

# Cluster → agent mapping
CLUSTER_TO_AGENT = {
    "pg1": "backup_restore_pg1",
    "pg2": "backup_restore_pg2"
}

# ----------------------------
# Consistency Group Logging
# ----------------------------
def log_consistency_group(group_data):
    if os.path.exists(CONSISTENCY_LOG):
        with open(CONSISTENCY_LOG, "r") as f:
            try:
                existing = json.load(f)
            except json.JSONDecodeError:
                existing = []
    else:
        existing = []

    existing.append(group_data)
    with open(CONSISTENCY_LOG, "w") as f:
        json.dump(existing, f, indent=2)


# ----------------------------
# Run task by task_id
# ----------------------------
def run_task(task_id, inputs=None):
    task_id = task_id.lower()
    results = {}

    # Consistency group detection
    if "both" in task_id or "pg1" in task_id and "pg2" in task_id:
        print("🔄 Running in Consistency Group Mode...")
        group_timestamp = time.strftime("%Y%m%d-%H%M%S")
        group_result = {}

        def run_cluster(cluster):
            agent_id = CLUSTER_TO_AGENT[cluster]
            action = "backup"
            payload = {"action": action, "cluster": cluster}
            if inputs:
                payload.update(inputs)
            result = execute_agent(agent_id, [payload])
            group_result[cluster] = result

        threads = []
        for cluster in ["pg1", "pg2"]:
            t = threading.Thread(target=run_cluster, args=(cluster,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Save consistency group result
        group_entry = {
            "timestamp": group_timestamp,
            "clusters": {
                "pg1": group_result.get("pg1"),
                "pg2": group_result.get("pg2")
            }
        }
        log_consistency_group(group_entry)

        print(f"✅ Consistency Group completed at {group_timestamp}")
        return group_entry

    # Non-group logic — preserve exactly
    task = TASKS.get(task_id)
    if not task:
        m = re.match(r"(backup|restore|list)_(pg\d+)(?:_(db\d+))?", task_id)
        if not m:
            return f"⚠️ Unknown task '{task_id}'"
        action_type, cluster, db = m.groups()
        agent_id = CLUSTER_TO_AGENT.get(cluster)
        if not agent_id:
            return f"❌ Unknown cluster '{cluster}'"
        task = {
            "id": task_id,
            "actions": [{
                "agent": agent_id,
                "action": action_type,
                "cluster": cluster,
                "database": db
            }]
        }

    for action in task.get("actions", []):
        agent_id = action.get("agent")
        payload_base = {k: v for k, v in action.items() if k != "agent"}
        cluster = payload_base.get("cluster")
        db = payload_base.get("database")
        if inputs:
            payload_base.update(inputs)

        if action.get("action") == "backup":
            if db:
                results.setdefault(agent_id, {})[db] = execute_agent(agent_id, [payload_base])
            elif cluster:
                for db_name in CLUSTER_DATABASES.get(cluster, []):
                    payload = payload_base.copy()
                    payload["database"] = db_name
                    results.setdefault(agent_id, {})[db_name] = execute_agent(agent_id, [payload])
            else:
                results[agent_id] = execute_agent(agent_id, [payload_base])

        elif action.get("action") == "restore":
            if db:
                payload_base["database"] = db
                results.setdefault(agent_id, {})[db] = execute_agent(agent_id, [payload_base])
            elif cluster:
                for db_name in CLUSTER_DATABASES.get(cluster, []):
                    payload = payload_base.copy()
                    payload["database"] = db_name
                    results.setdefault(agent_id, {})[db_name] = execute_agent(agent_id, [payload])
            else:
                results[agent_id] = execute_agent(agent_id, [payload_base])

        elif action.get("action") == "list":
            results[agent_id] = execute_agent(agent_id, [payload_base])

        else:
            results[agent_id] = f"Unknown action {action.get('action')}"

    return results


# ----------------------------
# NLP Command Parser
# ----------------------------
def parse_command_with_ollama(user_input: str) -> dict:
    user_input_lower = user_input.lower()
    tasks = []
    action_type = "backup"
    if "list" in user_input_lower:
        action_type = "list"
    elif "restore" in user_input_lower:
        action_type = "restore"

    backup_type = "full"
    if "incremental" in user_input_lower or "incr" in user_input_lower:
        backup_type = "incr"

    if user_input_lower in ["exit", "quit"]:
        return {"task_id": "exit"}

    if "both clusters" in user_input_lower or ("pg1" in user_input_lower and "pg2" in user_input_lower):
        if action_type == "backup":
            return {"task_id": "backup_both", "inputs": {"backup_type": backup_type}}
        elif action_type == "list":
            return {"tasks": [{"task_id": "list_pg1"}, {"task_id": "list_pg2"}]}
        elif action_type == "restore":
            return {"task_id": "restore_both", "inputs": {"recent": "recent" in user_input_lower}}

    for cluster in ["pg1", "pg2"]:
        if cluster in user_input_lower:
            return {"task_id": f"{action_type}_{cluster}", "inputs": {"backup_type": backup_type}}

    return {"error": f"Cannot determine task from input: {user_input}"}


# ----------------------------
# Supervisor Agent (CrewAI)
# ----------------------------
supervisor_agent = Agent(
    name="Supervisor Agent",
    description="Supervises pgBackRest-based backup, restore, and consistency group operations across clusters.",
    role="supervisor",
    goal="Maintain consistent backups across pg1 and pg2 clusters.",
    backstory="This agent acts as a top-level coordinator between PostgreSQL clusters pg1 and pg2. It ensures consistency across clusters during backup and restore operations using pgBackRest and MCP servers."
)


def run_supervisor():
    print("🚀 Supervisor Agent started.")
    print("You can type:")
    print(' - "backup both clusters" (Consistency Group)')
    print(' - "backup pg1 incremental"')
    print(' - "restore pg2"')
    print(' - "list backups pg1"\n')

    from rich import print as rprint
    from rich.pretty import pprint as rpprint
    from rich.json import JSON

    while True:
        try:
            cmd = input("You: ").strip()
        except KeyboardInterrupt:
            print("\n👋 Exiting supervisor...")
            break

        if not cmd:
            continue

        parsed = parse_command_with_ollama(cmd)
        rprint("[bold cyan]📝 Parsed Command:[/bold cyan]")
        rprint(parsed)

        task_id = parsed.get("task_id")
        inputs = parsed.get("inputs", {})

        if not task_id:
            print("⚠️ Could not parse task.")
            continue

        if task_id == "exit":
            print("👋 Goodbye.")
            return

        results = run_task(task_id, inputs)

        rprint("[bold green]✅ Task Result:[/bold green]")
        try:
            rprint(JSON(json.dumps(results, indent=2)))
        except Exception:
            rpprint(results)


# ----------------------------
# Crew Setup
# ----------------------------
crew = Crew(
    agents=[supervisor_agent],
    tasks=[],
    process=Process.sequential
)

if __name__ == "__main__":
    run_supervisor()
