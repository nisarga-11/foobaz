import threading
import time
import requests
import yaml
import re
import json
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
# Load agents.yaml and task.yaml
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
            timeout=30
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
# Run task by task_id
# ----------------------------
def run_task(task_id, inputs=None):
    """
    Executes a task by task_id, handling:
    - Cluster-level full/incremental backups (applied to all DBs in the cluster)
    - DB-level backups
    - Restore and list actions
    """
    task_id = task_id.lower()  # normalize
    task = TASKS.get(task_id)
    results = {}

    # Dynamically detect task if not in YAML
    if not task:
        m = re.match(r"(backup|restore|list)_(pg\d+)(?:_(db\d+))?", task_id)
        if not m:
            return f"⚠️ Unknown task '{task_id}'"
        action_type, cluster, db = m.groups()

        # Determine agent based on cluster
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

        # Apply inputs overrides
        if inputs:
            payload_base.update(inputs)

        # ----------------------
        # Backup tasks
        # ----------------------
        if action.get("action") == "backup":
            if db:  # DB-level backup
                results.setdefault(agent_id, {})[db] = execute_agent(agent_id, [payload_base])
            elif cluster:  # Cluster-level backup
                for db_name in CLUSTER_DATABASES.get(cluster, []):
                    payload = payload_base.copy()
                    payload["database"] = db_name
                    results.setdefault(agent_id, {})[db_name] = execute_agent(agent_id, [payload])
            else:  # fallback
                results[agent_id] = execute_agent(agent_id, [payload_base])

        # ----------------------
        # Restore tasks
        # ----------------------
        elif action.get("action") == "restore":
            # DB-level restore
            if db:
                payload_base["database"] = db
                results.setdefault(agent_id, {})[db] = execute_agent(agent_id, [payload_base])
            # Cluster-level restore: restore each DB in the cluster
            elif cluster:
                for db_name in CLUSTER_DATABASES.get(cluster, []):
                    payload = payload_base.copy()
                    payload["database"] = db_name
                    results.setdefault(agent_id, {})[db_name] = execute_agent(agent_id, [payload])
            else:  # fallback
                results[agent_id] = execute_agent(agent_id, [payload_base])

        # ----------------------
        # List tasks
        # ----------------------
        elif action.get("action") == "list":
            results[agent_id] = execute_agent(agent_id, [payload_base])

        else:
            results[agent_id] = f"Unknown action {action.get('action')}"

    return results


# ----------------------------
# NLP Command Parser
# ----------------------------

def parse_command_with_ollama(user_input: str) -> dict:
    """
    Convert natural language command into tasks.
    Supports:
    - Cluster-level: pg1, pg2, both
    - DB-level: db1, db2, db3, db4 (multiple DBs in one command)
    - Full / incremental backups
    - List / restore commands
    - Restore latest backup via keyword 'recent'
    """
    user_input_lower = user_input.lower()
    tasks = []

    # Determine action type
    action_type = "backup"
    if "list" in user_input_lower:
        action_type = "list"
    elif "restore" in user_input_lower:
        action_type = "restore"

    # Determine backup type
    backup_type = "full"
    if "incremental" in user_input_lower or "incr" in user_input_lower:
        backup_type = "incr"

    # Check for exit
    if user_input_lower in ["exit", "quit"]:
        return {"task_id": "exit"}

    # ----------------
    # Cluster-level commands (both clusters)
    # ----------------
    if "both clusters" in user_input_lower or ("pg1" in user_input_lower and "pg2" in user_input_lower):
        if action_type == "backup":
            tasks.append({"task_id": "backup_both", "inputs": {"backup_type": backup_type}})
        elif action_type == "list":
            tasks.append({"task_id": "list_pg1", "inputs": {}})
            tasks.append({"task_id": "list_pg2", "inputs": {}})
        elif action_type == "restore":
            recent = "recent" in user_input_lower
            tasks.append({
                "task_id": "restore_both",
                "inputs": {"backup_name": None, "recent": recent}
            })
        return {"tasks": tasks}

    # ----------------
    # Cluster-level single commands
    # ----------------
    for cluster in ["pg1", "pg2"]:
        if cluster in user_input_lower:
            if action_type == "backup":
                tasks.append({"task_id": f"backup_{cluster}", "inputs": {"backup_type": backup_type}})
            elif action_type == "list":
                tasks.append({"task_id": f"list_{cluster}", "inputs": {}})
            elif action_type == "restore":
                backup_name = None
                recent = "recent" in user_input_lower

                if not recent:
                    match = re.search(r'\b(\d{8}-\d{6}[A-Z]?)\b', user_input)
                    if match:
                        backup_name = match.group(1)

                tasks.append({
                    "task_id": f"restore_{cluster}",
                    "inputs": {"backup_name": backup_name, "recent": recent}
                })
            return {"tasks": tasks}

    # ----------------
    # DB-level commands (multiple DB detection across clusters)
    # ----------------
    dbs_found = []
    for cluster, dbs in CLUSTER_DATABASES.items():
        for db in dbs:
            if re.search(rf'\b{db}\b', user_input_lower):
                dbs_found.append((cluster, db))

    for cluster, db in dbs_found:
        inputs = {"backup_type": backup_type} if action_type == "backup" else {}
        if action_type == "restore":
            backup_name = None
            recent = "recent" in user_input_lower
            if not recent:
                match = re.search(r'\b(\d{8}-\d{6}[A-Z]?)\b', user_input)
                if match:
                    backup_name = match.group(1)
            inputs.update({"backup_name": backup_name, "recent": recent, "db_name": db})
        else:
            inputs["db_name"] = db

        task_id = f"{action_type}_{cluster}_{db}" if action_type == "backup" else f"{action_type}_{cluster}_{db}"
        tasks.append({"task_id": task_id.lower(), "inputs": inputs})

    if not tasks:
        return {"actions": [], "error": f"Cannot determine task from input: {user_input}"}

    return {"tasks": tasks} if len(tasks) > 1 else tasks[0]


# ----------------------------
# Orchestrator Loop
# ----------------------------
def orchestrator():
    print("🚀 NLP Orchestrator started.")
    print("You can type commands in plain English, e.g.:")
    print(' - "backup both clusters"')
    print(' - "backup db1 incremental"')
    print(' - "restore pg2 from backup 20251001-123456F"')
    print(' - "list backups for PG1"')
    print(' - "exit"\n')

    from rich import print as rprint
    from rich.pretty import pprint as rpprint
    from rich.json import JSON

    while True:
        try:
            cmd = input("You: ").strip()
        except KeyboardInterrupt:
            print("\nExiting orchestrator...")
            break

        if not cmd:
            continue

        parsed = parse_command_with_ollama(cmd)

        # Pretty-print the parsed JSON
        rprint("[bold cyan]📝 Parsed JSON from Ollama:[/bold cyan]")
        try:
            rprint(JSON(json.dumps(parsed, indent=2)))
        except Exception:
            rpprint(parsed)

        # Support multiple tasks
        tasks_to_run = parsed.get("tasks") or [parsed]

        for task in tasks_to_run:
            task_id = task.get("task_id")
            inputs = task.get("inputs", {})

            if task_id == "exit":
                print("👋 Exiting orchestrator...")
                return

            if not task_id:
                print(f"⚠️ Could not determine task: {task.get('error', 'Unknown')}")
                continue

            results = run_task(task_id, inputs)

            # Pretty-print results
            rprint("[bold green]✅ Task Result:[/bold green]")
            try:
                rprint(JSON(json.dumps(results, indent=2)))
            except Exception:
                rpprint(results)
            print()

# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    orchestrator()