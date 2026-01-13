# orchestrator.py
import threading
import time
import re
import json
import requests

from mcp_server import MCPServerThread
from backup_restore import BackupRestore, CLUSTER_DATABASES

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 8000
MCP_NAME = "mcp_single"

# ============================================================
#  START THE SINGLE MCP SERVER
# ============================================================
def start_mcp_server():
    handler = BackupRestore(
        name="backup_restore",
        stanza="pg1_17",
        data_dir="/var/lib/postgresql/17/pg1_17",
        port=5433
    )
    server = MCPServerThread(
        host=SERVER_HOST,
        port=SERVER_PORT,
        handler=handler,
        name=MCP_NAME
    )
    server.start()
    return server


# ============================================================
#  SEND ACTIONS TO MCP SERVER
# ============================================================
def execute_via_mcp(actions: list):
    url = f"http://{SERVER_HOST}:{SERVER_PORT}/execute"
    try:
        resp = requests.post(url, json={"actions": actions}, timeout=60)
        return resp.json()
    except Exception as e:
        return {"error": f"Request failed: {e}"}


# ============================================================
#  NEW PURE KEYWORD-BASED COMMAND PARSER (FIXED)
# ============================================================
def _extract_backup_filename(text: str):
    """
    Try several patterns:
      - db1_YYYYMMDDHHMMSS.backup
      - anything_like_word_digits.backup
      - label format 20251204-123456F
    Returns first match or None.
    """
    # common file style: name_YYYYmmddHHMMSS.backup
    m = re.search(r"(\b\w+_\d{14}\.backup\b)", text, flags=re.IGNORECASE)
    if m:
        return m.group(1)

    # more general .backup filename (word + digits)
    m = re.search(r"(\b[\w\-\.]+\.backup\b)", text, flags=re.IGNORECASE)
    if m:
        return m.group(1)

    # pgBackRest label style: 20251204-123456F or ...I
    m = re.search(r"(\b\d{8}-\d{6}[A-Z]?\b)", text)
    if m:
        return m.group(1)

    return None


def parse_command(user_input: str):
    ui = user_input.strip()
    ui_l = ui.lower()

    # -------------------------
    # Detect ACTION with priority: restore > list > backup
    # -------------------------
    if "restore" in ui_l:
        action = "restore"
    elif "list" in ui_l:
        action = "list"
    elif re.search(r"\bbackup\b", ui_l):  # match whole word only
        action = "backup"
    else:
        return {"error": "Could not detect action (backup/restore/list)."}

    # -------------------------
    # Detect backup_type
    # -------------------------
    backup_type = "full"
    if re.search(r"\bincremental\b", ui_l) or re.search(r"\bincr\b", ui_l):
        backup_type = "incr"

    # -------------------------
    # Extract backup filename/label if present (file or label)
    # -------------------------
    backup_name = _extract_backup_filename(ui)

    # -------------------------
    # Find DB names and clusters
    # -------------------------
    # Build list of tasks (support multiple DB mentions)
    tasks = []
    db_list = []
    for cluster, dbs in CLUSTER_DATABASES.items():
        for db in dbs:
            if re.search(rf"\b{db}\b", ui_l):
                db_list.append((cluster, db))

    # If user explicitly supplied a db list (db-level commands)
    if db_list:
        for cluster, db in db_list:
            t = {
                "action": action,
                "cluster": cluster,
                "database": db,
                "backup_type": backup_type,
                "backup_name": None,
                "recent": False
            }

            # restore-specific handling: also accept `to`/`from` <file>
            if action == "restore":
                # try to find filename adjacent to 'to' or 'from'
                m = re.search(r'\b(?:to|from)\s+([^\s,;]+)', ui, flags=re.IGNORECASE)
                if m:
                    maybe = m.group(1)
                    # if it looks like a backup filename or label, use it
                    if maybe.lower().endswith(".backup") or re.match(r"^\d{8}-\d{6}[A-Z]?$", maybe):
                        t["backup_name"] = maybe
                    else:
                        # fallback to earlier extractor
                        t["backup_name"] = backup_name
                else:
                    t["backup_name"] = backup_name

                t["recent"] = ("recent" in ui_l)

            tasks.append(t)
        return tasks

    # -------------------------
    # Cluster-level detection (pg1, pg2, both)
    # -------------------------
    clusters = []
    if "both clusters" in ui_l:
        clusters = list(CLUSTER_DATABASES.keys())
    else:
        for cluster in CLUSTER_DATABASES.keys():
            if re.search(rf"\b{cluster}\b", ui_l):
                clusters.append(cluster)

    if not clusters:
        # If no explicit cluster or db, allow defaults in some cases:
        # e.g., "backup" alone -> error (must specify target)
        return {"error": "Could not determine target DB or cluster. Mention pg1 or db1."}

    for cluster in clusters:
        t = {
            "action": action,
            "cluster": cluster,
            "database": None,
            "backup_type": backup_type,
            "backup_name": None,
            "recent": False
        }

        if action == "restore":
            # prefer explicit 'to/from' extraction
            m = re.search(r'\b(?:to|from)\s+([^\s,;]+)', ui, flags=re.IGNORECASE)
            if m:
                maybe = m.group(1)
                if maybe.lower().endswith(".backup") or re.match(r"^\d{8}-\d{6}[A-Z]?$", maybe):
                    t["backup_name"] = maybe
                else:
                    t["backup_name"] = backup_name
            else:
                t["backup_name"] = backup_name

            t["recent"] = ("recent" in ui_l)

        tasks.append(t)

    return tasks


# ============================================================
#  MAIN ORCHESTRATOR LOOP
# ============================================================
def orchestrator_loop():
    print("🚀 NLP Orchestrator started.")
    print("You can type commands like:\n")
    print("  • backup pg1")
    print("  • backup db1 incremental")
    print("  • restore pg1 recent")
    print("  • restore db1 to db1_20251204123120.backup")
    print("  • list pg1")
    print("  • backup both clusters")
    print("  • exit\n")

    while True:
        try:
            cmd = input("You: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nExiting orchestrator.")
            break

        if not cmd:
            continue
        if cmd.lower() in ("exit", "quit"):
            print("👋 Bye.")
            break

        parsed = parse_command(cmd)

        print("\n🔎 Parsed tasks:")
        print(json.dumps(parsed, indent=2))
        print()

        if isinstance(parsed, dict) and parsed.get("error"):
            print("⚠️", parsed["error"])
            continue

        # ----------------------------------------------
        # Build actions list in MCP format
        # ----------------------------------------------
        actions = []
        for p in parsed:
            action = p.copy()
            action["user_input"] = cmd  # send raw natural text
            actions.append(action)

        print("➡️ Sending to MCP server...")
        res = execute_via_mcp(actions)

        print("\n✅ Result:")
        print(json.dumps(res, indent=2))
        print()


# ============================================================
#  MAIN
# ============================================================
if __name__ == "__main__":
    server = start_mcp_server()
    time.sleep(1)  # ensure MCP server is up
    orchestrator_loop()

    # optional shutdown
    try:
        server.shutdown()
    except:
        pass
