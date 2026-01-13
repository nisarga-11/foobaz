import subprocess
import os
import json
from glob import glob
from crewai.tools import tool

@tool("List PostgreSQL Backups Tool")
def list_backups_tool(stanza: str, pg_path: str) -> dict:
    """
    Lists all available backups for a PostgreSQL cluster.
    
    Args:
        stanza: The pgBackRest stanza name (e.g., 'pg1_17')
        pg_path: PostgreSQL data directory path (not used but kept for consistency)
    
    Returns:
        dict: Dictionary containing cluster_backups and db_backups lists
    """
    
    env = os.environ.copy()
    env["PGBACKREST_TMP_PATH"] = os.path.join(os.getcwd(), "pgbackrest", "tmp")
    env["PGBACKREST_LOG_PATH"] = os.path.join(os.getcwd(), "pgbackrest", "log")
    env["PGBACKREST_REPO1_PATH"] = os.path.join(os.getcwd(), "backups")
    
    cluster_backups = []
    db_backups = {}
    
    try:
        # Get cluster-level backups from pgBackRest
        info_output = subprocess.check_output([
            "sudo", "-u", "postgres", "pgbackrest",
            f"--stanza={stanza}",
            "info",
            "--output=json"
        ], stderr=subprocess.STDOUT, universal_newlines=True, env=env)
        
        info_json = json.loads(info_output)
        
        if isinstance(info_json, list):
            stanza_info = next((s for s in info_json if s.get("name") == stanza), None)
        elif isinstance(info_json, dict):
            stanza_info = info_json if info_json.get("name") == stanza else None
        else:
            stanza_info = None
        
        if stanza_info:
            backups_list = stanza_info.get("backup", [])
            for b in backups_list:
                cluster_backups.append({
                    "label": b.get("label"),
                    "type": b.get("type"),
                    "timestamp": b.get("timestamp", {}).get("start")
                })
    
    except subprocess.CalledProcessError as e:
        print(f"Warning: Failed to list cluster backups: {e.output.strip()}")
    
    # Get DB-level backups (pg_dump files)
    backup_dir = os.path.join(os.getcwd(), "backups", stanza)
    
    # Determine which databases belong to this cluster
    CLUSTER_DATABASES = {
        "pg1_17": ["db1", "db2"],
        "pg2_17": ["db3", "db4"]
    }
    
    databases = CLUSTER_DATABASES.get(stanza, [])
    
    for db in databases:
        files = sorted(glob(os.path.join(backup_dir, f"{db}_*.backup")))
        db_backups[db] = [{"label": os.path.basename(f)} for f in files]
    
    return {
        "stanza": stanza,
        "cluster_backups": cluster_backups,
        "db_backups": db_backups
    }