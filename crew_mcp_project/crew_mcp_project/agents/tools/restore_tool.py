
import subprocess
import os
import shutil
import time
from glob import glob
from crewai.tools import tool

@tool("PostgreSQL Restore Tool")
def restore_tool(stanza: str, pg_path: str, backup_name: str = None, db_name: str = None, recent: bool = False) -> str:
    """
    Restores a PostgreSQL backup using pgBackRest or pg_restore.
    
    Args:
        stanza: The pgBackRest stanza name (e.g., 'pg1_17')
        pg_path: PostgreSQL data directory path
        backup_name: Specific backup label or file name to restore
        db_name: If provided, performs DB-level restore using pg_restore
        recent: If True, restores the most recent backup
    
    Returns:
        str: Restore result message
    """
    
    env = os.environ.copy()
    env["PGBACKREST_TMP_PATH"] = os.path.join(os.getcwd(), "pgbackrest", "tmp")
    env["PGBACKREST_LOG_PATH"] = os.path.join(os.getcwd(), "pgbackrest", "log")
    env["PGBACKREST_REPO1_PATH"] = os.path.join(os.getcwd(), "backups")
    
    port = "5433" if "pg1" in stanza else "5434"
    
    try:
        if db_name:
            # DB-level restore using pg_restore
            backup_dir = os.path.join(os.getcwd(), "backups", stanza)
            
            if recent:
                files = sorted(glob(os.path.join(backup_dir, f"{db_name}_*.backup")))
                if not files:
                    return f"❌ No DB-level backups found for {db_name}"
                backup_file = files[-1]
            elif backup_name:
                backup_file = os.path.join(backup_dir, backup_name)
                if not os.path.exists(backup_file):
                    return f"❌ DB backup '{backup_name}' not found"
            else:
                return f"❌ Must provide backup_name or set recent=True for DB restore"
            
            subprocess.check_output([
                "sudo", "-u", "postgres", "pg_restore",
                "-p", port,
                "-d", db_name,
                "--clean", "--if-exists",
                backup_file
            ], stderr=subprocess.STDOUT, universal_newlines=True)
            
            return f"✅ DB-level restore completed for {db_name} from {os.path.basename(backup_file)}"
            
        else:
            # Cluster-level restore using pgBackRest
            
            # Get available backups
            import json
            info_output = subprocess.check_output([
                "sudo", "-u", "postgres", "pgbackrest",
                f"--stanza={stanza}",
                "info", "--output=json"
            ], stderr=subprocess.STDOUT, universal_newlines=True, env=env)
            
            info_json = json.loads(info_output)
            
            if isinstance(info_json, list):
                stanza_info = next((s for s in info_json if s.get("name") == stanza), None)
            else:
                stanza_info = info_json if info_json.get("name") == stanza else None
            
            if not stanza_info:
                return f"❌ No backups found for stanza {stanza}"
            
            backups_list = stanza_info.get("backup", [])
            if not backups_list:
                return f"❌ No cluster backups available"
            
            if recent:
                target_backup = backups_list[-1]["label"]
            elif backup_name:
                matching = [b for b in backups_list if b["label"] == backup_name]
                if not matching:
                    return f"❌ Backup '{backup_name}' not found"
                target_backup = matching[0]["label"]
            else:
                return f"❌ Must provide backup_name or set recent=True"
            
            # Stop PostgreSQL
            subprocess.check_output([
                "sudo", "-u", "postgres",
                "/usr/lib/postgresql/17/bin/pg_ctl", "-D", pg_path,
                "stop", "-m", "fast", "-w"
            ], stderr=subprocess.STDOUT, universal_newlines=True)
            
            # Clean data directory
            for item in os.listdir(pg_path):
                item_path = os.path.join(pg_path, item)
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.remove(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
            
            # Restore
            cmd = [
                "sudo", "-u", "postgres", "pgbackrest",
                f"--stanza={stanza}",
                f"--pg1-path={pg_path}",
                "restore",
                "--type=immediate",
                "--delta",
                f"--set={target_backup}"
            ]
            
            subprocess.check_output(cmd, stderr=subprocess.STDOUT, universal_newlines=True, env=env)
            
            # Start PostgreSQL
            log_dir = os.path.join(pg_path, "log")
            subprocess.run(["sudo", "-u", "postgres", "mkdir", "-p", log_dir], check=True)
            log_file = os.path.join(log_dir, "server.log")
            
            subprocess.check_output([
                "sudo", "-u", "postgres",
                "/usr/lib/postgresql/17/bin/pg_ctl",
                "-D", pg_path,
                "start",
                "-l", log_file
            ], stderr=subprocess.STDOUT, universal_newlines=True)
            
            time.sleep(3)
            
            # Promote if needed
            output = subprocess.check_output([
                "sudo", "-u", "postgres", "/usr/lib/postgresql/17/bin/psql",
                "-p", port, "-d", "postgres",
                "-t", "-c", "SELECT pg_is_in_recovery();"
            ], universal_newlines=True)
            
            if output.strip() == "t":
                subprocess.check_output([
                    "sudo", "-u", "postgres", "/usr/lib/postgresql/17/bin/pg_ctl",
                    "-D", pg_path, "promote"
                ], universal_newlines=True)
                time.sleep(2)
            
            return f"✅ Cluster-level restore completed for {stanza} from backup {target_backup}"
            
    except subprocess.CalledProcessError as e:
        return f"❌ Restore failed for {stanza}: {e.output.strip()}"
    except Exception as e:
        return f"❌ Restore error: {str(e)}"