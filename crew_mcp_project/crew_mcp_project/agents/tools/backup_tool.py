import subprocess
import os
import time
from crewai.tools import tool

@tool("PostgreSQL Backup Tool")
def backup_tool(stanza: str, pg_path: str, backup_type: str = "full", db_name: str = None) -> str:
    """
    Performs a backup using pgBackRest or pg_dump.
    
    Args:
        stanza: The pgBackRest stanza name (e.g., 'pg1_17')
        pg_path: PostgreSQL data directory path
        backup_type: Type of backup - 'full' or 'incr' (incremental)
        db_name: If provided, performs DB-level backup using pg_dump, otherwise cluster-level
    
    Returns:
        str: Backup result message with backup label or file path
    """
    
    # Environment paths for pgBackRest
    env = os.environ.copy()
    env["PGBACKREST_TMP_PATH"] = os.path.join(os.getcwd(), "pgbackrest", "tmp")
    env["PGBACKREST_LOG_PATH"] = os.path.join(os.getcwd(), "pgbackrest", "log")
    env["PGBACKREST_REPO1_PATH"] = os.path.join(os.getcwd(), "backups")
    
    os.makedirs(env["PGBACKREST_TMP_PATH"], exist_ok=True)
    os.makedirs(env["PGBACKREST_LOG_PATH"], exist_ok=True)
    
    try:
        if db_name:
            # DB-level backup using pg_dump
            backup_dir = os.path.join(os.getcwd(), "backups", stanza)
            os.makedirs(backup_dir, exist_ok=True)
            
            timestamp = time.strftime("%Y%m%d%H%M%S")
            dump_file = os.path.join(backup_dir, f"{db_name}_{timestamp}.backup")
            
            port = "5433" if "pg1" in stanza else "5434"
            
            subprocess.check_output([
                "sudo", "-u", "postgres", "pg_dump",
                "-p", port,
                "-F", "c",
                "-f", dump_file,
                db_name
            ], stderr=subprocess.STDOUT, universal_newlines=True)
            
            return f"✅ DB-level backup completed for {db_name}: {dump_file}"
            
        else:
            # Cluster-level backup using pgBackRest
            cmd = [
                "sudo", "-u", "postgres", "pgbackrest",
                f"--stanza={stanza}",
                f"--pg1-path={pg_path}",
                "backup",
                f"--type={backup_type}"
            ]
            
            output = subprocess.check_output(
                cmd, 
                stderr=subprocess.STDOUT, 
                universal_newlines=True, 
                env=env
            )
            
            # Get latest backup label
            info_output = subprocess.check_output([
                "sudo", "-u", "postgres", "pgbackrest",
                f"--stanza={stanza}", "info"
            ], stderr=subprocess.STDOUT, universal_newlines=True, env=env)
            
            import re
            backup_labels = re.findall(r'(\d{8}-\d{6}[FI])', info_output)
            latest_label = backup_labels[-1] if backup_labels else "unknown"
            
            return f"✅ Cluster-level {backup_type} backup completed for {stanza}: {latest_label}"
            
    except subprocess.CalledProcessError as e:
        return f"❌ Backup failed for {stanza}: {e.output.strip()}"
    except Exception as e:
        return f"❌ Backup error: {str(e)}"