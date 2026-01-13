import os
import subprocess
from typing import List

PG_BACKREST_CONF = "/etc/pgbackrest/pgbackrest.conf"

def list_backups(repo: str = "main") -> List[str]:
    """
    List all available backups in pgBackRest for a given repo.
    """
    try:
        result = subprocess.run(
            ["pgbackrest", "info", f"--stanza={repo}", "--output=json"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        return f"Error listing backups: {e.stderr}"


def restore_backup(repo: str = "main", backup_label: str = None, target_path: str = "/var/lib/pgsql/data"):
    """
    Restore a specific backup using pgBackRest.
    """
    try:
        cmd = ["pgbackrest", "restore", f"--stanza={repo}", f"--delta", f"--db-path={target_path}"]
        if backup_label:
            cmd.append(f"--set={backup_label}")

        subprocess.run(cmd, check=True)
        return f"Backup {backup_label or 'latest'} restored successfully to {target_path}"
    except subprocess.CalledProcessError as e:
        return f"Error restoring backup: {e.stderr}"
