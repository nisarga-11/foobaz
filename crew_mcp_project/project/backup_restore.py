#!/usr/bin/env python3
"""
backup_restore.py

Single BackupRestore handler for orchestrator + MCP server.

Features:
- Uses /usr/pgsql-17/bin (RHEL/CentOS PGDG) binaries.
- Auto-detects and attempts to recover from stale postmaster.pid.
- Starts Postgres automatically (sudo -u postgres) when needed.
- DB-level backups via pg_dump/pg_restore (custom format).
- Cluster-level backups/restores via pgBackRest.
- list_backups() returns cluster_backups and db_backups (from files).
"""

import os
import subprocess
import time
import shutil
import json
import re
from glob import glob
from pathlib import Path

# -------------------------
# Configuration - adjust if you want different defaults
# -------------------------
PG_BIN = "/usr/pgsql-17/bin"      # Detected working PG binaries on your system
PG_CTL = os.path.join(PG_BIN, "pg_ctl")
PSQL = os.path.join(PG_BIN, "psql")
PG_DUMP = os.path.join(PG_BIN, "pg_dump")
PG_RESTORE = os.path.join(PG_BIN, "pg_restore")

# Default data directory and port (change if needed)
DEFAULT_DATA_DIR = "/var/lib/postgresql/17/pg1_17"
DEFAULT_PORT = 5433
DEFAULT_STANZA = "pg1_17"

# Cluster mapping - keep as you had it
CLUSTER_DATABASES = {
    "pg1": ["db1", "db2"],
    "pg2": ["db3", "db4"]
}

# -------------------------
# Helper utilities
# -------------------------
def _sudo_postgres(cmd_list, env=None, timeout=300):
    """
    Run command as postgres user with sudo -u postgres ...
    Returns (success, output or exception string)
    """
    full_cmd = ["sudo", "-u", "postgres"] + cmd_list
    try:
        out = subprocess.check_output(full_cmd, stderr=subprocess.STDOUT, universal_newlines=True, env=env, timeout=timeout)
        return True, out
    except subprocess.CalledProcessError as e:
        return False, getattr(e, "output", str(e))
    except Exception as e:
        return False, str(e)

def _is_postgres_running(port):
    """
    Check if Postgres accepts connections on given port.
    Returns True if usable, False otherwise.
    """
    try:
        cmd = [PSQL, "-p", str(port), "-d", "postgres", "-t", "-c", "select 1"]
        success, out = _sudo_postgres(cmd, timeout=10)
        if success:
            return out is not None and out.strip() != ""
        return False
    except Exception:
        return False

def _remove_stale_pid(data_dir):
    """
    If postmaster.pid exists but Postgres is not running, remove it safely.
    Returns a message describing action taken.
    """
    pid_path = Path(data_dir) / "postmaster.pid"
    if not pid_path.exists():
        return f"No postmaster.pid at {pid_path}"
    # If postgres responds on socket/port then don't remove
    # We assume port set in postgresql.conf; here we check DEFAULT_PORT first.
    if _is_postgres_running(DEFAULT_PORT):
        return f"postmaster.pid exists but Postgres is running on port {DEFAULT_PORT}; not removing PID file."
    try:
        # optionally back up the pid file
        backup_path = Path(data_dir) / f"postmaster.pid.stale.{int(time.time())}"
        pid_path.replace(backup_path)  # move (atomic) to backup location
        return f"Moved stale postmaster.pid to {backup_path}"
    except Exception as e:
        try:
            pid_path.unlink()
            return f"Removed stale postmaster.pid: {e}"
        except Exception as e2:
            return f"Failed to remove stale postmaster.pid: {e2}"

def _ensure_dir_owned_by_postgres(path):
    """
    Ensure directory exists and is owned by postgres:postgres (attempt via sudo).
    """
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass
    try:
        subprocess.run(["sudo", "chown", "-R", "postgres:postgres", path], check=True)
    except Exception:
        # If chown fails, that's okay; operations may still work if sudo is available for commands.
        pass

# -------------------------
# BackupRestore class
# -------------------------
class BackupRestore:
    def __init__(self, name="backup_restore",
                 stanza=DEFAULT_STANZA,
                 data_dir=DEFAULT_DATA_DIR,
                 port=DEFAULT_PORT):
        self.name = name
        self.stanza = stanza
        self.data_dir = data_dir
        self.port = port
        self.backup_dir = os.path.join(os.getcwd(), "backups", self.name)
        os.makedirs(self.backup_dir, exist_ok=True)
        _ensure_dir_owned_by_postgres(self.backup_dir)

        # pgBackRest environment settings
        self.env = os.environ.copy()
        self.env["PGBACKREST_TMP_PATH"] = os.path.join(os.getcwd(), "pgbackrest", "tmp")
        self.env["PGBACKREST_LOG_PATH"] = os.path.join(os.getcwd(), "pgbackrest", "log")
        self.env["PGBACKREST_REPO1_PATH"] = os.path.join(os.getcwd(), "backups")
        os.makedirs(self.env["PGBACKREST_TMP_PATH"], exist_ok=True)
        os.makedirs(self.env["PGBACKREST_LOG_PATH"], exist_ok=True)
        os.makedirs(self.env["PGBACKREST_REPO1_PATH"], exist_ok=True)
        _ensure_dir_owned_by_postgres(self.env["PGBACKREST_TMP_PATH"])
        _ensure_dir_owned_by_postgres(self.env["PGBACKREST_LOG_PATH"])
        _ensure_dir_owned_by_postgres(self.env["PGBACKREST_REPO1_PATH"])

    # -------------------------
    # Postgres lifecycle helpers
    # -------------------------
    def start_postgres(self):
        """
        Start PostgreSQL using pg_ctl, trying to fix stale pid first.
        """
        # If already running — nothing to do
        if _is_postgres_running(self.port):
            return f"[{self.name}] PostgreSQL already running on port {self.port}."

        # Attempt to remove stale pid if present
        stale_msg = _remove_stale_pid(self.data_dir)
        # Try to start
        cmd = [PG_CTL, "-D", self.data_dir, "start", "-l", os.path.join(self.data_dir, "log", "server.log")]
        success, out = _sudo_postgres(cmd)
        if success:
            return f"[{self.name}] PostgreSQL started. ({stale_msg})"
        else:
            return f"[{self.name}] PostgreSQL failed to start: {out}. ({stale_msg})"

    def stop_postgres(self):
        """
        Stop PostgreSQL using pg_ctl. Returns status message.
        """
        # If not running, pg_ctl stop may report no server running
        cmd = [PG_CTL, "-D", self.data_dir, "stop", "-m", "fast", "-w"]
        success, out = _sudo_postgres(cmd)
        if success:
            return f"[{self.name}] PostgreSQL stopped."
        # If not running, that's fine - return message
        if "no server running" in out.lower():
            return f"[{self.name}] PostgreSQL not running."
        return f"[{self.name}] PostgreSQL stop failed: {out}"

    def _clean_data_dir(self):
        """Remove all files from data_dir to prepare for restore (keeps data_dir itself)."""
        try:
            if os.path.exists(self.data_dir):
                for item in os.listdir(self.data_dir):
                    item_path = os.path.join(self.data_dir, item)
                    # Be careful not to remove directory itself: only contents
                    if os.path.islink(item_path) or os.path.isfile(item_path):
                        os.remove(item_path)
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                # restore ownership
                subprocess.run(["sudo", "chown", "-R", "postgres:postgres", self.data_dir], check=False)
            return f"[{self.name}] Data directory cleaned."
        except Exception as e:
            return f"[{self.name}] Failed to clean data directory: {e}"

    def _promote_if_needed(self):
        """
        If cluster is in recovery (standby), promote it to primary.
        Returns status message.
        """
        try:
            cmd = [PSQL, "-p", str(self.port), "-d", "postgres", "-t", "-c", "SELECT pg_is_in_recovery();"]
            success, out = _sudo_postgres(cmd)
            if not success:
                return f"[{self.name}] Could not check recovery status: {out}"
            if out.strip() == "t":
                cmd_promote = [PG_CTL, "-D", self.data_dir, "promote"]
                success2, out2 = _sudo_postgres(cmd_promote)
                if success2:
                    time.sleep(2)
                    return f"[{self.name}] Promoted standby to primary."
                else:
                    return f"[{self.name}] Failed to promote standby: {out2}"
            return f"[{self.name}] Cluster already primary."
        except Exception as e:
            return f"[{self.name}] Promotion check failed: {e}"

    # -------------------------
    # Backup / Restore operations
    # -------------------------
    def perform_backup(self, backup_type="full", db_name=None):
        """
        - db_name provided => DB-level pg_dump (custom format)
        - otherwise cluster-level pgBackRest backup (full/incr)
        """
        if db_name:
            # DB-level backup via pg_dump
            ts = time.strftime("%Y%m%d%H%M%S")
            dump_file = os.path.join(self.backup_dir, f"{db_name}_{ts}.backup")
            try:
                cmd = [PG_DUMP, "-p", str(self.port), "-F", "c", "-f", dump_file, db_name]
                success, out = _sudo_postgres(cmd)
                if success:
                    return f"[{self.name}] DB-level backup completed: {dump_file}"
                else:
                    return f"[{self.name}] DB-level backup failed: {out}"
            except Exception as e:
                return f"[{self.name}] DB-level backup exception: {e}"

        # Cluster-level backup using pgBackRest
        # Ensure postgres is running and primary
        start_msg = self.start_postgres()
        promote_msg = self._promote_if_needed()
        if "failed" in start_msg.lower() and not _is_postgres_running(self.port):
            return f"[{self.name}] Cannot run cluster backup because Postgres start failed: {start_msg}"

        cmd = [
            "sudo", "-u", "postgres", "pgbackrest",
            f"--stanza={self.stanza}",
            f"--pg1-path={self.data_dir}",
            "backup",
            f"--type={backup_type}"
        ]
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, universal_newlines=True, env=self.env, timeout=600)
            # Run info to find latest label
            info_cmd = [
                "sudo", "-u", "postgres", "pgbackrest",
                f"--stanza={self.stanza}", "info"
            ]
            info_out = subprocess.check_output(info_cmd, stderr=subprocess.STDOUT, universal_newlines=True, env=self.env, timeout=30)
            labels = re.findall(r'(\d{8}-\d{6}[FI])', info_out)
            latest = labels[-1] if labels else None
            return f"[{self.name}] Cluster backup completed. latest_label={latest}. StartMsg={start_msg}. PromoteMsg={promote_msg}"
        except subprocess.CalledProcessError as e:
            return f"[{self.name}] Cluster backup failed: {getattr(e,'output',str(e))}"
        except Exception as e:
            return f"[{self.name}] Cluster backup exception: {e}"

    def perform_restore(self, backup_name=None, db_name=None, recent=False):
        """
        - db_name provided => DB-level restore via pg_restore
        - otherwise cluster-level restore via pgBackRest (set backup label)
        """
        # DB-level restore
        if db_name:
            # Ensure cluster primary
            promote_res = self._promote_if_needed()
            if recent:
                files = sorted(glob(os.path.join(self.backup_dir, f"{db_name}_*.backup")))
                if not files:
                    return f"[{self.name}] No DB-level backups found for {db_name}"
                backup_file = files[-1]
            elif backup_name:
                backup_file = os.path.join(self.backup_dir, backup_name)
                if not os.path.exists(backup_file):
                    return f"[{self.name}] DB backup '{backup_name}' not found"
            else:
                return f"[{self.name}] Provide backup_name or recent=True for DB restore"

            try:
                cmd = [PG_RESTORE, "-p", str(self.port), "-d", db_name, "--clean", "--if-exists", "--verbose", backup_file]
                success, out = _sudo_postgres(cmd, timeout=600)
                if success:
                    return f"[{self.name}] DB-level restore completed: {backup_file}. {promote_res}"
                else:
                    return f"[{self.name}] DB-level restore failed: {out}"
            except Exception as e:
                return f"[{self.name}] DB-level restore exception: {e}"

        # Cluster-level restore
        info = self.list_backups()
        cluster_backups = info.get("cluster_backups", [])
        if not cluster_backups:
            return f"[{self.name}] No cluster backups found."

        if recent:
            target = cluster_backups[-1]["label"]
        elif backup_name:
            matches = [b for b in cluster_backups if b["label"] == backup_name]
            if not matches:
                return f"[{self.name}] Backup '{backup_name}' not found"
            target = matches[0]["label"]
        else:
            return f"[{self.name}] Provide backup_name or recent=True for cluster restore"

        # Stop postgres, clean data_dir, run pgbackrest restore, start & promote
        stop_msg = self.stop_postgres()
        clean_msg = self._clean_data_dir()

        cmd = [
            "sudo", "-u", "postgres", "pgbackrest",
            f"--stanza={self.stanza}",
            f"--pg1-path={self.data_dir}",
            "restore",
            "--type=immediate",
            "--delta",
            f"--set={target}"
        ]
        try:
            subprocess.check_output(cmd, stderr=subprocess.STDOUT, universal_newlines=True, env=self.env, timeout=600)
            start_msg = self.start_postgres()
            promote_msg = self._promote_if_needed()
            return f"[{self.name}] Cluster restore completed: {target}. StopMsg={stop_msg}. CleanMsg={clean_msg}. StartMsg={start_msg}. PromoteMsg={promote_msg}"
        except subprocess.CalledProcessError as e:
            # Attempt to start postgres so cluster isn't left down
            self.start_postgres()
            return f"[{self.name}] Cluster restore failed: {getattr(e,'output',str(e))}"
        except Exception as e:
            self.start_postgres()
            return f"[{self.name}] Cluster restore exception: {e}"

    # -------------------------
    # List backups (pgBackRest + pg_dump files)
    # -------------------------
    def list_backups(self):
        cluster_backups = []
        try:
            info_out = subprocess.check_output([
                "sudo", "-u", "postgres", "pgbackrest",
                f"--stanza={self.stanza}", "info", "--output=json"
            ], stderr=subprocess.STDOUT, universal_newlines=True, env=self.env, timeout=30)
            info_json = json.loads(info_out)
            # info_json may be a list or dict; find stanza
            stanza_info = None
            if isinstance(info_json, list):
                stanza_info = next((s for s in info_json if s.get("name") == self.stanza), None)
            elif isinstance(info_json, dict):
                if info_json.get("name") == self.stanza:
                    stanza_info = info_json
            if stanza_info:
                backups = stanza_info.get("backup", [])
                for b in backups:
                    cluster_backups.append({"label": b.get("label")})
        except subprocess.CalledProcessError:
            cluster_backups = []
        except Exception:
            cluster_backups = []

        # DB-level backups from backup_dir (pg_dump)
        db_backups = {}
        for cluster, dbs in CLUSTER_DATABASES.items():
            for db in dbs:
                files = sorted(glob(os.path.join(self.backup_dir, f"{db}_*.backup")))
                db_backups[db] = [{"label": os.path.basename(f)} for f in files]

        return {"cluster_backups": cluster_backups, "db_backups": db_backups}

    # -------------------------
    # Execute API (called by MCP server)
    # -------------------------
    def execute(self, request):
        """
        Input format:
          {"actions": [{"action":"backup", "cluster":"pg1", "database":"db1", "backup_type":"full", ...}, ...]}
        Returns:
          {"handler": self.name, "result": {"task_1": "...", ...}}
        """
        actions = request.get("actions", [])
        results = {}

        for i, act in enumerate(actions, start=1):
            action = (act.get("action") or "").lower()
            user_input = act.get("user_input") or act.get("inputs", {}).get("message") or ""
            db_name = act.get("database") or act.get("db_name")
            backup_name = act.get("backup_name")
            recent_flag = act.get("recent", False) or bool(re.search(r'\brecent\b', user_input, re.IGNORECASE))
            backup_type = act.get("backup_type", "full")

            # Try to extract db_name from natural user_input if missing
            if not db_name:
                m = re.search(r'\bdatabase\s+(\w+)\b', user_input, re.IGNORECASE)
                if m:
                    db_name = m.group(1)
                else:
                    for cluster, dbs in CLUSTER_DATABASES.items():
                        for d in dbs:
                            if re.search(rf'\b{d}\b', user_input, re.IGNORECASE):
                                db_name = d
                                break
                        if db_name:
                            break

            try:
                if action == "backup":
                    results[f"task_{i}"] = self.perform_backup(backup_type=backup_type, db_name=db_name)
                elif action == "restore":
                    results[f"task_{i}"] = self.perform_restore(backup_name=backup_name, db_name=db_name, recent=recent_flag)
                elif action == "list":
                    results[f"task_{i}"] = self.list_backups()
                else:
                    results[f"task_{i}"] = f"Unknown action: {action}"
            except Exception as e:
                results[f"task_{i}"] = f"Exception during {action}: {e}"

        return {"handler": self.name, "result": results}

# -------------------------
# If run stand-alone for quick test
# -------------------------
if __name__ == "__main__":
    br = BackupRestore()
    print("Sample list_backups():")
    print(json.dumps(br.list_backups(), indent=2))
