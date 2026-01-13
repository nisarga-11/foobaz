import os

# ---- CrewAI LLM Disable Patch ----
# Prevent CrewAI from auto-loading OpenAI provider when llm=None
os.environ["CREWAI_DISABLE_LLM"] = "true"
os.environ["OPENAI_API_KEY"] = "DUMMY_KEY"  # Fake key to stop fallback path
os.environ["MODEL"] = "none"                # Ensure provider check short-circuits
os.environ["OPENAI_MODEL_NAME"] = "none"    # CrewAI uses this to pick default
# -----------------------------------
import subprocess
import os
import re
import shutil
import time
from glob import glob
import json
from crewai import Agent

from crewai import Agent

agent = Agent(
    name="PG1 Backup Agent",
    description="Handles backup and restore for PostgreSQL Server 1",
    instructions="Use pgBackRest or appropriate scripts to perform backups and restores.",
    role="backup_agent",  # a short role name
    goal="Perform backup and restore of PG1 database",  # primary objective
    backstory="Responsible for maintaining PG1 backups safely",  # optional context
    llm=None  # 🚀 disable automatic LLM loading (fixes OPENAI_API_KEY error)
)



# ----------------------------
# Cluster to DB mapping
# ----------------------------
CLUSTER_DATABASES = {
    "pg1": ["db1", "db2"],
    "pg2": ["db3", "db4"]
}

class BackupRestoreAgent1:
    def __init__(self):
        self.name = "agent1"
        self.stanza = "pg1_17"
        self.data_dir = "/var/lib/postgresql/17/pg1_17"  # PG1 data directory
        self.port = 5433
        self.backup_dir = os.path.join(os.getcwd(), "backups", self.name)
        os.makedirs(self.backup_dir, exist_ok=True)

        # Environment paths for pgBackRest
        self.env = os.environ.copy()
        self.env["PGBACKREST_TMP_PATH"] = os.path.join(os.getcwd(), "pgbackrest", "tmp")
        self.env["PGBACKREST_LOG_PATH"] = os.path.join(os.getcwd(), "pgbackrest", "log")
        self.env["PGBACKREST_REPO1_PATH"] = os.path.join(os.getcwd(), "backups")  
        os.makedirs(self.env["PGBACKREST_TMP_PATH"], exist_ok=True)
        os.makedirs(self.env["PGBACKREST_LOG_PATH"], exist_ok=True)

    # ----------------------------
    # Core DB operations
    # ----------------------------
    def stop_postgres(self):
        try:
            subprocess.check_output([
                "sudo", "-u", "postgres",
                "/usr/lib/postgresql/17/bin/pg_ctl", "-D", self.data_dir,
                "stop", "-m", "fast", "-w"
            ], stderr=subprocess.STDOUT, universal_newlines=True)
            return f"[{self.name}] PostgreSQL stopped successfully."
        except subprocess.CalledProcessError as e:
            if "no server running" in e.output or "PID file" in e.output:
                return f"[{self.name}] PostgreSQL was already stopped."
            else:
                return f"[{self.name}] PostgreSQL stop failed: {e.output.strip()}"

    def start_postgres(self):
        try:
            log_dir = os.path.join(self.data_dir, "log")

            # Ensure log directory exists with correct ownership
            subprocess.run(["sudo", "-u", "postgres", "mkdir", "-p", log_dir], check=True)
            subprocess.run(["sudo", "chown", "-R", "postgres:postgres", log_dir], check=True)

            log_file = os.path.join(log_dir, "server.log")

            # Start PostgreSQL as postgres user
            subprocess.check_output([
                "sudo", "-u", "postgres",
                "/usr/lib/postgresql/17/bin/pg_ctl",
                "-D", self.data_dir,
                "start",
                "-l", log_file
            ], stderr=subprocess.STDOUT, universal_newlines=True)

            return f"[{self.name}] PostgreSQL started successfully."

        except subprocess.CalledProcessError as e:
            return f"[{self.name}] PostgreSQL start failed: {e.output.strip()}"
    
    def _clean_data_dir(self):
        """Remove all files from data_dir to prepare for restore."""
        try:
            if os.path.exists(self.data_dir):
                # Remove all contents but keep the parent data_dir
                for item in os.listdir(self.data_dir):
                    item_path = os.path.join(self.data_dir, item)
                    if os.path.isfile(item_path) or os.path.islink(item_path):
                        os.remove(item_path)
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                subprocess.run(["sudo", "chown", "-R", "postgres:postgres", self.data_dir], check=True)
            return f"[{self.name}] Data directory cleaned."
        except Exception as e:
            return f"[{self.name}] Failed to clean data directory: {str(e)}"


    def _promote_if_needed(self):
        try:
            output = subprocess.check_output([
                "sudo", "-u", "postgres", "/usr/lib/postgresql/17/bin/psql",
                "-p", str(self.port), "-d", "postgres",
                "-t", "-c", "SELECT pg_is_in_recovery();"
            ], universal_newlines=True)
            if output.strip() == "t":
                subprocess.check_output([
                    "sudo", "-u", "postgres", "/usr/lib/postgresql/17/bin/pg_ctl",
                    "-D", self.data_dir, "promote"
                ], universal_newlines=True)
                time.sleep(5)
                return f"[{self.name}] Cluster was standby, promoted to primary."
            return f"[{self.name}] Cluster already primary."
        except subprocess.CalledProcessError as e:
            return f"[{self.name}] Could not check/promote cluster: {e.output.strip()}"

    def make_db_change(self):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS test_table (
            id SERIAL PRIMARY KEY,
            name TEXT
        );
        """
        insert_sql = "INSERT INTO test_table (name) VALUES ('Incremental Change');"
        try:
            subprocess.run([
                "sudo", "-u", "postgres", "psql",
                "-p", str(self.port),
                "-d", "postgres",
                "-c", create_table_sql
            ], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            subprocess.run([
                "sudo", "-u", "postgres", "psql",
                "-p", str(self.port),
                "-d", "postgres",
                "-c", insert_sql
            ], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            return f"[{self.name}] Dummy DB change applied for incremental backup."
        except subprocess.CalledProcessError as e:
            return f"[{self.name}] DB change failed: {e.stderr.strip()}"

    # ----------------------------
    # Backup / Restore
    # ----------------------------
    def perform_backup(self, backup_type="full", db_name=None):
        """Cluster-level: pgBackRest, DB-level: pg_dump"""
        if db_name:
            # DB-level backup using pg_dump
            timestamp = time.strftime("%Y%m%d%H%M%S")
            dump_file = os.path.join(self.backup_dir, f"{db_name}_{timestamp}.backup")
            try:
                subprocess.check_output([
                    "sudo", "-u", "postgres", "pg_dump",
                    "-p", str(self.port),
                    "-F", "c",  # custom format
                    "-f", dump_file,
                    db_name
                ], stderr=subprocess.STDOUT, universal_newlines=True)
                return f"[{self.name}] DB-level backup completed: {dump_file}"
            except subprocess.CalledProcessError as e:
                return f"[{self.name}] DB-level backup failed: {e.output.strip()}"
        else:
            # Cluster-level backup using pgBackRest
            self._promote_if_needed()
            self.start_postgres()
            if backup_type == "incr":
                self.make_db_change()
            cmd = [
                "sudo", "-u", "postgres", "pgbackrest",
                f"--stanza={self.stanza}",
                f"--pg1-path={self.data_dir}",
                "backup",
                f"--type={backup_type}"
            ]
            try:
                output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, universal_newlines=True, env=self.env)
                # Extract latest backup label
                info_output = subprocess.check_output([
                    "sudo", "-u", "postgres", "pgbackrest",
                    f"--stanza={self.stanza}", "info"
                ], stderr=subprocess.STDOUT, universal_newlines=True, env=self.env)
                backup_labels = re.findall(r'(\d{8}-\d{6}[FI])', info_output)
                latest_label = backup_labels[-1] if backup_labels else None
                return f"[{self.name}] Cluster backup completed. Latest backup label: {latest_label}"
            except subprocess.CalledProcessError as e:
                return f"[{self.name}] Cluster backup failed: {e.output.strip()}"

    def perform_restore(self, backup_name=None, db_name=None, recent=False):
        """
        Restore a cluster backup by label (full or incremental chain) or DB backup.
        Ensures the cluster is primary (read-write) after restore.
        """
        # -------------------------
        # DB-level restore
        # -------------------------
        if db_name:
            # Ensure cluster is primary (read-write) before DB restore
            promote_result = self._promote_if_needed()

            # Determine backup file to use
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
                return f"[{self.name}] Must provide backup_name or set recent=True for DB restore"

            try:
                # Run pg_restore
                subprocess.check_output([
                    "sudo", "-u", "postgres", "pg_restore",
                    "-p", str(self.port),
                    "-d", db_name,
                    "--clean", "--if-exists",
                    "--verbose",
                    backup_file
                ], stderr=subprocess.STDOUT, universal_newlines=True)
                return f"[{self.name}] DB-level restore completed: {backup_file}. {promote_result}"

            except subprocess.CalledProcessError as e:
                return f"[{self.name}] DB-level restore failed: {e.output.strip()}"

        # -------------------------
        # Cluster-level restore (fixed)
        # -------------------------
        else:
            backups_info = self.list_backups()
            cluster_backups = backups_info.get("cluster_backups", [])

            if not cluster_backups:
                return f"[{self.name}] No cluster backups found."

            if recent:
                target_backup = cluster_backups[-1]["label"]
            elif backup_name:
                matching = [b for b in cluster_backups if b["label"] == backup_name]
                if not matching:
                    return f"[{self.name}] Backup '{backup_name}' not found"
                target_backup = matching[0]["label"]
            else:
                return f"[{self.name}] Must provide backup_name or set recent=True for cluster restore"

            # Stop cluster and clean data dir
            self.stop_postgres()
            self._clean_data_dir()

            cmd = [
                "sudo", "-u", "postgres", "pgbackrest",
                f"--stanza={self.stanza}",
                f"--pg1-path={self.data_dir}",
                "restore",
                "--type=immediate",
                "--delta",
                f"--set={target_backup}"
            ]
            try:
                subprocess.check_output(cmd, stderr=subprocess.STDOUT, universal_newlines=True, env=self.env)

                # ✅ Start PostgreSQL before promotion
                self.start_postgres()

                # ✅ Promote if needed
                promote_result = self._promote_if_needed()

                return f"[{self.name}] Cluster restore completed: {target_backup}. {promote_result}"

            except subprocess.CalledProcessError as e:
                self.start_postgres()
                return f"[{self.name}] Cluster restore failed: {e.output.strip()}"




    # ----------------------------
    # List backups
        # ----------------------------
    def list_backups(self):
        """
        List all backups:
        - Cluster-level backups using pgBackRest (full + incremental)
        - DB-level backups using pg_dump
        Returns a dict: {"cluster_backups": [...], "db_backups": {...}}
        """
        cluster_backups = []

        try:
            # Get JSON output from pgBackRest
            info_output = subprocess.check_output([
                "sudo", "-u", "postgres", "pgbackrest",
                f"--stanza={self.stanza}",
                "info",
                "--output=json"
            ], stderr=subprocess.STDOUT, universal_newlines=True, env=self.env)

            info_json = json.loads(info_output)

            # Handle JSON as list or dict
            if isinstance(info_json, list):
                # Find the stanza matching self.stanza
                stanza_info = next((s for s in info_json if s.get("name") == self.stanza), None)
            elif isinstance(info_json, dict):
                stanza_info = info_json if info_json.get("name") == self.stanza else None
            else:
                stanza_info = None

            if stanza_info:
                # The correct key is "backup", not "backups"
                backups_list = stanza_info.get("backup", [])
                for b in backups_list:
                    cluster_backups.append({
                        "label": b.get("label"),
                        #"type": b.get("type"),  # 'full' or 'incr'
                        #"timestamp_start": b.get("timestamp", {}).get("start"),
                        #"timestamp_stop": b.get("timestamp", {}).get("stop"),
                        #"prior": b.get("prior"),  # For incremental chain reference
                        #"reference": b.get("reference")  # Full/incr relationships
                    })

        except subprocess.CalledProcessError as e:
            cluster_backups = []
            print(f"[{self.name}] Failed to list cluster backups: {e.output.strip()}")

        # ----------------------------
        # DB-level backups (pg_dump)
        # ----------------------------
        db_backups = {}
        for cluster, db_list in CLUSTER_DATABASES.items():
            for db in db_list:
                files = sorted(glob(os.path.join(self.backup_dir, f"{db}_*.backup")))
                db_backups[db] = [{"label": os.path.basename(f)} for f in files]

        return {"cluster_backups": cluster_backups, "db_backups": db_backups}

    def execute(self, request: dict):
        """
        Execute tasks from JSON request or natural language user input.
        Automatically detects:
            - database name after 'database'
            - backup label after 'backup' or 'at'
            - supports DB-level and cluster-level restore/backup
            - handles 'recent' correctly for DB-level restore
        """
        actions = request.get("actions", [])
        results = {}

        for i, act in enumerate(actions, 1):
            action = act.get("action")

            # -----------------------------
            # Get raw user message from Ollama JSON
            # It could be in 'user_input' or 'inputs.message'
            # -----------------------------
            user_input = act.get("user_input")
            if not user_input:
                inputs = act.get("inputs", {})
                user_input = inputs.get("message") or " ".join(str(v) for v in inputs.values())

            user_input = user_input or ""

            # -----------------------------
            # Extract database name
            # -----------------------------
            db_name_match = re.search(r'\bdatabase\s+(\w+)\b', user_input, re.IGNORECASE)
            db_name = db_name_match.group(1) if db_name_match else act.get("db_name")

            # -----------------------------
            # Extract backup label
            # -----------------------------
            backup_name_match = re.search(r'\b(?:backup|at)\s+([\w\-\_\.]+\.backup)\b', user_input, re.IGNORECASE)
            backup_name = backup_name_match.group(1) if backup_name_match else act.get("backup_name")

            # -----------------------------
            # Detect recent keyword from user input
            # -----------------------------
            recent_flag = bool(re.search(r'\brecent\b', user_input, re.IGNORECASE))
            # Corrected: respect explicit 'recent' from Ollama JSON, otherwise use detected keyword
            recent = act["recent"] if "recent" in act else recent_flag

            # Debugging output
            print(f"[DEBUG] task_{i}: action={action}, db_name={db_name}, backup_name={backup_name}, recent={recent}")

            # -----------------------------
            # Execute based on action type
            # -----------------------------
            if action.lower() == "backup":
                results[f"task_{i}"] = self.perform_backup(
                    backup_type=act.get("backup_type", "full"),
                    db_name=db_name
                )

            elif action.lower() == "restore":
                results[f"task_{i}"] = self.perform_restore(
                    backup_name=backup_name,
                    db_name=db_name,
                    recent=recent
                )

            elif action.lower() == "list":
                results[f"task_{i}"] = self.list_backups()

            else:
                results[f"task_{i}"] = f"Unknown action {action}"

        return {"agent": self.name, "result": results}