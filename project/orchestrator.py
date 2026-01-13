#!/usr/bin/env python3
"""
AI Orchestrator for PostgreSQL Backup System with WAL Archiving
Ollama (llama3) + FastAPI
COMPLETE VERSION - AUTOMATIC PITR RESTORE (CORRECTED)
"""

import sys
import json
import time
import os
import shutil
import subprocess
import requests
from datetime import datetime
from pathlib import Path
from typing import Optional  # ✅ portable union types

try:
    import ollama
except ImportError:
    print("❌ Install Ollama SDK: pip install ollama")
    sys.exit(1)

# ======================= CONFIG =======================

FASTAPI = "http://localhost:8001"
MODEL = "llama3"

# Logical "server" name for this host
SERVER_NAME = "PG1"

# Databases that live on this server (must match FastAPI validation)
DATABASES = ["users_db", "products_db", "reports_db"]

# WAL / BACKUP Configuration
BACKUP_ROOT = os.environ.get("BACKUP_ROOT", "/root/sp-lakehouse-backup/project/backups")
WAL_ARCHIVE_DIR = os.path.join(BACKUP_ROOT, "wal")
BASE_BACKUP_DIR = os.path.join(BACKUP_ROOT, "base")
FULL_BACKUP_DIR = os.path.join(BACKUP_ROOT, "full")

# REAL data directory (from SHOW data_directory;)
PG_DATA_DIR = os.environ.get("PGDATA", "/var/lib/pgsql/17/data")

# Try these service names when stopping/starting PostgreSQL
PG_SERVICE_CANDIDATES = ["postgresql-17", "postgresql"]

# Ensure directories exist
Path(WAL_ARCHIVE_DIR).mkdir(parents=True, exist_ok=True)
Path(BASE_BACKUP_DIR).mkdir(parents=True, exist_ok=True)
Path(FULL_BACKUP_DIR).mkdir(parents=True, exist_ok=True)


# ======================= SYSTEMD HELPERS (for auto PITR) =======================

def _run_systemctl(action: str, service: str):
    """
    Helper to run systemctl commands safely.
    Returns (success: bool, output: str).
    """
    try:
        cmd = ["systemctl", action, service]
        result = subprocess.run(cmd, capture_output=True, text=True)
        ok = result.returncode == 0
        out = (result.stdout or "") + (result.stderr or "")
        return ok, out.strip()
    except Exception as e:
        return False, f"systemctl {action} {service} failed: {e}"


def stop_postgres_service():
    """
    Stop PostgreSQL using one of the known service names.
    Returns (success: bool, service_used: str | None, output: str)
    """
    last_output = ""
    for svc in PG_SERVICE_CANDIDATES:
        ok, out = _run_systemctl("stop", svc)
        last_output = out
        if ok:
            return True, svc, out or f"Stopped service {svc}"
    return False, None, last_output or "Unable to stop any PostgreSQL service"


def start_postgres_service():
    """
    Start PostgreSQL using one of the known service names.
    Returns (success: bool, service_used: str | None, output: str)
    """
    last_output = ""
    for svc in PG_SERVICE_CANDIDATES:
        ok, out = _run_systemctl("start", svc)
        last_output = out
        if ok:
            return True, svc, out or f"Started service {svc}"
    return False, None, last_output or "Unable to start any PostgreSQL service"


def get_postgres_status():
    """
    Returns (status: str, service_used: str | None, raw_output: str)
    status ∈ {"active", "inactive", "failed", "unknown"}
    """
    for svc in PG_SERVICE_CANDIDATES:
        ok, out = _run_systemctl("is-active", svc)
        if ok:
            return out.strip(), svc, out
    return "unknown", None, "No active PostgreSQL service found"


# ======================= WAL ARCHIVING SETUP =======================

def setup_wal_archiving():
    """
    Configure PostgreSQL for WAL archiving.
    Writes an archive script and a sample config file.
    """
    print("\n📁 Setting up WAL archiving...")
    try:
        archive_script_path = os.path.join(BACKUP_ROOT, "archive_wal.sh")
        archive_script_content = f"""#!/bin/bash
# WAL Archive Script
# Called by PostgreSQL when a WAL segment is ready
# Usage: archive_wal.sh %p %f

WAL_FILE="$1"
WAL_NAME="$2"
ARCHIVE_DIR="{WAL_ARCHIVE_DIR}"

mkdir -p "$ARCHIVE_DIR"
cp "$WAL_FILE" "$ARCHIVE_DIR/$WAL_NAME" || exit 1
chmod 600 "$ARCHIVE_DIR/$WAL_NAME" || exit 1

if [ ! -f "$ARCHIVE_DIR/$WAL_NAME" ]; then
    echo "Archive failed: $WAL_NAME" >&2
    exit 1
fi

echo "Archived: $WAL_NAME to $ARCHIVE_DIR"
exit 0
"""
        with open(archive_script_path, "w", encoding="utf-8") as f:
            f.write(archive_script_content)
        os.chmod(archive_script_path, 0o755)
        print(f"✅ Created archive script: {archive_script_path}")

        pg_config = f"""
# WAL Archiving Configuration for {SERVER_NAME}
# Add these to postgresql.conf

wal_level = replica
archive_mode = on
archive_command = '{archive_script_path} %p %f'
archive_timeout = 60

max_wal_senders = 3
wal_keep_size = 512MB

checkpoint_timeout = 5min
max_wal_size = 1GB
min_wal_size = 80MB
"""
        config_file = os.path.join(BACKUP_ROOT, "postgresql_wal_config.txt")
        with open(config_file, "w", encoding="utf-8") as f:
            f.write(pg_config)

        print(f"✅ PostgreSQL config saved: {config_file}")
        print(f"✅ WAL archive directory: {WAL_ARCHIVE_DIR}")
        print("\n⚠️  IMPORTANT: Add config to postgresql.conf and restart PostgreSQL")
        return True

    except Exception as e:
        print(f"❌ Failed to setup WAL archiving: {e}")
        return False


def force_wal_rotation():
    """
    Force PostgreSQL to switch to a new WAL segment.
    """
    print("\n🔄 Forcing WAL rotation (pg_switch_wal)...")
    try:
        cmd = [
            "psql",
            "-h", "localhost",
            "-U", "postgres",
            "-d", "postgres",
            "-c", "SELECT pg_switch_wal();",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            print(f"❌ WAL rotation failed:\n{result.stderr}")
            return False

        print("✅ WAL rotation successful")
        print(f"   Output: {result.stdout.strip()}")
        time.sleep(2)
        list_wal_files()
        return True

    except subprocess.TimeoutExpired:
        print("❌ WAL rotation timed out")
        return False
    except Exception as e:
        print(f"❌ Error during WAL rotation: {e}")
        return False


def list_wal_files():
    """
    List all archived WAL files.
    """
    print(f"\n📂 Archived WAL Files in {WAL_ARCHIVE_DIR}:")
    try:
        if not os.path.exists(WAL_ARCHIVE_DIR):
            print("   (No WAL archive directory found)")
            return []

        wal_files = []
        for filename in sorted(os.listdir(WAL_ARCHIVE_DIR)):
            filepath = os.path.join(WAL_ARCHIVE_DIR, filename)
            if os.path.isfile(filepath):
                stat = os.stat(filepath)
                size_mb = stat.st_size / (1024 * 1024)
                mtime = datetime.fromtimestamp(stat.st_mtime)
                wal_files.append(
                    {
                        "name": filename,
                        "size_mb": round(size_mb, 2),
                        "size_bytes": stat.st_size,
                        "modified": mtime.isoformat(),
                        "path": filepath,
                    }
                )
                print(
                    f"   • {filename:40s} {size_mb:6.1f}MB  {mtime.strftime('%Y-%m-%d %H:%M:%S')}"
                )

        if not wal_files:
            print("   (No WAL files archived yet)")
        else:
            total_size = sum(f["size_bytes"] for f in wal_files)
            print(
                f"\n   Total: {len(wal_files)} files, {total_size / (1024**2):.1f} MB"
            )

        return wal_files

    except Exception as e:
        print(f"❌ Error listing WAL files: {e}")
        return []


def cleanup_old_wal_files(keep_count=50):
    """
    Clean up old WAL files, keeping only the most recent ones.
    """
    print(f"\n🧹 Cleaning up old WAL files (keeping {keep_count} most recent)...")
    try:
        wal_files = []
        for filename in os.listdir(WAL_ARCHIVE_DIR):
            filepath = os.path.join(WAL_ARCHIVE_DIR, filename)
            if os.path.isfile(filepath):
                mtime = os.path.getmtime(filepath)
                wal_files.append((filepath, mtime, filename))

        wal_files.sort(key=lambda x: x[1])

        if len(wal_files) <= keep_count:
            print(f"   ✅ Only {len(wal_files)} files, no cleanup needed")
            return

        to_delete = wal_files[:-keep_count]
        deleted_count = 0
        deleted_size = 0

        for filepath, _, filename in to_delete:
            try:
                size = os.path.getsize(filepath)
                os.remove(filepath)
                deleted_count += 1
                deleted_size += size
                print(f"   🗑️  Deleted: {filename}")
            except Exception as e:
                print(f"   ⚠️  Failed to delete {filename}: {e}")

        print(
            f"\n   ✅ Cleanup complete: {deleted_count} files removed, {deleted_size / (1024**2):.1f} MB freed"
        )

    except Exception as e:
        print(f"❌ Error during WAL cleanup: {e}")


def verify_wal_archiving():
    """
    Verify WAL archiving configuration and presence of WAL files.
    """
    print("\n🔍 Verifying WAL archiving configuration...")
    checks = []

    if os.path.exists(WAL_ARCHIVE_DIR):
        print(f"   ✅ WAL archive directory exists: {WAL_ARCHIVE_DIR}")
        checks.append(True)
    else:
        print(f"   ❌ WAL archive directory missing: {WAL_ARCHIVE_DIR}")
        checks.append(False)

    archive_script = os.path.join(BACKUP_ROOT, "archive_wal.sh")
    if os.path.exists(archive_script) and os.access(archive_script, os.X_OK):
        print("   ✅ Archive script exists and is executable")
        checks.append(True)
    else:
        print(f"   ❌ Archive script missing or not executable: {archive_script}")
        checks.append(False)

    try:
        cmd = [
            "psql",
            "-h",
            "localhost",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-t",
            "-c",
            "SHOW archive_mode;",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            archive_mode = result.stdout.strip()
            if archive_mode == "on":
                print(f"   ✅ PostgreSQL archive_mode: {archive_mode}")
                checks.append(True)
            else:
                print(
                    f"   ⚠️  PostgreSQL archive_mode: {archive_mode} (should be 'on')"
                )
                checks.append(False)
        else:
            print("   ❌ Could not check archive_mode")
            checks.append(False)
    except Exception as e:
        print(f"   ❌ Error checking PostgreSQL config: {e}")
        checks.append(False)

    wal_files = list_wal_files()
    if wal_files:
        print(f"   ✅ Found {len(wal_files)} archived WAL files")
        checks.append(True)
    else:
        print("   ⚠️  No WAL files archived yet")
        checks.append(False)

    passed = sum(1 for c in checks if c)
    total = len(checks)

    print(f"\n   📊 Verification: {passed}/{total} checks passed")
    if passed == total:
        print("   ✅ WAL archiving is properly configured and working")
        return True
    elif passed >= total - 1:
        print("   ⚠️  WAL archiving is mostly configured (minor issues)")
        return True
    else:
        print("   ❌ WAL archiving needs configuration")
        return False


def list_available_base_backups():
    """
    List all available base backups in the BASE_BACKUP_DIR.
    Each one is a directory like: pg_base_YYYYMMDD_HHMMSS
    """
    base_dir = Path(BASE_BACKUP_DIR)
    if not base_dir.exists():
        return []

    backups = []
    for item in base_dir.iterdir():
        if item.is_dir():
            backups.append(
                {
                    "name": item.name,
                    "path": str(item),
                    "created": datetime.fromtimestamp(item.stat().st_mtime).isoformat(),
                }
            )

    backups.sort(key=lambda x: x["created"], reverse=True)
    return backups


def select_base_backup():
    """
    Print available base backups for the user.
    """
    print("\n📦 Available Base Backups (for PITR):")
    backups = list_available_base_backups()
    if not backups:
        print("   ❌ No base backups found!")
        print("   💡 Create one first: 'take base backup'")
        return None

    for i, backup in enumerate(backups, 1):
        print(f"   {i}. {backup['name']} (created: {backup['created']})")
    return backups


# ======================= AUDIT LOGGER =======================

class AuditLogger:
    def __init__(self, log_file="backup_audit.log"):
        self.log_file = log_file

    def log(self, user_input, parsed, success, result):
        entry = {
            "time": datetime.now().isoformat(),
            "input": user_input,
            "parsed": parsed,
            "success": success,
            "result": (result or "")[:300],
        }
        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            print(f"⚠️ Failed to write audit log: {e}")


# ======================= SAFETY =======================

def safety_timer(sec=10):
    print(f"\n⏳ Safety Timer: {sec}s (CTRL+C to cancel)")
    try:
        for i in range(sec, 0, -1):
            print(f"   {i} seconds left...", end="\r")
            time.sleep(1)
        print("\n✅ Proceeding...")
        return True
    except KeyboardInterrupt:
        print("\n🛑 Cancelled!")
        return False


def confirm(op, params):
    """Only used for destructive restore operations"""
    print("\n⚠️ CONFIRM OPERATION")
    print("Operation:", op)
    print("Params:", params)
    return input("Type YES to continue: ").strip() == "YES"


# ======================= AUTO PITR IMPLEMENTATION =======================

def perform_pitr_restore(base_backup_name: str, target_time: Optional[str] = None):
    """
    REAL AUTOMATIC PITR RESTORE (Base Backup + WAL)

    - base_backup_name: directory name under BASE_BACKUP_DIR (e.g. pg_base_20251210_172506)
    - target_time: string 'YYYY-MM-DD HH:MM:SS' or None for "latest"
    """

    print("\n⚙️  Starting REAL automatic PITR restore...")

    base_dir = Path(BASE_BACKUP_DIR) / base_backup_name

    if not base_dir.exists():
        msg = f"Base backup not found at {base_dir}"
        print(f"❌ {msg}")
        return {"success": False, "error": msg}

    print(f"   Using base backup: {base_dir}")
    print(f"   PGDATA: {PG_DATA_DIR}")

    # 1️⃣ Stop PostgreSQL
    print("1️⃣ Stopping PostgreSQL service...")
    stopped, svc_used, stop_out = stop_postgres_service()
    if not stopped:
        msg = f"Failed to stop PostgreSQL: {stop_out}"
        print(f"❌ {msg}")
        return {"success": False, "error": msg}

    print(f"✅ Stopped service: {svc_used}")

    # 2️⃣ Backup current data directory
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_existing_dir = f"{PG_DATA_DIR}.pre_pitr_{ts}"

    if os.path.exists(PG_DATA_DIR):
        print(f"2️⃣ Backing up current data directory → {backup_existing_dir}")
        shutil.move(PG_DATA_DIR, backup_existing_dir)
    else:
        print("2️⃣ PGDATA does not exist yet, skipping pre-backup move")

    # 3️⃣ Restore base backup (directory copy)
    print(f"3️⃣ Restoring BASE BACKUP → {PG_DATA_DIR}")
    shutil.copytree(base_dir, PG_DATA_DIR)

    # Ownership
    subprocess.run(["chown", "-R", "postgres:postgres", PG_DATA_DIR], check=False)

    # ✅ REQUIRED: backup_label must exist (basic sanity check)
    if not Path(PG_DATA_DIR, "backup_label").exists():
        return {
            "success": False,
            "error": "INVALID BASE BACKUP (backup_label missing)",
            "pg_data_dir": PG_DATA_DIR,
            "base_backup_dir": str(base_dir),
        }

    # 4️⃣ Create recovery.signal
    print("4️⃣ Creating recovery.signal")
    recovery_signal = Path(PG_DATA_DIR) / "recovery.signal"
    recovery_signal.touch(exist_ok=True)
    subprocess.run(["chown", "postgres:postgres", str(recovery_signal)], check=False)

    # 5️⃣ Configure WAL restore
    print("5️⃣ Writing restore_command + recovery_target_time into postgresql.auto.conf")
    auto_conf = Path(PG_DATA_DIR) / "postgresql.auto.conf"

    # Optional: remove old restore_command/recovery_target_time lines
    if auto_conf.exists():
        try:
            with open(auto_conf, "r", encoding="utf-8") as f:
                lines = f.readlines()
            new_lines = []
            for line in lines:
                if line.strip().startswith("restore_command") or line.strip().startswith("recovery_target_time"):
                    continue
                new_lines.append(line)
            with open(auto_conf, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
        except Exception as e:
            print(f"   ⚠️ Could not clean old restore_command/recovery_target_time: {e}")

    # Append our settings
    with open(auto_conf, "a", encoding="utf-8") as f:
        f.write(f"\nrestore_command = 'cp {WAL_ARCHIVE_DIR}/%f %p'\n")
        if target_time:
            f.write(f"recovery_target_time = '{target_time}'\n")
        else:
            # "latest" - just replay all WAL
            f.write("# recovery_target_time not set -> restore to latest available WAL\n")

    # 6️⃣ Start PostgreSQL
    print("6️⃣ Starting PostgreSQL for WAL replay...")
    started, svc_used, start_out = start_postgres_service()

    if not started:
        return {
            "success": False,
            "error": start_out,
            "data_backup": backup_existing_dir,
        }

    time.sleep(4)
    status, svc, raw = get_postgres_status()

    return {
        "success": True,
        "server": SERVER_NAME,
        "service": svc_used,
        "status": status,
        "pg_data_dir": PG_DATA_DIR,
        "base_backup_dir": str(base_dir),
        "wal_archive_dir": WAL_ARCHIVE_DIR,
        "target_time": target_time,
        "pre_pitr_backup_dir": backup_existing_dir,
        "note": "REAL PITR restore started. PostgreSQL is now replaying WAL files.",
    }


# ======================= AI PROMPT =======================

SYSTEM_PROMPT = f"""
You control a PostgreSQL backup system via HTTP API with WAL archiving.

There is ONE logical server called "{SERVER_NAME}" with these databases:
{DATABASES}

VERY IMPORTANT DISTINCTION:
- LOGICAL backups are .sql files per-database (created by /backup/full)
- BASE backups are pg_base_YYYYMMDD_HHMMSS directories (used for PITR with WAL)

SUPPORTED INTENTS (map them carefully):

- "backup users_db" / "backup products_db" / "backup reports_db"
- "backup pg1" / "backup server" / "backup all databases"

- "rotate wal" / "force wal rotation" / "incremental backup of pg1"
- "list wal files" / "cleanup wal"

- "setup wal" / "verify wal"

- "take base backup"  (creates pg_base_YYYYMMDD_HHMMSS directory)

- "list base backups"  (list pg_base_YYYYMMDD_HHMMSS directories)

- "list backups for users_db"  (list LOGICAL .sql backups for that DB)
- "list backups for pg1" / "list backups for server" (list LOGICAL .sql for all DBs)

- "list servers"

- "logical restore users_db from users_db_full_xxx.sql"
  (use LOGICAL_RESTORE, NOT PITR)

- "restore to point in time using pg_base_YYYYMMDD_HHMMSS"
- "restore to point in time using pg_base_YYYYMMDD_HHMMSS at 2025-12-10 17:20:00"
  (these use PITR_RESTORE with base_backup_name and optional target_time)

YOU MUST RETURN EXACTLY ONE OF THESE JSON SHAPES:

1) FULL BACKUP
   {{ "FULL_BACKUP": {{ "db_name": "users_db" }} }}
   {{ "FULL_BACKUP": {{ "db_name": "pg1" }} }}

2) WAL ROTATE
   {{ "WAL_ROTATE": {{}} }}

3) LIST WAL
   {{ "LIST_WAL": {{}} }}

4) CLEANUP WAL
   {{ "CLEANUP_WAL": {{ "keep_count": 50 }} }}

5) VERIFY WAL
   {{ "VERIFY_WAL": {{}} }}

6) SETUP WAL
   {{ "SETUP_WAL": {{}} }}

7) BASE BACKUP
   {{ "BASE_BACKUP": {{}} }}

8) LIST BASE BACKUPS
   {{ "LIST_BASE_BACKUPS": {{}} }}

9) HEALTH CHECK
   {{ "HEALTH": {{}} }}

10) LOGICAL RESTORE
   {{ "LOGICAL_RESTORE": {{ "db_name": "users_db", "backup_file": "users_db_full_20251210_120000.sql" }} }}

11) PITR RESTORE (AUTO)
   {{ "PITR_RESTORE": {{ "base_backup_name": "pg_base_20251210_172506", "target_time": null }} }}
   or with time:
   {{ "PITR_RESTORE": {{ "base_backup_name": "pg_base_20251210_172506", "target_time": "2025-12-10 17:20:00" }} }}

12) LIST BACKUPS (LOGICAL .sql BACKUPS)
   {{ "LIST_BACKUPS": {{ "db_name": "users_db" }} }}
   {{ "LIST_BACKUPS": {{ "db_name": "pg1" }} }}

13) LIST SERVERS
   {{ "LIST_SERVERS": {{}} }}

STRICT RULES:
- Return ONLY ONE of these objects.
- NO extra keys, comments, or text.
- NO "action" wrapper.
- JSON only.
"""


# ======================= ORCHESTRATOR =======================

class Orchestrator:
    def __init__(self):
        self.audit = AuditLogger()

    def ask_ai(self, text):
        res = ollama.chat(
            model=MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": text},
            ],
            format="json",
            options={"temperature": 0.1},
        )
        raw = res["message"]["content"].strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        try:
            return json.loads(raw)
        except Exception as e:
            print("❌ AI returned invalid JSON:")
            print(raw)
            print(f"Error: {e}")
            return None

    def _run_full_backup_single(self, db_name: str):
        r = requests.post(f"{FASTAPI}/backup/full", json={"db_name": db_name})
        try:
            return r.json()
        except Exception:
            return {"raw": r.text}

    def _run_list_backups_single(self, db_name: str):
        r = requests.get(f"{FASTAPI}/backups/{db_name}")
        try:
            return r.json()
        except Exception:
            return {"raw": r.text}

    def execute(self, action, user_input):
        if not action:
            return

        try:
            # SETUP WAL
            if "SETUP_WAL" in action:
                result = setup_wal_archiving()
                self.audit.log(
                    user_input,
                    action,
                    result,
                    "WAL setup completed" if result else "WAL setup failed",
                )
                return

            # VERIFY WAL
            if "VERIFY_WAL" in action:
                result = verify_wal_archiving()
                self.audit.log(
                    user_input, action, result, "WAL verification completed"
                )
                return

            # LIST WAL
            if "LIST_WAL" in action:
                wal_files = list_wal_files()
                self.audit.log(
                    user_input, action, True, f"Found {len(wal_files)} WAL files"
                )
                return

            # CLEANUP WAL
            if "CLEANUP_WAL" in action:
                payload = action.get("CLEANUP_WAL") or {}
                keep_count = payload.get("keep_count", 50)
                cleanup_old_wal_files(keep_count)
                self.audit.log(
                    user_input,
                    action,
                    True,
                    f"WAL cleanup with keep_count={keep_count}",
                )
                return

            # LIST BASE BACKUPS
            if "LIST_BASE_BACKUPS" in action:
                print("\n📦 Available Base Backups for PITR:\n")
                backups = list_available_base_backups()
                if not backups:
                    print("   ❌ No base backups found!")
                    print("   💡 Create one first: 'take base backup'\n")
                else:
                    for i, backup in enumerate(backups, 1):
                        print(f"   {i}. {backup['name']}")
                        print(f"      Path: {backup['path']}")
                        print(f"      Created: {backup['created']}\n")
                    print(f"   Total: {len(backups)} base backup(s)\n")
                    print("   🔁 To use one for PITR, say:")
                    print("      restore to point in time using <name>")
                    print("      or")
                    print("      restore to point in time using <name> at YYYY-MM-DD HH:MM:SS\n")
                self.audit.log(
                    user_input, action, True, f"Found {len(backups)} base backups"
                )
                return

            # LIST SERVERS
            if "LIST_SERVERS" in action:
                r = requests.get(f"{FASTAPI}/servers")
                try:
                    data = r.json()
                except Exception:
                    data = {"raw": r.text}
                print("\n🖥️ SERVERS:")
                print(json.dumps(data, indent=2))
                self.audit.log(user_input, action, True, json.dumps(data))
                return

            # HEALTH
            if "HEALTH" in action:
                r = requests.get(f"{FASTAPI}/health")
                try:
                    data = r.json()
                    print("\n✅ HEALTH CHECK:")
                    print(json.dumps(data, indent=2))
                    self.audit.log(user_input, action, True, json.dumps(data))
                except Exception:
                    print(r.text)
                    self.audit.log(user_input, action, False, r.text)
                return

            # 👉 AUTO PITR RESTORE
            if "PITR_RESTORE" in action:
                payload = action.get("PITR_RESTORE") or {}
                base_backup_name = payload.get("base_backup_name")
                target_time = payload.get("target_time")

                # Defensive: handle None safely
                if isinstance(base_backup_name, str):
                    base_backup_name = base_backup_name.strip()
                else:
                    base_backup_name = ""

                if isinstance(target_time, str):
                    target_time = target_time.strip() or None
                else:
                    target_time = None

                if not base_backup_name:
                    print("\n❌ No base backup name specified.")
                    backups = select_base_backup()
                    if backups:
                        print("\n💡 Example:")
                        print(
                            f"   restore to point in time using {backups[0]['name']}"
                        )
                    self.audit.log(
                        user_input, action, False, "No base backup specified"
                    )
                    return

                # Confirm PITR restore
                print(f"\n⚠️ Preparing automatic PITR restore using: {base_backup_name}")
                if target_time:
                    print(f"   Target time: {target_time}")
                else:
                    print("   Target time: LATEST (replay all WAL)")

                if not confirm("PITR_RESTORE", action):
                    print("❌ Cancelled by user")
                    return
                if not safety_timer(10):
                    print("❌ Cancelled by safety timer")
                    return

                result = perform_pitr_restore(base_backup_name, target_time)
                print("\n✅ PITR RESULT:")
                print(json.dumps(result, indent=2))
                self.audit.log(
                    user_input, action, result.get("success", False), json.dumps(result)
                )
                return

            # FULL BACKUP (LOGICAL .sql)
            if "FULL_BACKUP" in action:
                payload = action.get("FULL_BACKUP") or {}
                requested = str(payload.get("db_name", "")).strip()

                if not requested:
                    msg = "FULL_BACKUP requires 'db_name'"
                    print(f"❌ {msg}")
                    self.audit.log(user_input, action, False, msg)
                    return

                if requested.lower() in ["pg1", "server", SERVER_NAME.lower()]:
                    print(
                        f"\n📦 Server-level logical backup: ALL databases on {SERVER_NAME}\n"
                    )
                    results = []
                    for db in DATABASES:
                        print(f"\n📦 Backing up {db}...")
                        res = self._run_full_backup_single(db)
                        print(json.dumps(res, indent=2))
                        results.append({"db": db, "result": res})
                    print("\n✅ Server-level logical backup complete (.sql files)")
                    self.audit.log(user_input, action, True, json.dumps(results))
                    return

                if requested in DATABASES:
                    print(f"\n📦 Starting logical full backup for: {requested}")
                    res = self._run_full_backup_single(requested)
                    print("\n✅ BACKUP RESULT (.sql):")
                    print(json.dumps(res, indent=2))
                    self.audit.log(user_input, action, True, json.dumps(res))
                    return

                msg = f"Unknown database: {requested}. Valid: {DATABASES}"
                print(f"❌ {msg}")
                self.audit.log(user_input, action, False, msg)
                return

            # BASE BACKUP (PHYSICAL, for PITR)
            if "BASE_BACKUP" in action:
                print("\n📦 Starting physical base backup (pg_basebackup)...")
                print("   This creates a snapshot for PITR under BASE_BACKUP_DIR\n")

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_name = f"pg_base_{timestamp}"
                backup_path = os.path.join(BASE_BACKUP_DIR, backup_name)

                try:
                    print(f"🔄 Running pg_basebackup → {backup_path}")
                    # Plain format (-Fp) to make directory-based restore easy
                    cmd = [
                        "pg_basebackup",
                        "-h",
                        "localhost",
                        "-U",
                        "postgres",
                        "-D",
                        backup_path,
                        "-Fp",  # plain directory
                        "-X",
                        "stream",
                        "-P",
                    ]
                    result = subprocess.run(
                        cmd, capture_output=True, text=True, timeout=1200
                    )
                    if result.returncode != 0:
                        print("\n❌ Base backup failed:")
                        print(result.stderr)
                        self.audit.log(user_input, action, False, result.stderr)
                        return

                    total_size = 0
                    for root, dirs, files in os.walk(backup_path):
                        for fname in files:
                            total_size += os.path.getsize(os.path.join(root, fname))
                    size_mb = total_size / (1024 * 1024)

                    info = {
                        "success": True,
                        "type": "base",
                        "server": SERVER_NAME,
                        "base_backup_dir": backup_path,
                        "name": backup_name,
                        "size_mb": round(size_mb, 2),
                        "timestamp": timestamp,
                    }
                    print("\n✅ Base backup completed:")
                    print(json.dumps(info, indent=2))
                    print("\n💡 Use this for PITR restore, e.g.:")
                    print(f"   restore to point in time using {backup_name}")
                    self.audit.log(user_input, action, True, json.dumps(info))
                    return

                except subprocess.TimeoutExpired:
                    msg = "Base backup timed out"
                    print(f"\n❌ {msg}")
                    self.audit.log(user_input, action, False, msg)
                    return
                except Exception as e:
                    msg = f"Base backup error: {e}"
                    print(f"\n❌ {msg}")
                    self.audit.log(user_input, action, False, msg)
                    return

            # WAL ROTATE
            if "WAL_ROTATE" in action:
                print("\n🔄 Performing WAL rotation (incremental backup)...")
                ok = force_wal_rotation()
                if ok:
                    print("\n✅ Incremental backup complete (WAL rotation successful)")
                    self.audit.log(
                        user_input, action, True, "WAL rotation successful"
                    )
                else:
                    print("\n❌ Incremental backup failed")
                    self.audit.log(
                        user_input, action, False, "WAL rotation failed"
                    )
                return

            # LIST BACKUPS (LOGICAL .sql backups)
            if "LIST_BACKUPS" in action:
                payload = action.get("LIST_BACKUPS") or {}
                requested = str(payload.get("db_name", "")).strip()

                if not requested:
                    msg = "LIST_BACKUPS requires 'db_name'"
                    print(f"❌ {msg}")
                    self.audit.log(user_input, action, False, msg)
                    return

                # Server-level: list logical backups for all DBs
                if requested.lower() in ["pg1", "server", SERVER_NAME.lower()]:
                    print(
                        f"\n📋 Listing LOGICAL (.sql) backups for ALL databases on {SERVER_NAME}:\n"
                    )
                    all_results = []
                    for db in DATABASES:
                        print(f"\n{'='*60}")
                        print(f"Database: {db}  (LOGICAL .sql backups)")
                        print("=" * 60)
                        res = self._run_list_backups_single(db)
                        if "full_backups" in res:
                            backups = res["full_backups"]
                            if backups:
                                for i, backup in enumerate(backups, 1):
                                    print(f"  {i}. {backup}")
                                print(f"\n  Total: {len(backups)} backup(s)")
                                print("  💡 To restore a .sql backup, say:")
                                print(f"     logical restore {db} from <filename>.sql")
                            else:
                                print("  No logical (.sql) backups found")
                        else:
                            print(f"  Response: {res}")
                        all_results.append({"db": db, "result": res})
                    print(f"\n{'='*60}\n")
                    self.audit.log(user_input, action, True, json.dumps(all_results))
                    return

                # Single database
                if requested in DATABASES:
                    print(f"\n📋 LOGICAL (.sql) backups for {requested}:\n")
                    res = self._run_list_backups_single(requested)
                    if "full_backups" in res:
                        backups = res["full_backups"]
                        if backups:
                            for i, backup in enumerate(backups, 1):
                                print(f"  {i}. {backup}")
                            print(f"\n  Total: {len(backups)} backup(s)\n")
                            print("  💡 To restore one of these, say:")
                            print(
                                f"     logical restore {requested} from <filename>.sql\n"
                            )
                        else:
                            print("  No logical (.sql) backups found\n")
                    else:
                        print(json.dumps(res, indent=2))
                    self.audit.log(user_input, action, True, json.dumps(res))
                    return

                msg = f"Unknown database: {requested}. Valid: {DATABASES}"
                print(f"❌ {msg}")
                self.audit.log(user_input, action, False, msg)
                return

            # LOGICAL RESTORE (.sql -> per-DB)
            if "LOGICAL_RESTORE" in action:
                payload = action.get("LOGICAL_RESTORE") or {}
                db_name = payload.get("db_name", "").strip()
                backup_file = payload.get("backup_file", "").strip()

                if not db_name or not backup_file:
                    msg = "LOGICAL_RESTORE requires 'db_name' and 'backup_file'"
                    print(f"❌ {msg}")
                    self.audit.log(user_input, action, False, msg)
                    return

                print(
                    f"\n⚠️ LOGICAL RESTORE (this uses .sql, NOT base backup + WAL)\n"
                    f"   Database: {db_name}\n"
                    f"   Backup file: {backup_file}\n"
                )

                if not confirm("LOGICAL_RESTORE", action):
                    print("❌ Cancelled by user")
                    return
                if not safety_timer(10):
                    print("❌ Cancelled by safety timer")
                    return

                print(f"\n🔄 Restoring {db_name} from {backup_file}...")
                r = requests.post(
                    f"{FASTAPI}/restore/logical",
                    json={"db_name": db_name, "backup_file": backup_file},
                )
                try:
                    data = r.json()
                    print("\n✅ LOGICAL RESTORE RESULT:")
                    print(json.dumps(data, indent=2))
                    self.audit.log(user_input, action, True, json.dumps(data))
                except Exception:
                    print(r.text)
                    self.audit.log(user_input, action, False, r.text)
                return

            # UNKNOWN
            print(f"❌ Unknown action: {list(action.keys())}")
            self.audit.log(user_input, action, False, "Unknown action")

        except Exception as e:
            print(f"❌ Execution error: {e}")
            import traceback

            traceback.print_exc()
            self.audit.log(user_input, action, False, str(e))


# ======================= MAIN LOOP =======================

def main():
    print("\n" + "=" * 70)
    print("🤖 AI-Powered PostgreSQL Backup Orchestrator (AUTO PITR - CORRECTED)")
    print(f"   Server: {SERVER_NAME}")
    print(f"   Databases: {', '.join(DATABASES)}")
    print(f"   Model: {MODEL}")
    print(f"   PGDATA: {PG_DATA_DIR}")
    print(f"   BACKUP_ROOT: {BACKUP_ROOT}")
    print("=" * 70)

    print("\n🔍 Quick System Check:")
    if not os.path.exists(WAL_ARCHIVE_DIR):
        print(f"   ⚠️  WAL archive directory not found: {WAL_ARCHIVE_DIR}")
        print("   💡 Run 'setup wal archiving'")
    else:
        print(f"   ✅ WAL archive directory: {WAL_ARCHIVE_DIR}")

    if not os.path.exists(BASE_BACKUP_DIR):
        print(f"   ⚠️  Base backup directory not found: {BASE_BACKUP_DIR}")
    else:
        print(f"   ✅ Base backup directory: {BASE_BACKUP_DIR}")

    print("\n💡 Useful commands:")
    print("   • 'backup users_db'                 → logical .sql backup")
    print("   • 'list backups for users_db'       → list .sql backups")
    print("   • 'logical restore users_db from ...sql'")
    print("   • 'take base backup'                → pg_base_YYYYMMDD_HHMMSS for PITR")
    print("   • 'list base backups'               → show PITR-capable base backups")
    print("   • 'rotate wal'                      → force WAL rotation")
    print("   • 'list wal files'                  → show archived WAL segments")
    print("   • 'restore to point in time using pg_base_YYYYMMDD_HHMMSS'")
    print("   • 'restore to point in time using pg_base_YYYYMMDD_HHMMSS at 2025-12-10 17:20:00'")
    print("\n" + "=" * 70 + "\n")

    orch = Orchestrator()

    while True:
        try:
            user_input = input("🎤 You: ").strip()
            if not user_input:
                continue
            if user_input.lower() in ["exit", "quit", "q"]:
                print("\n👋 Goodbye!")
                break

            print("\n🤔 Thinking...")
            action = orch.ask_ai(user_input)
            if action:
                print(f"📋 Parsed Action: {json.dumps(action, indent=2)}")
                orch.execute(action, user_input)
            else:
                print("❌ Failed to parse command")
            print()

        except KeyboardInterrupt:
            print("\n\n🛑 Interrupted. Exiting...")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback

            traceback.print_exc()


if __name__ == "__main__":
    main()
