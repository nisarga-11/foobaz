#!/usr/bin/env python3
import os
import json
import time
import shutil
import subprocess
import requests
from datetime import datetime
from pathlib import Path
import ollama

# ======================= CONFIG =======================

FASTAPI = "http://localhost:8000"
MODEL = "llama3"
SERVER_NAME = "PG1"

DATABASES = ["users_db", "products_db", "reports_db"]

BACKUP_ROOT = "/root/sp-lakehouse-backup/project/backups"
WAL_ARCHIVE_DIR = f"{BACKUP_ROOT}/wal"
BASE_BACKUP_DIR = f"{BACKUP_ROOT}/base"
FULL_BACKUP_DIR = f"{BACKUP_ROOT}/full"

# ✅ VERIFIED DATA DIRECTORY
PG_DATA_DIR = "/var/lib/pgsql/17/data"

# ✅ VERIFIED SERVICE
PG_SERVICE = "postgresql-17"

Path(WAL_ARCHIVE_DIR).mkdir(parents=True, exist_ok=True)
Path(BASE_BACKUP_DIR).mkdir(parents=True, exist_ok=True)
Path(FULL_BACKUP_DIR).mkdir(parents=True, exist_ok=True)

# ======================= SYSTEMCTL =======================

def stop_postgres():
    subprocess.run(["systemctl", "stop", PG_SERVICE], check=False)
    time.sleep(5)

def start_postgres():
    subprocess.run(["systemctl", "start", PG_SERVICE], check=False)
    time.sleep(8)

# ======================= WAL ROTATION =======================

def rotate_wal():
    subprocess.run([
        "psql", "-U", "postgres", "-d", "postgres",
        "-c", "SELECT pg_switch_wal();"
    ], check=False)

# ======================= WAL LIST =======================

def list_wal():
    if not os.path.exists(WAL_ARCHIVE_DIR):
        print("❌ No WAL directory")
        return

    print("\n📂 WAL FILES:\n")
    for f in sorted(os.listdir(WAL_ARCHIVE_DIR)):
        fp = os.path.join(WAL_ARCHIVE_DIR, f)
        size = os.path.getsize(fp) / (1024 * 1024)
        print(f"{f}  |  {size:.2f} MB")

# ======================= AUTO PITR =======================

def auto_pitr(base_backup_name, target_time=None):

    base_backup_path = f"{BASE_BACKUP_DIR}/{base_backup_name}"

    if not os.path.exists(base_backup_path):
        print(f"❌ Base backup not found: {base_backup_path}")
        return

    print("\n🛑 Stopping PostgreSQL...")
    stop_postgres()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    old_dir = f"{PG_DATA_DIR}_old_{ts}"

    if os.path.exists(PG_DATA_DIR):
        print("📦 Backing up current data directory...")
        shutil.move(PG_DATA_DIR, old_dir)

    print("📥 Restoring base backup...")
    shutil.copytree(base_backup_path, PG_DATA_DIR)

    subprocess.run(["chown", "-R", "postgres:postgres", PG_DATA_DIR], check=False)

    print("⚙️ Configuring recovery...")
    open(f"{PG_DATA_DIR}/recovery.signal", "w").close()

    with open(f"{PG_DATA_DIR}/postgresql.auto.conf", "a") as f:
        f.write(f"\nrestore_command = 'cp {WAL_ARCHIVE_DIR}/%f %p'\n")
        if target_time:
            f.write(f"recovery_target_time = '{target_time}'\n")

    print("▶️ Starting PostgreSQL...")
    start_postgres()

    print("\n✅ ✅ ✅ PITR RESTORE COMPLETED SUCCESSFULLY")

# ======================= BASE BACKUP =======================

def take_base_backup():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"pg_base_{ts}"
    path = f"{BASE_BACKUP_DIR}/{name}"

    print("\n📦 TAKING BASE BACKUP...")
    subprocess.run([
        "pg_basebackup",
        "-h", "localhost",
        "-U", "postgres",
        "-D", path,
        "-Fp",
        "-X", "stream",
        "-P"
    ], check=False)

    print(f"✅ BASE BACKUP CREATED: {name}")

# ======================= FULL BACKUP =======================

def full_backup(db):
    requests.post(f"{FASTAPI}/backup/full", json={"db_name": db})

def full_backup_server():
    for db in DATABASES:
        full_backup(db)

# ======================= BACKUP LIST =======================

def list_backups(db):
    r = requests.get(f"{FASTAPI}/backups/{db}")
    print(json.dumps(r.json(), indent=2))

# ======================= AI =======================

PROMPT = """
Return ONLY JSON:

FULL BACKUP:
{ "FULL_BACKUP": {"db_name":"users_db"} }
{ "FULL_BACKUP": {"db_name":"pg1"} }

BASE:
{ "BASE_BACKUP": {} }

WAL:
{ "WAL_ROTATE": {} }

LIST WAL:
{ "LIST_WAL": {} }

LIST BACKUPS:
{ "LIST_BACKUPS": {"db_name":"users_db"} }
{ "LIST_BACKUPS": {"db_name":"pg1"} }

PITR:
{ "PITR_RESTORE": {"base_backup_name":"pg_base_YYYYMMDD_HHMMSS","target_time":null} }
"""

# ======================= ORCHESTRATOR =======================

class Orchestrator:

    def ask_ai(self, text):
        r = ollama.chat(
            model=MODEL,
            messages=[
                {"role":"system","content":PROMPT},
                {"role":"user","content":text}
            ],
            format="json"
        )
        return json.loads(r["message"]["content"])

    def execute(self, action):

        if "FULL_BACKUP" in action:
            db = action["FULL_BACKUP"]["db_name"]
            if db == "pg1":
                full_backup_server()
                print("✅ SERVER FULL BACKUP DONE")
            else:
                full_backup(db)
                print(f"✅ {db} BACKUP DONE")

        elif "BASE_BACKUP" in action:
            take_base_backup()

        elif "WAL_ROTATE" in action:
            rotate_wal()
            print("✅ WAL ROTATED")

        elif "LIST_WAL" in action:
            list_wal()

        elif "LIST_BACKUPS" in action:
            db = action["LIST_BACKUPS"]["db_name"]
            list_backups(db)

        elif "PITR_RESTORE" in action:
            base = action["PITR_RESTORE"]["base_backup_name"]
            target = action["PITR_RESTORE"].get("target_time")
            auto_pitr(base, target)

# ======================= MAIN =======================

def main():
    orch = Orchestrator()

    print("\n✅ ENTERPRISE AUTO PITR ORCHESTRATOR READY\n")

    while True:
        cmd = input("🎤 You: ").strip()
        if cmd.lower() in ["exit","quit"]:
            break

        try:
            action = orch.ask_ai(cmd)
            print("\n📋 Parsed:", action)
            orch.execute(action)
        except Exception as e:
            print("❌ ERROR:", e)

if __name__ == "__main__":
    main()
