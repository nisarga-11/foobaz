"""
PostgreSQL Backup Server (Port 8001)
FILE: fastapi_backup_server.py
CORRECTED: Unified API with CG metadata support
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import subprocess
from datetime import datetime
from pathlib import Path
import time

app = FastAPI(title="PostgreSQL Backup Server")

# ==============================
# CONFIGURATION
# ==============================
SERVER_NAME = "PG1"
POSTGRES_USER = "postgres"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"

DATABASES = ["users_db", "products_db", "reports_db"]

BACKUP_BASE_DIR = Path("./backups")
FULL_BACKUP_DIR = BACKUP_BASE_DIR / "full"
BASE_BACKUP_DIR = BACKUP_BASE_DIR / "base"
WAL_ARCHIVE_DIR = Path("backups/wal")

FULL_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
BASE_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
WAL_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

# ==============================
# UNIFIED REQUEST MODELS
# ==============================
class BackupRequest(BaseModel):
    db_name: Optional[str] = None  # Required for full backups
    cg_id: Optional[str] = None  # Consistency Group ID
    cg_name: Optional[str] = None  # Consistency Group Name
    backup_id: Optional[str] = None  # Orchestrator's backup ID

class LogicalRestoreRequest(BaseModel):
    db_name: str
    backup_file: str

class PITRRestoreRequest(BaseModel):
    base_backup_name: str
    target_time: Optional[str] = None

# ==============================
# UTILITIES
# ==============================
def validate_db(db_name: str):
    if db_name not in DATABASES:
        raise HTTPException(400, f"Invalid database. Available: {DATABASES}")

def is_in_recovery() -> bool:
    try:
        cmd = ["psql", "-U", POSTGRES_USER, "-h", POSTGRES_HOST, "-p", POSTGRES_PORT,
               "-d", "postgres", "-t", "-c", "SELECT pg_is_in_recovery();"]
        result = subprocess.check_output(cmd, text=True).strip()
        return result == "t"
    except:
        return True

def force_terminate_and_drop(db_name: str, max_retries: int = 10) -> bool:
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[DROP] Attempt {attempt}/{max_retries} to drop {db_name}")
            drop_cmd = ["psql", "-U", POSTGRES_USER, "-h", POSTGRES_HOST, "-p", POSTGRES_PORT,
                       "-d", "postgres", "-c", f"""
                    UPDATE pg_database SET datallowconn = false WHERE datname = '{db_name}';
                    SELECT pg_terminate_backend(pid) FROM pg_stat_activity 
                    WHERE datname = '{db_name}' AND pid <> pg_backend_pid();
                    DROP DATABASE IF EXISTS {db_name};
                """]
            result = subprocess.run(drop_cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                print(f"[DROP] ✅ Successfully dropped {db_name}")
                return True
            elif "does not exist" in (result.stderr or "").lower():
                print(f"[DROP] ✅ Database {db_name} already dropped")
                return True
            
            print(f"[DROP] ⚠️  Attempt {attempt} failed: {result.stderr}")
            time.sleep(0.5)
        except subprocess.TimeoutExpired:
            print(f"[DROP] ⏱️  Attempt {attempt} timed out")
            time.sleep(0.5)
        except Exception as e:
            print(f"[DROP] ❌ Attempt {attempt} error: {str(e)}")
            time.sleep(0.5)
    
    print(f"[DROP] ❌ Failed to drop {db_name} after {max_retries} attempts")
    return False

def enable_db_connections(db_name: str):
    try:
        cmd = ["psql", "-U", POSTGRES_USER, "-h", POSTGRES_HOST, "-p", POSTGRES_PORT,
               "-d", "postgres", "-c", f"""
                UPDATE pg_database SET datallowconn = true WHERE datname = '{db_name}';
                GRANT CONNECT ON DATABASE {db_name} TO public;
            """]
        subprocess.run(cmd, check=False, capture_output=True)
        print(f"[RESTORE] ✅ Re-enabled connections to {db_name}")
    except Exception as e:
        print(f"[RESTORE] ⚠️  Failed to re-enable connections: {e}")

# ==============================
# UNIFIED STATUS ENDPOINTS
# ==============================
@app.get("/health")
def health():
    return {
        "status": "healthy",
        "service": "postgres",
        "type": "postgres",
        "server": SERVER_NAME,
        "databases": DATABASES,
        "recovery_mode": is_in_recovery()
    }

@app.get("/")
def root():
    return {
        "service": "PostgreSQL Backup Server",
        "type": "postgres",
        "server": SERVER_NAME,
        "port": 8001,
        "databases": DATABASES,
        "endpoints": {
            "backup": "POST /backup/{type}",
            "restore_logical": "POST /restore/logical",
            "restore_auto": "POST /restore/auto",
            "list_backups": "GET /backups/{db_name}"
        }
    }

@app.get("/databases")
def list_databases():
    return {"type": "postgres", "databases": DATABASES, "total": len(DATABASES)}

# ==============================
# UNIFIED BACKUP ENDPOINT
# ==============================
@app.post("/backup/{backup_type}")
def unified_backup(backup_type: str, req: BackupRequest):
    """
    Unified backup endpoint: /backup/full, /backup/base, /backup/incremental
    Includes CG metadata (cg_id, cg_name, backup_id) in request
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Add CG metadata to response if provided
    cg_metadata = {}
    if req.cg_id:
        cg_metadata["cg_id"] = req.cg_id
    if req.cg_name:
        cg_metadata["cg_name"] = req.cg_name
    if req.backup_id:
        cg_metadata["backup_id"] = req.backup_id
    
    if backup_type == "full":
        if not req.db_name:
            raise HTTPException(400, "db_name required for full backup")
        
        validate_db(req.db_name)
        
        # Generate backup filename with optional CG info
        filename_parts = [req.db_name, "full", timestamp]
        if req.cg_name:
            filename_parts.insert(1, req.cg_name)
        backup_file = FULL_BACKUP_DIR / f"{'_'.join(filename_parts)}.sql"
        
        cmd = ["pg_dump", "-U", POSTGRES_USER, "-h", POSTGRES_HOST, 
               "-p", POSTGRES_PORT, req.db_name]
        
        try:
            with open(backup_file, "w", encoding="utf-8") as f:
                subprocess.run(cmd, stdout=f, check=True)
            
            return {
                "success": True,
                "type": "postgres",
                "backup_type": "full",
                "server": SERVER_NAME,
                "database": req.db_name,
                "backup_file": backup_file.name,
                "backup_path": str(backup_file),
                "timestamp": timestamp,
                **cg_metadata
            }
        except Exception as e:
            raise HTTPException(500, str(e))
    
    elif backup_type == "base":
        # Generate directory name with optional CG info
        dir_parts = ["pg_base", timestamp]
        if req.cg_name:
            dir_parts.insert(1, req.cg_name)
        dest_dir = BASE_BACKUP_DIR / '_'.join(dir_parts)
        
        cmd = ["pg_basebackup", "-U", POSTGRES_USER, "-h", POSTGRES_HOST,
               "-p", POSTGRES_PORT, "-D", str(dest_dir), "-F", "p", "-X", "stream", "-P"]
        
        try:
            subprocess.run(cmd, check=True)
            return {
                "success": True,
                "type": "postgres",
                "backup_type": "base",
                "server": SERVER_NAME,
                "base_backup_dir": str(dest_dir),
                "base_backup_name": dest_dir.name,
                "backup_file": dest_dir.name,
                "timestamp": timestamp,
                **cg_metadata
            }
        except Exception as e:
            raise HTTPException(500, str(e))
    
    elif backup_type == "incremental":
        if is_in_recovery():
            return {
                "success": True,
                "type": "postgres",
                "backup_type": "incremental",
                "server": SERVER_NAME,
                "status": "skipped",
                "reason": "PostgreSQL is in recovery mode",
                **cg_metadata
            }
        
        try:
            cmd = ["psql", "-U", POSTGRES_USER, "-h", POSTGRES_HOST, "-p", POSTGRES_PORT,
                   "-d", "postgres", "-c", "SELECT pg_switch_wal();"]
            subprocess.run(cmd, check=True)
            
            wal_files = sorted([f.name for f in WAL_ARCHIVE_DIR.glob("*") if f.is_file()], reverse=True)
            
            return {
                "success": True,
                "type": "postgres",
                "backup_type": "incremental",
                "server": SERVER_NAME,
                "wal_archive_dir": str(WAL_ARCHIVE_DIR),
                "current_wal_files": wal_files[:10],
                "timestamp": timestamp,
                **cg_metadata
            }
        except subprocess.CalledProcessError as e:
            raise HTTPException(500, f"WAL switch failed: {str(e)}")
    
    else:
        raise HTTPException(400, f"Invalid backup type: {backup_type}. Use: full, base, incremental")

# ==============================
# LIST BACKUPS
# ==============================
@app.get("/backups/{db_name}")
def list_backups(db_name: str):
    validate_db(db_name)
    
    full_backups = sorted([f.name for f in FULL_BACKUP_DIR.glob(f"{db_name}_*.sql") 
                          if f.is_file()], reverse=True)
    base_backups = sorted([d.name for d in BASE_BACKUP_DIR.iterdir() if d.is_dir()], reverse=True)
    wal_files = sorted([f.name for f in WAL_ARCHIVE_DIR.glob("*") if f.is_file()], reverse=True)
    
    return {
        "type": "postgres",
        "server": SERVER_NAME,
        "database": db_name,
        "full_backups": full_backups,
        "base_backups": base_backups,
        "wal_archive_files": wal_files[:20],
        "total_full": len(full_backups),
        "total_base": len(base_backups),
        "total_wal": len(wal_files)
    }

# ==============================
# RESTORE OPERATIONS
# ==============================
@app.post("/restore/logical")
def restore_logical(req: LogicalRestoreRequest):
    validate_db(req.db_name)
    backup_path = FULL_BACKUP_DIR / req.backup_file
    
    if not backup_path.exists():
        raise HTTPException(404, f"Backup file not found: {req.backup_file}")
    
    try:
        print(f"[RESTORE] Force dropping {req.db_name}...")
        if not force_terminate_and_drop(req.db_name):
            raise HTTPException(409, f"Failed to drop database {req.db_name}")
        
        time.sleep(0.5)
        
        print(f"[RESTORE] Creating database {req.db_name}...")
        create_cmd = ["psql", "-U", POSTGRES_USER, "-h", POSTGRES_HOST, "-p", POSTGRES_PORT,
                     "-d", "postgres", "-c", f"CREATE DATABASE {req.db_name};"]
        subprocess.run(create_cmd, check=True, capture_output=True, text=True)
        
        print(f"[RESTORE] Restoring {req.db_name} from {req.backup_file}...")
        restore_cmd = ["psql", "-U", POSTGRES_USER, "-h", POSTGRES_HOST, "-p", POSTGRES_PORT,
                      "-d", req.db_name, "-f", str(backup_path)]
        subprocess.run(restore_cmd, check=True, capture_output=True, text=True)
        
        enable_db_connections(req.db_name)
        
        return {
            "success": True,
            "type": "postgres",
            "restore_type": "logical",
            "server": SERVER_NAME,
            "database": req.db_name,
            "restored_from": req.backup_file,
            "message": "Database restored successfully",
            "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S")
        }
    except subprocess.CalledProcessError as e:
        raise HTTPException(500, f"Restore failed: {e.stderr if hasattr(e, 'stderr') else str(e)}")
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/restore/auto")
def auto_restore(req: BackupRequest):
    if not req.db_name:
        raise HTTPException(400, "db_name required")
    
    validate_db(req.db_name)
    
    backup_files = sorted([f for f in FULL_BACKUP_DIR.glob(f"{req.db_name}_*.sql")], reverse=True)
    
    if not backup_files:
        raise HTTPException(404, f"No backup files found for database '{req.db_name}'")
    
    latest_backup = backup_files[0]
    
    try:
        print(f"[AUTO-RESTORE] Force dropping {req.db_name}...")
        if not force_terminate_and_drop(req.db_name):
            raise HTTPException(409, f"Failed to drop database {req.db_name}")
        
        time.sleep(0.5)
        
        print(f"[AUTO-RESTORE] Creating database {req.db_name}...")
        create_cmd = ["psql", "-U", POSTGRES_USER, "-h", POSTGRES_HOST, "-p", POSTGRES_PORT,
                     "-d", "postgres", "-c", f"CREATE DATABASE {req.db_name};"]
        subprocess.run(create_cmd, check=True, capture_output=True, text=True)
        
        print(f"[AUTO-RESTORE] Restoring {req.db_name}...")
        restore_cmd = ["psql", "-U", POSTGRES_USER, "-h", POSTGRES_HOST, "-p", POSTGRES_PORT,
                      "-d", req.db_name, "-f", str(latest_backup)]
        subprocess.run(restore_cmd, check=True, capture_output=True, text=True)
        
        enable_db_connections(req.db_name)
        
        return {
            "success": True,
            "type": "postgres",
            "restore_type": "auto",
            "server": SERVER_NAME,
            "database": req.db_name,
            "restored_from": latest_backup.name,
            "message": "Database automatically restored from latest backup",
            "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S")
        }
    except subprocess.CalledProcessError as e:
        raise HTTPException(500, f"Auto restore failed: {e.stderr if hasattr(e, 'stderr') else str(e)}")
    except Exception as e:
        raise HTTPException(500, str(e))

# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    import uvicorn
    print("="*60)
    print("🗄️  POSTGRESQL BACKUP SERVER (UNIFIED API)")
    print("="*60)
    print(f"Type: postgres")
    print(f"Server: {SERVER_NAME}")
    print(f"Port: 8001")
    print(f"Databases: {', '.join(DATABASES)}")
    print(f"Unified Backup API: POST /backup/{{type}}")
    print(f"  - /backup/full (requires db_name)")
    print(f"  - /backup/base")
    print(f"  - /backup/incremental")
    print("="*60)
    uvicorn.run(app, host="0.0.0.0", port=8001)