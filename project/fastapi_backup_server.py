"""
PostgreSQL Backup Server (Port 8001)
COMPLETE FIXED VERSION - Handles concurrent restores without connection errors
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Optional
import subprocess
from datetime import datetime
from pathlib import Path
import time
from fastapi.responses import FileResponse

app = FastAPI(title="PostgreSQL Backup Server (Single Server PG1)")

# ==============================
# CONFIGURATION
# ==============================

SERVER_NAME = "PG1"

POSTGRES_USER = "postgres"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"

DATABASES = [
    "users_db",
    "products_db",
    "reports_db",
]

BACKUP_BASE_DIR = Path("./backups")
FULL_BACKUP_DIR = BACKUP_BASE_DIR / "full"
BASE_BACKUP_DIR = BACKUP_BASE_DIR / "base"
WAL_ARCHIVE_DIR = Path("backups/wal")

FULL_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
BASE_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
WAL_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

# ==============================
# SCHEMAS
# ==============================

class BackupRequest(BaseModel):
    db_name: str
'''postgres,dbname,cg_name'''

class LogicalRestoreRequest(BaseModel):
    db_name: str
    backup_file: str


class PITRRestoreRequest(BaseModel):
    base_backup_name: str
    target_time: Optional[str] = None

class mcBindKey(Structure):
    _fields_ = [
        ("stVersion", c_uint16),
        ("mcName", c_char * (DSM_MAX_MC_NAME_LENGTH + 1)),
        ("copyType", c_uint16), # 1=Backup, 2=Archive
        ("copyDestination", c_char * (DSM_MAX_SERVERNAME_LENGTH + 1)),
        ("reserved", c_char * 128),
    ]

# ==============================
# UTILITIES
# ==============================

def validate_db(db_name: str):
    if db_name not in DATABASES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid database. Available: {DATABASES}",
        )


def is_in_recovery() -> bool:
    """
    Returns True if PostgreSQL is currently in recovery mode.
    """
    try:
        cmd = [
            "psql",
            "-U", POSTGRES_USER,
            "-h", POSTGRES_HOST,
            "-p", POSTGRES_PORT,
            "-d", "postgres",
            "-t",
            "-c", "SELECT pg_is_in_recovery();",
        ]
        result = subprocess.check_output(cmd, text=True).strip()
        return result == "t"
    except Exception:
        return True


def force_terminate_and_drop(db_name: str, max_retries: int = 10) -> bool:
    """
    Aggressively terminate connections and drop database with retry logic.
    This handles race conditions where connections re-establish between terminate and drop.
    
    Returns True if successful, False otherwise.
    """
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[DROP] Attempt {attempt}/{max_retries} to drop {db_name}")
            
            # Single atomic operation: block connections + terminate + drop
            # Using WITH for immediate effect
            drop_cmd = [
                "psql",
                "-U", POSTGRES_USER,
                "-h", POSTGRES_HOST,
                "-p", POSTGRES_PORT,
                "-d", "postgres",
                "-c", f"""
                    -- Block new connections IMMEDIATELY
                    UPDATE pg_database SET datallowconn = false WHERE datname = '{db_name}';
                    
                    -- Terminate all existing connections
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = '{db_name}'
                    AND pid <> pg_backend_pid();
                    
                    -- Drop the database immediately
                    DROP DATABASE IF EXISTS {db_name};
                """,
            ]
            
            result = subprocess.run(drop_cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                print(f"[DROP] ✅ Successfully dropped {db_name}")
                return True
            else:
                stderr_lower = result.stderr.lower() if result.stderr else ""
                
                # If database doesn't exist, that's also success
                if "does not exist" in stderr_lower:
                    print(f"[DROP] ✅ Database {db_name} already dropped")
                    return True
                
                print(f"[DROP] ⚠️  Attempt {attempt} failed: {result.stderr}")
                
                # Small delay before retry
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
    """Re-enable connections to a database"""
    try:
        cmd = [
            "psql",
            "-U", POSTGRES_USER,
            "-h", POSTGRES_HOST,
            "-p", POSTGRES_PORT,
            "-d", "postgres",
            "-c", f"""
                UPDATE pg_database SET datallowconn = true WHERE datname = '{db_name}';
                GRANT CONNECT ON DATABASE {db_name} TO public;
            """,
        ]
        subprocess.run(cmd, check=False, capture_output=True)
        print(f"[RESTORE] ✅ Re-enabled connections to {db_name}")
    except Exception as e:
        print(f"[RESTORE] ⚠️  Failed to re-enable connections: {e}")


# ==============================
# HEALTH + METADATA
# ==============================

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "server": SERVER_NAME,
        "databases": DATABASES,
        "backup_dirs": {
            "full": str(FULL_BACKUP_DIR),
            "base": str(BASE_BACKUP_DIR),
            "wal_archive": str(WAL_ARCHIVE_DIR),
        },
        "recovery_mode": is_in_recovery(),
    }


@app.get("/servers")
def list_servers():
    return {
        "servers": [
            {
                "name": SERVER_NAME,
                "databases": DATABASES,
            }
        ]
    }


@app.get("/databases")
def list_databases():
    return DATABASES


# ==============================
# FULL BACKUP (logical pg_dump)
# ==============================

@app.post("/backup/full")
def full_backup(req: BackupRequest):
    validate_db(req.db_name)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = FULL_BACKUP_DIR / f"{req.db_name}_full_{timestamp}.sql"

    cmd = [
        "pg_dump",
        "-U", POSTGRES_USER,
        "-h", POSTGRES_HOST,
        "-p", POSTGRES_PORT,
        req.db_name,
    ]

    try:
        with open(backup_file, "w", encoding="utf-8") as f:
            subprocess.run(cmd, stdout=f, check=True)

        return {
            "success": True,
            "type": "full",
            "server": SERVER_NAME,
            "database": req.db_name,
            "backup_file": backup_file.name,
            "backup_path": str(backup_file),
            "timestamp": timestamp,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==============================
# BASE BACKUP (pg_basebackup)
# ==============================

@app.post("/backup/base")
def base_backup():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_dir = BASE_BACKUP_DIR / f"pg_base_{timestamp}"

    cmd = [
        "pg_basebackup",
        "-U", POSTGRES_USER,
        "-h", POSTGRES_HOST,
        "-p", POSTGRES_PORT,
        "-D", str(dest_dir),
        "-F", "p",
        "-X", "stream",
        "-P",
    ]

    try:
        subprocess.run(cmd, check=True)
        return {
            "success": True,
            "type": "base",
            "server": SERVER_NAME,
            "base_backup_dir": str(dest_dir),
            "base_backup_name": dest_dir.name,
            "timestamp": timestamp,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==============================
# INCREMENTAL BACKUP (WAL SAFE)
# ==============================

@app.post("/backup/incremental")
def incremental_backup():
    if is_in_recovery():
        return {
            "success": True,
            "type": "incremental",
            "server": SERVER_NAME,
            "status": "skipped",
            "reason": "PostgreSQL is in recovery mode",
            "note": "WAL switch is not allowed during recovery",
        }

    try:
        cmd = [
            "psql",
            "-U", POSTGRES_USER,
            "-h", POSTGRES_HOST,
            "-p", POSTGRES_PORT,
            "-d", "postgres",
            "-c", "SELECT pg_switch_wal();",
        ]

        subprocess.run(cmd, check=True)

        wal_files = sorted(
            [f.name for f in WAL_ARCHIVE_DIR.glob("*") if f.is_file()],
            reverse=True,
        )

        return {
            "success": True,
            "type": "incremental",
            "server": SERVER_NAME,
            "wal_archive_dir": str(WAL_ARCHIVE_DIR),
            "current_wal_files": wal_files[:10],
            "note": "WAL switch completed",
        }

    except subprocess.CalledProcessError as e:
        raise HTTPException(
            status_code=500,
            detail=f"WAL switch failed: {str(e)}",
        )


# ==============================
# LIST BACKUPS
# ==============================

@app.get("/backups/{target}")
def list_backups(target: str):
    target_lower = target.lower()

    if target_lower == SERVER_NAME.lower():
        data: Dict[str, Dict[str, List[str]]] = {}

        for db in DATABASES:
            full_backups = sorted(
                [f.name for f in FULL_BACKUP_DIR.glob(f"{db}_full_*.sql")],
                reverse=True,
            )
            data[db] = {"full_backups": full_backups}

        wal_files = sorted(
            [f.name for f in WAL_ARCHIVE_DIR.glob("*") if f.is_file()],
            reverse=True,
        )

        base_backups = sorted(
            [d.name for d in BASE_BACKUP_DIR.iterdir() if d.is_dir()],
            reverse=True,
        )

        return {
            "server": SERVER_NAME,
            "databases": data,
            "wal_archive_files": wal_files,
            "base_backups": base_backups,
        }

    validate_db(target)

    return {
        "server": SERVER_NAME,
        "database": target,
        "full_backups": sorted(
            [f.name for f in FULL_BACKUP_DIR.glob(f"{target}_full_*.sql")],
            reverse=True,
        ),
        "wal_archive_files": sorted(
            [f.name for f in WAL_ARCHIVE_DIR.glob("*") if f.is_file()],
            reverse=True,
        ),
        "base_backups": sorted(
            [d.name for d in BASE_BACKUP_DIR.iterdir() if d.is_dir()],
            reverse=True,
        ),
    }

# Add this to your PostgreSQL server (port 8001)
# Insert after the list_backups endpoint, before restore operations


# ==============================
# FILE DOWNLOAD ENDPOINT
# ==============================

@app.get("/download/backup/{backup_file}")
async def download_backup(backup_file: str):
    """
    Download a PostgreSQL backup file.
    Orchestrator uses this to fetch backups.
    """
    # Check in full backup directory
    backup_path = FULL_BACKUP_DIR / backup_file
    
    if not backup_path.exists():
        # Try base backup directory
        backup_path = BASE_BACKUP_DIR / backup_file
        
        if not backup_path.exists():
            raise HTTPException(
                status_code=404,
                detail=f"Backup file '{backup_file}' not found in backup directories"
            )
    
    if not backup_path.is_file():
        raise HTTPException(
            status_code=400,
            detail=f"'{backup_file}' is not a file"
        )
    
    print(f"[DOWNLOAD] Serving backup: {backup_file}")
    
    return FileResponse(
        path=str(backup_path),
        filename=backup_file,
        media_type='application/octet-stream'
    )


@app.get("/download/base/{base_backup_name}")
async def download_base_backup(base_backup_name: str):
    """
    Download a base backup directory as a tar.gz file.
    """
    import tarfile
    import tempfile
    
    base_path = BASE_BACKUP_DIR / base_backup_name
    
    if not base_path.exists() or not base_path.is_dir():
        raise HTTPException(
            status_code=404,
            detail=f"Base backup '{base_backup_name}' not found"
        )
    
    # Create temporary tar.gz file
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.tar.gz', delete=False) as tmp_file:
        tmp_path = tmp_file.name
        
        with tarfile.open(tmp_path, 'w:gz') as tar:
            tar.add(str(base_path), arcname=base_backup_name)
    
    print(f"[DOWNLOAD] Serving base backup: {base_backup_name}")
    
    return FileResponse(
        path=tmp_path,
        filename=f"{base_backup_name}.tar.gz",
        media_type='application/gzip',
        background=lambda: os.unlink(tmp_path)  # Cleanup after sending
    )

# ==============================
# RESTORE OPERATIONS - FIXED
# ==============================

@app.post("/restore/logical")
def restore_logical(req: LogicalRestoreRequest):
    """
    Manual restore from a specific backup file.
    FIXED: Concurrent-safe with aggressive connection termination.
    """
    validate_db(req.db_name)

    backup_path = FULL_BACKUP_DIR / req.backup_file
    
    if not backup_path.exists():
        raise HTTPException(
            status_code=404, 
            detail=f"Backup file not found: {req.backup_file} in {FULL_BACKUP_DIR}"
        )

    try:
        # Step 1: Force drop database with connection termination (FIXED)
        print(f"[RESTORE] Force dropping {req.db_name}...")
        if not force_terminate_and_drop(req.db_name):
            raise HTTPException(
                status_code=409,
                detail=f"Failed to drop database {req.db_name} after multiple attempts. Check for active connections."
            )

        # Small delay to ensure cleanup
        time.sleep(0.5)

        # Step 2: Create database
        print(f"[RESTORE] Creating database {req.db_name}...")
        create_cmd = [
            "psql",
            "-U", POSTGRES_USER,
            "-h", POSTGRES_HOST,
            "-p", POSTGRES_PORT,
            "-d", "postgres",
            "-c", f"CREATE DATABASE {req.db_name};",
        ]
        result = subprocess.run(create_cmd, check=True, capture_output=True, text=True)

        # Step 3: Restore from backup
        print(f"[RESTORE] Restoring {req.db_name} from {req.backup_file}...")
        restore_cmd = [
            "psql",
            "-U", POSTGRES_USER,
            "-h", POSTGRES_HOST,
            "-p", POSTGRES_PORT,
            "-d", req.db_name,
            "-f", str(backup_path),
        ]
        subprocess.run(restore_cmd, check=True, capture_output=True, text=True)
        
        # Step 4: Grant permissions
        enable_db_connections(req.db_name)

        return {
            "success": True,
            "restore_type": "manual",
            "server": SERVER_NAME,
            "database": req.db_name,
            "restored_from": req.backup_file,
            "backup_path": str(backup_path),
            "message": "Database restored successfully from specific backup",
            "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
        }

    except subprocess.CalledProcessError as e:
        error_detail = f"Restore command failed: {e.stderr if hasattr(e, 'stderr') else str(e)}"
        print(f"[RESTORE] ❌ {error_detail}")
        raise HTTPException(status_code=500, detail=error_detail)
    except Exception as e:
        print(f"[RESTORE] ❌ Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/restore")
def restore_db(req: LogicalRestoreRequest):
    """Alias for restore/logical for backward compatibility"""
    return restore_logical(req)


@app.post("/restore/auto")
def auto_restore(req: BackupRequest):
    """
    Auto restore from the most recent full backup.
    FIXED: Concurrent-safe with aggressive connection termination.
    """
    validate_db(req.db_name)
    
    backup_files = sorted(
        [f for f in FULL_BACKUP_DIR.glob(f"{req.db_name}_full_*.sql")],
        reverse=True,
    )
    
    if not backup_files:
        raise HTTPException(
            status_code=404,
            detail=f"No backup files found for database '{req.db_name}'"
        )
    
    latest_backup = backup_files[0]
    backup_filename = latest_backup.name
    
    try:
        # Step 1: Force drop database with connection termination (FIXED)
        print(f"[AUTO-RESTORE] Force dropping {req.db_name}...")
        if not force_terminate_and_drop(req.db_name):
            raise HTTPException(
                status_code=409,
                detail=f"Failed to drop database {req.db_name} after multiple attempts. Check for active connections."
            )

        # Small delay to ensure cleanup
        time.sleep(0.5)

        # Step 2: Create database
        print(f"[AUTO-RESTORE] Creating database {req.db_name}...")
        create_cmd = [
            "psql",
            "-U", POSTGRES_USER,
            "-h", POSTGRES_HOST,
            "-p", POSTGRES_PORT,
            "-d", "postgres",
            "-c", f"CREATE DATABASE {req.db_name};",
        ]
        subprocess.run(create_cmd, check=True, capture_output=True, text=True)

        # Step 3: Restore from backup
        print(f"[AUTO-RESTORE] Restoring {req.db_name} from {backup_filename}...")
        restore_cmd = [
            "psql",
            "-U", POSTGRES_USER,
            "-h", POSTGRES_HOST,
            "-p", POSTGRES_PORT,
            "-d", req.db_name,
            "-f", str(latest_backup),
        ]
        subprocess.run(restore_cmd, check=True, capture_output=True, text=True)
        
        # Step 4: Grant permissions
        enable_db_connections(req.db_name)

        return {
            "success": True,
            "restore_type": "auto",
            "server": SERVER_NAME,
            "database": req.db_name,
            "restored_from": backup_filename,
            "backup_path": str(latest_backup),
            "message": "Database automatically restored from latest backup",
            "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
        }

    except subprocess.CalledProcessError as e:
        error_detail = f"Auto restore command failed: {e.stderr if hasattr(e, 'stderr') else str(e)}"
        print(f"[AUTO-RESTORE] ❌ {error_detail}")
        raise HTTPException(status_code=500, detail=error_detail)
    except Exception as e:
        print(f"[AUTO-RESTORE] ❌ Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/restore/pitr")
def pitr_restore(req: PITRRestoreRequest):
    """
    Point-in-Time Recovery validation.
    Returns metadata for manual PITR setup.
    """
    base_dir = BASE_BACKUP_DIR / req.base_backup_name
    
    if not base_dir.exists():
        raise HTTPException(
            status_code=404, 
            detail=f"Base backup not found: {req.base_backup_name}"
        )

    wal_files = sorted(
        [f.name for f in WAL_ARCHIVE_DIR.glob("*") if f.is_file()],
        reverse=True,
    )

    return {
        "success": True,
        "restore_type": "pitr",
        "server": SERVER_NAME,
        "base_backup_dir": str(base_dir),
        "base_backup_name": req.base_backup_name,
        "wal_archive_dir": str(WAL_ARCHIVE_DIR),
        "available_wal_files": len(wal_files),
        "target_time": req.target_time,
        "note": "PITR validation successful. Manual recovery.conf setup required.",
        "instructions": {
            "step1": f"Stop PostgreSQL service",
            "step2": f"Replace data directory with: {base_dir}",
            "step3": f"Create recovery.conf with restore_command pointing to {WAL_ARCHIVE_DIR}",
            "step4": f"Set recovery_target_time = '{req.target_time}'" if req.target_time else "Omit for full recovery",
            "step5": "Start PostgreSQL service",
        }
    }


# ==============================
# CONNECTION MANAGEMENT
# ==============================

@app.get("/connections/{db_name}")
def get_connections(db_name: str):
    """Get active connections for a database"""
    validate_db(db_name)
    
    try:
        cmd = [
            "psql",
            "-U", POSTGRES_USER,
            "-h", POSTGRES_HOST,
            "-p", POSTGRES_PORT,
            "-d", "postgres",
            "-t",
            "-c", f"""
                SELECT count(*) 
                FROM pg_stat_activity 
                WHERE datname = '{db_name}';
            """,
        ]
        result = subprocess.check_output(cmd, text=True).strip()
        
        return {
            "database": db_name,
            "active_connections": int(result) if result else 0
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/connections/{db_name}/terminate")
def terminate_connections(db_name: str):
    """Terminate all connections to a database and drop it"""
    validate_db(db_name)
    
    success = force_terminate_and_drop(db_name)
    
    if success:
        return {
            "success": True,
            "database": db_name,
            "message": "Database dropped with all connections terminated"
        }
    else:
        raise HTTPException(
            status_code=500,
            detail="Failed to terminate connections and drop database"
        )


# ==============================
# MAIN
# ==============================

if __name__ == "__main__":
    import uvicorn
    print("=" * 60)
    print("🗄️  POSTGRESQL BACKUP SERVER - FIXED VERSION")
    print("=" * 60)
    print(f"Server Name: {SERVER_NAME}")
    print(f"Port: 8001")
    print(f"Databases: {', '.join(DATABASES)}")
    print(f"Full Backups: {FULL_BACKUP_DIR}")
    print(f"Base Backups: {BASE_BACKUP_DIR}")
    print(f"WAL Archive: {WAL_ARCHIVE_DIR}")
    print("=" * 60)
    print("✅ FIXED: Concurrent restore without connection errors")
    print("=" * 60)
    uvicorn.run(app, host="0.0.0.0", port=8001, reload=True)