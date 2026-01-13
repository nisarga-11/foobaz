"""
Combined FastAPI Server for S3 Log Parser and PostgreSQL Backup
"""
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from datetime import datetime
from pathlib import Path
import os
import zipfile
import io
import subprocess

from timebasedparser_fastapi import S3LogParser, OperationSummary

app = FastAPI(
    title="Combined S3 Log Parser & PostgreSQL Backup API",
    description="Parse S3 logs, mirror operations, and manage PostgreSQL backups",
    version="1.0.0"
)

# ==============================
# S3 LOG PARSER INITIALIZATION
# ==============================
parser = S3LogParser()

# ==============================
# POSTGRESQL CONFIGURATION
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
# REQUEST MODELS
# ==============================

# S3 Models
class TimeRangeRequest(BaseModel):
    start_time: str = Field(..., example="06/Nov/2025:04:00:00", description="Start time in format DD/MMM/YYYY:HH:MM:SS")
    end_time: str = Field(..., example="06/Nov/2025:05:00:00", description="End time in format DD/MMM/YYYY:HH:MM:SS")
    operation_filter: Optional[str] = Field("ALL", example="PUT", description="Operation type: PUT, GET, DELETE, or ALL")

class StatusResponse(BaseModel):
    status: str
    message: str
    details: Optional[dict] = None

# PostgreSQL Models
class BackupRequest(BaseModel):
    db_name: str

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
        raise HTTPException(
            status_code=400,
            detail=f"Invalid database. Available: {DATABASES}",
        )

# ==============================
# MAIN HEALTH CHECK
# ==============================

@app.get("/", response_model=StatusResponse)
async def root():
    """Health check endpoint for combined services"""
    return StatusResponse(
        status="online",
        message="Combined S3 Log Parser & PostgreSQL Backup API is running",
        details={
            "version": "1.0.0",
            "services": ["S3 Log Parser", "PostgreSQL Backup"],
            "s3_endpoints": ["/parse", "/status", "/download", "/files"],
            "postgres_endpoints": ["/backup/full", "/backup/base", "/backup/incremental", "/restore/logical", "/restore/pitr"]
        }
    )

# ==============================
# S3 LOG PARSER ENDPOINTS
# ==============================

@app.post("/parse", response_model=StatusResponse)
async def parse_logs(request: TimeRangeRequest, background_tasks: BackgroundTasks):
    """
    Parse S3 logs and mirror operations in the specified time range
    
    - **start_time**: Start timestamp (format: DD/MMM/YYYY:HH:MM:SS)
    - **end_time**: End timestamp (format: DD/MMM/YYYY:HH:MM:SS)
    - **operation_filter**: Filter by operation type (PUT, GET, DELETE, or ALL)
    """
    try:
        time_fmt = "%d/%b/%Y:%H:%M:%S"
        try:
            start_dt = datetime.strptime(request.start_time, time_fmt)
            end_dt = datetime.strptime(request.end_time, time_fmt)
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid time format. Use DD/MMM/YYYY:HH:MM:SS. Error: {str(e)}"
            )
        
        if start_dt >= end_dt:
            raise HTTPException(
                status_code=400,
                detail="Start time must be before end time"
            )
        
        valid_operations = ["ALL", "PUT", "GET", "DELETE"]
        operation = request.operation_filter.upper()
        if operation not in valid_operations:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid operation. Must be one of: {', '.join(valid_operations)}"
            )
        
        summary = parser.parse_and_mirror(
            request.start_time,
            request.end_time,
            operation
        )
        
        return StatusResponse(
            status="success",
            message=f"Successfully processed {summary.total_operations} operation(s)",
            details={
                "time_range": f"{request.start_time} → {request.end_time}",
                "operation_filter": operation,
                "logs_downloaded": summary.logs_downloaded,
                "total_operations": summary.total_operations,
                "put_operations": summary.put_operations,
                "get_operations": summary.get_operations,
                "delete_operations": summary.delete_operations,
                "files_remaining": summary.files_remaining,
                "remaining_files": summary.remaining_file_list,
                "log_file": summary.log_file_path,
                "download_dir": summary.download_dir
            }
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing logs: {str(e)}"
        )

@app.get("/status", response_model=StatusResponse)
async def get_status():
    """Get current status of downloads directory"""
    try:
        download_dir = parser.download_dir
        
        if not os.path.exists(download_dir):
            return StatusResponse(
                status="empty",
                message="Downloads directory does not exist",
                details={"download_dir": download_dir}
            )
        
        files = [f for f in os.listdir(download_dir) if os.path.isfile(os.path.join(download_dir, f))]
        
        file_details = []
        for file_name in files:
            file_path = os.path.join(download_dir, file_name)
            stat = os.stat(file_path)
            file_details.append({
                "name": file_name,
                "size": stat.st_size,
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
            })
        
        return StatusResponse(
            status="success",
            message=f"Found {len(files)} file(s) in downloads directory",
            details={
                "download_dir": download_dir,
                "file_count": len(files),
                "files": file_details
            }
        )
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error getting status: {str(e)}"
        )

@app.get("/files", response_class=JSONResponse)
async def list_files():
    """List all files in downloads directory"""
    try:
        download_dir = parser.download_dir
        
        if not os.path.exists(download_dir):
            return {"files": [], "count": 0}
        
        files = [f for f in os.listdir(download_dir) if os.path.isfile(os.path.join(download_dir, f))]
        
        return {
            "files": files,
            "count": len(files),
            "download_dir": download_dir
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error listing files: {str(e)}"
        )

@app.get("/download/{filename}")
async def download_file(filename: str):
    """Download a specific file from downloads directory"""
    try:
        file_path = os.path.join(parser.download_dir, filename)
        
        if not os.path.exists(file_path):
            raise HTTPException(
                status_code=404,
                detail=f"File '{filename}' not found"
            )
        
        if not os.path.isfile(file_path):
            raise HTTPException(
                status_code=400,
                detail=f"'{filename}' is not a file"
            )
        
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type="application/octet-stream"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error downloading file: {str(e)}"
        )

@app.get("/download-all")
async def download_all_files():
    """Download all files as a ZIP archive"""
    try:
        download_dir = parser.download_dir
        
        if not os.path.exists(download_dir):
            raise HTTPException(
                status_code=404,
                detail="Downloads directory does not exist"
            )
        
        files = [f for f in os.listdir(download_dir) if os.path.isfile(os.path.join(download_dir, f))]
        
        if not files:
            raise HTTPException(
                status_code=404,
                detail="No files available for download"
            )
        
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for file_name in files:
                file_path = os.path.join(download_dir, file_name)
                zip_file.write(file_path, arcname=file_name)
        
        zip_buffer.seek(0)
        
        return FileResponse(
            path=zip_buffer,
            filename="s3_downloads.zip",
            media_type="application/zip"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error creating ZIP: {str(e)}"
        )

@app.delete("/files/{filename}")
async def delete_file(filename: str):
    """Delete a specific file from downloads directory"""
    try:
        file_path = os.path.join(parser.download_dir, filename)
        
        if not os.path.exists(file_path):
            raise HTTPException(
                status_code=404,
                detail=f"File '{filename}' not found"
            )
        
        os.remove(file_path)
        
        return StatusResponse(
            status="success",
            message=f"Successfully deleted '{filename}'",
            details={"deleted_file": filename}
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error deleting file: {str(e)}"
        )

@app.delete("/files")
async def clear_downloads():
    """Clear all files from downloads directory"""
    try:
        download_dir = parser.download_dir
        
        if not os.path.exists(download_dir):
            return StatusResponse(
                status="success",
                message="Downloads directory is already empty"
            )
        
        files = [f for f in os.listdir(download_dir) if os.path.isfile(os.path.join(download_dir, f))]
        deleted_count = 0
        
        for file_name in files:
            file_path = os.path.join(download_dir, file_name)
            try:
                os.remove(file_path)
                deleted_count += 1
            except Exception as e:
                print(f"Warning: Could not delete {file_name}: {e}")
        
        return StatusResponse(
            status="success",
            message=f"Successfully deleted {deleted_count} file(s)",
            details={"deleted_count": deleted_count}
        )
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error clearing downloads: {str(e)}"
        )

@app.get("/logs")
async def get_latest_log():
    """Download the latest combined log file"""
    try:
        log_file = parser.log_file
        
        if not os.path.exists(log_file):
            raise HTTPException(
                status_code=404,
                detail="No log file available. Run /parse first."
            )
        
        return FileResponse(
            path=log_file,
            filename="latest-log.txt",
            media_type="text/plain"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving log: {str(e)}"
        )

# ==============================
# POSTGRESQL BACKUP ENDPOINTS
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
            "backup_file": str(backup_file),
            "timestamp": timestamp,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/backup/base")
def base_backup():
    """
    Takes a physical base backup using pg_basebackup.
    You can use this as the starting point for PITR.
    """
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
            "timestamp": timestamp,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/backup/incremental")
def incremental_backup():
    """
    Forces a WAL switch using SELECT pg_switch_wal().
    Real WAL segments are archived by PostgreSQL itself to WAL_ARCHIVE_DIR
    via archive_command in postgresql.conf.
    """
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

        wal_files = []
        if WAL_ARCHIVE_DIR.exists():
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
            "note": "Real WAL segment generated and archived by PostgreSQL using archive_command.",
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/backups/{target}")
def list_backups(target: str):
    """
    target can be:
      - a database name (users_db, products_db, ...)
      - the server name (PG1) for server-level listing
    """
    target_lower = target.lower()

    if target_lower == SERVER_NAME.lower():
        data: Dict[str, Dict[str, List[str]]] = {}

        for db in DATABASES:
            full_backups = sorted(
                [f.name for f in FULL_BACKUP_DIR.glob(f"{db}_full_*.sql")],
                reverse=True,
            )
            data[db] = {
                "full_backups": full_backups,
            }

        wal_files = []
        if WAL_ARCHIVE_DIR.exists():
            wal_files = sorted(
                [f.name for f in WAL_ARCHIVE_DIR.glob("*") if f.is_file()],
                reverse=True,
            )

        base_backups = []
        if BASE_BACKUP_DIR.exists():
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

    full_backups = sorted(
        [f.name for f in FULL_BACKUP_DIR.glob(f"{target}_full_*.sql")],
        reverse=True,
    )

    wal_files = []
    if WAL_ARCHIVE_DIR.exists():
        wal_files = sorted(
            [f.name for f in WAL_ARCHIVE_DIR.glob("*") if f.is_file()],
            reverse=True,
        )

    base_backups = []
    if BASE_BACKUP_DIR.exists():
        base_backups = sorted(
            [d.name for d in BASE_BACKUP_DIR.iterdir() if d.is_dir()],
            reverse=True,
        )

    return {
        "server": SERVER_NAME,
        "database": target,
        "full_backups": full_backups,
        "wal_archive_files": wal_files,
        "base_backups": base_backups,
    }

@app.post("/restore/logical")
def restore_logical(req: LogicalRestoreRequest):
    validate_db(req.db_name)

    backup_path = FULL_BACKUP_DIR / req.backup_file
    if not backup_path.exists():
        raise HTTPException(status_code=404, detail="Backup file not found")

    try:
        drop_cmd = [
            "psql", "-U", POSTGRES_USER, "-h", POSTGRES_HOST,
            "-p", POSTGRES_PORT, "-d", "postgres",
            "-c", f"DROP DATABASE IF EXISTS {req.db_name};",
        ]

        create_cmd = [
            "psql", "-U", POSTGRES_USER, "-h", POSTGRES_HOST,
            "-p", POSTGRES_PORT, "-d", "postgres",
            "-c", f"CREATE DATABASE {req.db_name};",
        ]

        restore_cmd = [
            "psql", "-U", POSTGRES_USER, "-h", POSTGRES_HOST,
            "-p", POSTGRES_PORT, "-d", req.db_name,
            "-f", str(backup_path),
        ]

        subprocess.run(drop_cmd, check=True)
        subprocess.run(create_cmd, check=True)
        subprocess.run(restore_cmd, check=True)

        return {
            "success": True,
            "server": SERVER_NAME,
            "database": req.db_name,
            "restored_from": req.backup_file,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/restore")
def restore_db(req: LogicalRestoreRequest):
    return restore_logical(req)

@app.post("/restore/pitr")
def pitr_restore(req: PITRRestoreRequest):
    """
    This endpoint does NOT automatically stop/start PostgreSQL or manipulate
    the actual data directory (to avoid destroying data by accident).

    Instead, it validates that:
      - the requested base backup dir exists
      - the WAL archive dir exists

    And returns instructions on how to perform PITR manually using those.
    """
    base_dir = BASE_BACKUP_DIR / req.base_backup_name
    if not base_dir.exists() or not base_dir.is_dir():
        raise HTTPException(
            status_code=404,
            detail=f"Base backup directory not found: {base_dir}",
        )

    if not WAL_ARCHIVE_DIR.exists():
        raise HTTPException(
            status_code=500,
            detail=f"WAL archive directory not found: {WAL_ARCHIVE_DIR}",
        )

    return {
        "success": True,
        "server": SERVER_NAME,
        "base_backup_dir": str(base_dir),
        "wal_archive_dir": str(WAL_ARCHIVE_DIR),
        "target_time": req.target_time,
        "note": (
            "PITR is validated and ready. "
            "To actually perform PITR, stop PostgreSQL, replace the data directory "
            "with this base backup, configure restore_command to read from wal_archive_dir, "
            "and optionally set recovery_target_time, then start PostgreSQL."
        ),
    }

# ==============================
# MAIN (for uvicorn)
# ==============================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)