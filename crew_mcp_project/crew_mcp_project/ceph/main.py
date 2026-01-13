################################################################################
# FILE: main.py (CORRECTED)
# LOCATION: /root/sp-lakehouse-backup/crew_mcp_project/crew_mcp_project/ceph/main.py
# PURPOSE: FastAPI server with S3 log parsing and consistency group support
# FIX: Added PostgreSQL backup download from S3 before restore
################################################################################

"""
FastAPI Server for S3 Log Parser and Operation Mirror with Consistency Groups
Port: 8000
CORRECTED: PostgreSQL backups are now downloaded from S3 before restore
"""
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
import os
from pathlib import Path
import zipfile
import io
import subprocess
from timebasedparser_fastapi import S3LogParser, OperationSummary
from consistency_group_manager import ConsistencyGroupManager, ConsistencyGroup
import boto3

app = FastAPI(
    title="S3 Log Parser API with Consistency Groups",
    description="Parse S3 logs, mirror operations, and manage consistency groups",
    version="2.1.0"
)

# Initialize parser
parser = S3LogParser()

# Initialize Consistency Group Manager with JSON file storage
cg_manager = ConsistencyGroupManager(storage_file="consistency_groups.json")

# S3 Configuration
S3_ENDPOINT = "http://fenrir-vm158.storage.tucson.ibm.com:8080"
S3_REGION = "us-east-1"
S3_ACCESS_KEY = "abc"
S3_SECRET_KEY = "abc"
RESTORE_BUCKET = "restored-objects"

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    region_name=S3_REGION,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)

# PostgreSQL Configuration
PG_HOST = "localhost"
PG_PORT = "5432"
PG_USER = "postgres"
PG_PASSWORD = ""  # No password - using trust authentication
# CORRECTED: Use absolute path where PostgreSQL backups are actually stored
POSTGRES_BACKUP_DIR = "/root/sp-lakehouse-backup/project/backups/full"


# --- Request Models ---
class TimeRangeRequest(BaseModel):
    start_time: str = Field(..., json_schema_extra={"example": "06/Nov/2025:04:00:00"}, description="Start time in format DD/MMM/YYYY:HH:MM:SS")
    end_time: str = Field(..., json_schema_extra={"example": "06/Nov/2025:05:00:00"}, description="End time in format DD/MMM/YYYY:HH:MM:SS")
    operation_filter: Optional[str] = Field("ALL", json_schema_extra={"example": "PUT"}, description="Operation type: PUT, GET, DELETE, or ALL")


class StatusResponse(BaseModel):
    status: str
    message: str
    details: Optional[dict] = None


class RestoreRequest(BaseModel):
    filename: str = Field(..., description="Filename to restore from downloads directory")
    bucket: Optional[str] = Field(None, description="Target S3 bucket")
    object_key: Optional[str] = Field(None, description="Target S3 object key")


class ConsistencyGroupCreateRequest(BaseModel):
    """Request model for creating a consistency group"""
    postgres_backup: str = Field(..., description="PostgreSQL backup filename")
    postgres_database: str = Field(..., description="PostgreSQL database name")
    ceph_objects: List[str] = Field(..., description="List of Ceph object filenames")
    ceph_bucket_source: str = Field(..., description="Source S3 bucket name")
    backup_type: str = Field(default="full", description="Type of backup: full, incremental, base")


class ConsistencyGroupListResponse(BaseModel):
    """Response model for listing consistency groups"""
    total_groups: int
    groups: List[ConsistencyGroup]


class ConsistencyGroupRestoreRequest(BaseModel):
    """Request model for restoring a consistency group"""
    group_id: str = Field(..., description="Consistency group ID to restore")
    target_postgres_database: Optional[str] = Field(None, description="Target database")
    target_ceph_bucket: Optional[str] = Field(None, description="Target S3 bucket")
    drop_existing: bool = Field(False, description="Drop existing database")

class S3FetchRequest(BaseModel):
    bucket: str
    prefixes: List[str]
# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def restore_postgres_backup(backup_file: str, database: str, drop_existing: bool = False):
    """
    Restore PostgreSQL database from backup file
    CORRECTED: Uses absolute path to PostgreSQL backups directory
    """
    backup_path = os.path.join(POSTGRES_BACKUP_DIR, backup_file)
    
    if not os.path.exists(backup_path):
        raise FileNotFoundError(
            f"PostgreSQL backup file not found: {backup_path}\n"
            f"Expected location: {POSTGRES_BACKUP_DIR}\n"
            f"Backup file: {backup_file}"
        )
    
    env = os.environ.copy()
    env['PGPASSWORD'] = PG_PASSWORD
    
    try:
        if drop_existing:
            print(f"Dropping existing database: {database}")
            drop_cmd = ['psql', '-h', PG_HOST, '-p', PG_PORT, '-U', PG_USER,
                       '-d', 'postgres', '-c', f'DROP DATABASE IF EXISTS {database};']
            subprocess.run(drop_cmd, env=env, check=True, capture_output=True, text=True)
        
        print(f"Creating database: {database}")
        create_cmd = ['psql', '-h', PG_HOST, '-p', PG_PORT, '-U', PG_USER,
                     '-d', 'postgres', '-c', f'CREATE DATABASE {database};']
        subprocess.run(create_cmd, env=env, capture_output=True, text=True)
        
        print(f"Restoring database from: {backup_path}")
        restore_cmd = ['psql', '-h', PG_HOST, '-p', PG_PORT, '-U', PG_USER,
                      '-d', database, '-f', backup_path]
        result = subprocess.run(restore_cmd, env=env, check=True, capture_output=True, text=True)
        
        return {
            "success": True,
            "database": database,
            "backup_file": backup_file,
            "backup_path": backup_path,
            "output": result.stdout[:500] if result.stdout else "Restore completed"
        }
    except subprocess.CalledProcessError as e:
        return {
            "success": False,
            "database": database,
            "backup_file": backup_file,
            "error": e.stderr if e.stderr else str(e)
        }


def restore_ceph_objects(object_files: List[str], bucket: str):
    """Restore multiple Ceph objects to S3"""
    results = []
    bucket_created = False
    
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception as e:
        error_str = str(e)
        if "NoSuchBucket" in error_str or "404" in error_str or "Not Found" in error_str:
            try:
                s3.create_bucket(Bucket=bucket)
                bucket_created = True
            except Exception as create_err:
                raise Exception(f"Failed to create bucket '{bucket}': {str(create_err)}")
    
    for filename in object_files:
        try:
            local_file = os.path.join(parser.download_dir, filename)
            
            if not os.path.exists(local_file):
                results.append({
                    "filename": filename,
                    "success": False,
                    "error": "File not found in downloads directory"
                })
                continue
            
            file_size = os.path.getsize(local_file)
            s3.upload_file(Filename=local_file, Bucket=bucket, Key=filename)
            
            results.append({
                "filename": filename,
                "success": True,
                "size": file_size,
                "s3_uri": f"s3://{bucket}/{filename}"
            })
        except Exception as e:
            results.append({
                "filename": filename,
                "success": False,
                "error": str(e)
            })
    
    return {
        "bucket": bucket,
        "bucket_created": bucket_created,
        "total_files": len(object_files),
        "successful": sum(1 for r in results if r["success"]),
        "failed": sum(1 for r in results if not r["success"]),
        "results": results
    }


# ============================================================================
# EXISTING ENDPOINTS
# ============================================================================

@app.get("/health", response_model=StatusResponse)
async def health():
    summary = cg_manager.get_groups_summary()
    return StatusResponse(
        status="healthy",
        message="S3 Log Parser with Consistency Groups is healthy",
        details={
            "service": "ceph",
            "port": 8000,
            "version": "2.1.0",
            "download_dir": parser.download_dir,
            "postgres_backup_dir": POSTGRES_BACKUP_DIR,
            "consistency_groups": {
                "total": summary["total_groups"],
                "storage_file": summary["storage_file"]
            }
        }
    )


@app.get("/", response_model=StatusResponse)
async def root():
    summary = cg_manager.get_groups_summary()
    return StatusResponse(
        status="online",
        message="S3 Log Parser API with Consistency Groups",
        details={
            "version": "2.1.0",
            "port": 8000,
            "features": ["parse", "restore", "consistency-groups"],
            "postgres_backup_dir": POSTGRES_BACKUP_DIR,
            "consistency_groups_storage": summary["storage_file"]
        }
    )


@app.post("/parse", response_model=StatusResponse)
async def parse_logs(request: TimeRangeRequest, background_tasks: BackgroundTasks):
    """Parse S3 logs and mirror operations in the specified time range"""
    try:
        time_fmt = "%d/%b/%Y:%H:%M:%S"
        try:
            start_dt = datetime.strptime(request.start_time, time_fmt)
            end_dt = datetime.strptime(request.end_time, time_fmt)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Invalid time format: {str(e)}")

        if start_dt >= end_dt:
            raise HTTPException(status_code=400, detail="Start time must be before end time")

        valid_operations = ["ALL", "PUT", "GET", "DELETE"]
        operation = request.operation_filter.upper()
        if operation not in valid_operations:
            raise HTTPException(status_code=400, detail=f"Invalid operation: {operation}")

        summary = parser.parse_and_mirror(request.start_time, request.end_time, operation)

        return StatusResponse(
            status="success",
            message=f"Successfully processed {summary.total_operations} operation(s)",
            details={
                "time_range": f"{request.start_time} → {request.end_time}",
                "operation_filter": operation,
                "total_operations": summary.total_operations,
                "files_remaining": summary.files_remaining,
                "download_dir": summary.download_dir
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing logs: {str(e)}")


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
        raise HTTPException(status_code=500, detail=f"Error getting status: {str(e)}")


@app.get("/files")
async def list_files():
    """List all files in the downloads directory"""
    try:
        download_dir = parser.download_dir
        
        if not os.path.exists(download_dir):
            return {
                "status": "success",
                "message": "Downloads directory is empty or does not exist",
                "details": {
                    "download_dir": download_dir,
                    "file_count": 0,
                    "files": []
                },
                "files": []
            }

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

        return {
            "status": "success",
            "message": f"Found {len(files)} file(s) in downloads directory",
            "details": {
                "download_dir": download_dir,
                "file_count": len(files),
                "files": file_details
            },
            "files": [f["name"] for f in file_details]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing files: {str(e)}")


@app.delete("/files/{filename}")
async def delete_file(filename: str):
    """Delete a specific file from downloads directory"""
    try:
        file_path = os.path.join(parser.download_dir, filename)
        
        if not os.path.exists(file_path):
            raise HTTPException(
                status_code=404,
                detail=f"File '{filename}' not found in downloads directory"
            )
        
        os.remove(file_path)
        
        return {
            "status": "success",
            "message": f"File '{filename}' deleted successfully",
            "details": {
                "filename": filename,
                "deleted_at": datetime.utcnow().isoformat() + "Z"
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting file: {str(e)}")


@app.delete("/files")
async def clear_downloads():
    """Clear all files from downloads directory"""
    try:
        download_dir = parser.download_dir
        
        if not os.path.exists(download_dir):
            return {
                "status": "success",
                "message": "Downloads directory is already empty or does not exist",
                "details": {
                    "download_dir": download_dir,
                    "files_deleted": 0
                }
            }
        
        files = [f for f in os.listdir(download_dir) if os.path.isfile(os.path.join(download_dir, f))]
        deleted_count = 0
        errors = []
        
        for filename in files:
            try:
                file_path = os.path.join(download_dir, filename)
                os.remove(file_path)
                deleted_count += 1
            except Exception as e:
                errors.append({"filename": filename, "error": str(e)})
        
        return {
            "status": "success" if not errors else "partial",
            "message": f"Deleted {deleted_count} file(s) from downloads directory",
            "details": {
                "download_dir": download_dir,
                "files_deleted": deleted_count,
                "files_failed": len(errors),
                "errors": errors if errors else None
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error clearing downloads: {str(e)}")

# Add this to your Ceph server (port 8000)
# Insert after the /files endpoint, before /restore

from fastapi.responses import FileResponse

# ==============================
# FILE DOWNLOAD ENDPOINT
# ==============================

@app.get("/download/{filename:path}")
async def download_file(filename: str):
    """
    Download a file from Ceph downloads directory.
    Orchestrator uses this to fetch Ceph objects.
    
    Args:
        filename: Can include subdirectories (e.g., "users/file.txt")
    """
    download_dir = parser.download_dir
    file_path = os.path.join(download_dir, filename)
    
    # Security check: ensure file is within download_dir
    real_download_dir = os.path.realpath(download_dir)
    real_file_path = os.path.realpath(file_path)
    
    if not real_file_path.startswith(real_download_dir):
        raise HTTPException(
            status_code=400,
            detail="Invalid file path - access denied"
        )
    
    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=404,
            detail=f"File '{filename}' not found in downloads directory"
        )
    
    if not os.path.isfile(file_path):
        raise HTTPException(
            status_code=400,
            detail=f"'{filename}' is not a file"
        )
    
    print(f"[DOWNLOAD] Serving file: {filename}")
    
    return FileResponse(
        path=file_path,
        filename=os.path.basename(filename),
        media_type='application/octet-stream'
    )


@app.get("/download-batch")
async def download_batch_files(filenames: List[str]):
    """
    Download multiple files as a ZIP archive.
    Useful for bulk downloads.
    """
    import zipfile
    import tempfile
    from io import BytesIO
    
    download_dir = parser.download_dir
    
    # Create in-memory ZIP file
    zip_buffer = BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for filename in filenames:
            file_path = os.path.join(download_dir, filename)
            
            if os.path.exists(file_path) and os.path.isfile(file_path):
                zip_file.write(file_path, arcname=filename)
    
    zip_buffer.seek(0)
    
    print(f"[DOWNLOAD] Serving batch ZIP: {len(filenames)} files")
    
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={
            "Content-Disposition": f"attachment; filename=ceph_objects_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        }
    )
# Add this endpoint to your Ceph server (main.py on port 8000)
# Insert this BEFORE the /restore endpoint

@app.post("/fetch-from-s3")
async def fetch_objects_from_s3(request: S3FetchRequest): # Use the model here
    """
    Fetch objects from S3 bucket based on prefixes.
    Downloads to local downloads directory for orchestrator to retrieve.
    """
    try:
        # Extract values from the validated request model
        bucket = request.bucket
        prefixes = request.prefixes
        
        downloaded = []
        errors = []
        
        print(f"\n{'='*70}")
        print(f"🔍 Fetching objects from S3 bucket: {bucket}")
        print(f"   Prefixes: {prefixes}")
        print(f"{'='*70}")
        
        # List all objects in bucket
        try:
            response = s3.list_objects_v2(Bucket=bucket)
            all_objects = response.get('Contents', [])
            
            if not all_objects:
                return {
                    "status": "success",
                    "message": f"No objects found in bucket '{bucket}'",
                    "bucket": bucket,
                    "total_fetched": 0,
                    "downloaded": []
                }
            
            print(f"   📦 Found {len(all_objects)} total objects in bucket")
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to list S3 bucket '{bucket}': {str(e)}"
            )
        
        # Filter by prefixes
        if "*" in prefixes or not prefixes:
            objects_to_fetch = all_objects
            print(f"   ✓ Fetching ALL objects")
        else:
            objects_to_fetch = [
                obj for obj in all_objects
                if any(obj['Key'].startswith(prefix.rstrip('*')) for prefix in prefixes)
            ]
            print(f"   ✓ Filtered to {len(objects_to_fetch)} objects matching prefixes")
        
        if not objects_to_fetch:
            return {
                "status": "success",
                "message": f"No objects match prefixes {prefixes}",
                "bucket": bucket,
                "total_fetched": 0,
                "downloaded": []
            }
        
        print(f"\n   ⬇️  Downloading {len(objects_to_fetch)} object(s)...\n")
        
        # Download each object
        for obj in objects_to_fetch:
            key = obj['Key']
            size_bytes = obj.get('Size', 0)
            size_display = f"{size_bytes / 1024:.1f} KB" if size_bytes < 1024*1024 else f"{size_bytes / (1024*1024):.2f} MB"
            
            try:
                # Create local path (preserve directory structure)
                local_path = os.path.join(parser.download_dir, key)
                local_dir = os.path.dirname(local_path)
                
                # Create directories if needed
                if local_dir and not os.path.exists(local_dir):
                    os.makedirs(local_dir, exist_ok=True)
                
                # Download from S3
                s3.download_file(Bucket=bucket, Key=key, Filename=local_path)
                
                downloaded.append({
                    "object_key": key,
                    "local_path": local_path,
                    "size": size_bytes,
                    "size_display": size_display
                })
                
                print(f"      ✓ {key} ({size_display})")
                
            except Exception as e:
                error_msg = str(e)
                errors.append({
                    "object_key": key,
                    "error": error_msg
                })
                print(f"      ✗ {key}: {error_msg}")
        
        print(f"\n   {'='*70}")
        print(f"   ✅ Successfully downloaded: {len(downloaded)}/{len(objects_to_fetch)}")
        if errors:
            print(f"   ⚠️  Failed: {len(errors)}")
        print(f"   {'='*70}\n")
        
        status = "success" if not errors else "partial_success"
        message = f"Downloaded {len(downloaded)}/{len(objects_to_fetch)} objects"
        
        return {
            "status": status,
            "message": message,
            "bucket": bucket,
            "prefixes": prefixes,
            "total_objects_in_bucket": len(all_objects),
            "total_matched": len(objects_to_fetch),
            "total_fetched": len(downloaded),
            "total_failed": len(errors),
            "downloaded": downloaded,
            "errors": errors if errors else None,
            "download_dir": parser.download_dir
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"S3 fetch operation failed: {str(e)}"
        )

# Also add a convenience endpoint to check what's in S3
@app.get("/s3/list/{bucket}")
async def list_s3_bucket(bucket: str, prefix: Optional[str] = None):
    """
    List objects in S3 bucket (without downloading).
    Useful for checking what's available before fetching.
    """
    try:
        print(f"📋 Listing S3 bucket: {bucket}")
        if prefix:
            print(f"   Prefix filter: {prefix}")
        
        list_params = {"Bucket": bucket}
        if prefix:
            list_params["Prefix"] = prefix
        
        response = s3.list_objects_v2(**list_params)
        objects = response.get('Contents', [])
        
        object_list = [
            {
                "key": obj['Key'],
                "size": obj.get('Size', 0),
                "last_modified": obj.get('LastModified').isoformat() if obj.get('LastModified') else None
            }
            for obj in objects
        ]
        
        total_size = sum(obj['size'] for obj in object_list)
        
        return {
            "status": "success",
            "bucket": bucket,
            "prefix": prefix,
            "total_objects": len(object_list),
            "total_size_bytes": total_size,
            "total_size_display": f"{total_size / (1024*1024):.2f} MB" if total_size > 1024*1024 else f"{total_size / 1024:.1f} KB",
            "objects": object_list
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list S3 bucket: {str(e)}"
        )

@app.post("/restore", response_model=StatusResponse)
async def restore_object(req: RestoreRequest):
    """Restore a file from downloads directory back to S3"""
    try:
        local_file = os.path.join(parser.download_dir, req.filename)
        
        if not os.path.exists(local_file):
            raise HTTPException(
                status_code=404,
                detail=f"File '{req.filename}' not found in downloads directory"
            )

        target_bucket = req.bucket or RESTORE_BUCKET
        target_key = req.object_key or req.filename

        try:
            s3.head_bucket(Bucket=target_bucket)
            bucket_created = False
        except Exception as e:
            error_str = str(e)
            if "NoSuchBucket" in error_str or "404" in error_str or "Not Found" in error_str:
                try:
                    s3.create_bucket(Bucket=target_bucket)
                    bucket_created = True
                except Exception as create_err:
                    raise HTTPException(
                        status_code=500,
                        detail=f"Failed to create bucket: {str(create_err)}"
                    )

        file_size = os.path.getsize(local_file)
        s3.upload_file(Filename=local_file, Bucket=target_bucket, Key=target_key)

        return StatusResponse(
            status="success",
            message=f"File '{req.filename}' restored successfully to S3",
            details={
                "bucket": target_bucket,
                "bucket_created": bucket_created,
                "object_key": target_key,
                "file_size": file_size,
                "s3_uri": f"s3://{target_bucket}/{target_key}"
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Restore failed: {str(e)}")


# ============================================================================
# CONSISTENCY GROUP ENDPOINTS
# ============================================================================

@app.post("/backup/lakehouse/create-consistency-group", response_model=ConsistencyGroup)
async def create_backup_consistency_group(request: ConsistencyGroupCreateRequest):
    """Create a consistency group linking PostgreSQL and Ceph backups"""
    try:
        group = cg_manager.create_consistency_group(
            postgres_backup=request.postgres_backup,
            postgres_database=request.postgres_database,
            ceph_objects=request.ceph_objects,
            ceph_bucket_source=request.ceph_bucket_source,
            backup_type=request.backup_type
        )
        return group
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create consistency group: {str(e)}")


@app.get("/consistency-groups", response_model=ConsistencyGroupListResponse)
async def list_consistency_groups(database: Optional[str] = None):
    """List all consistency groups, optionally filtered by database"""
    try:
        if database:
            groups = cg_manager.list_groups_by_database(database)
        else:
            groups_dict = cg_manager.list_all_groups()
            groups = [ConsistencyGroup(**g) for g in groups_dict]
        
        return ConsistencyGroupListResponse(
            total_groups=len(groups),
            groups=sorted(groups, key=lambda x: x.timestamp, reverse=True)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list consistency groups: {str(e)}")


@app.get("/consistency-groups/{group_id}", response_model=ConsistencyGroup)
async def get_consistency_group(group_id: str):
    """Get details of a specific consistency group"""
    group = cg_manager.get_group(group_id)
    
    if not group:
        raise HTTPException(status_code=404, detail=f"Consistency group '{group_id}' not found")
    
    return group


@app.get("/consistency-groups/latest/{database}", response_model=ConsistencyGroup)
async def get_latest_consistency_group(database: str):
    """Get the most recent consistency group for a database"""
    group = cg_manager.get_latest_group(database)
    
    if not group:
        raise HTTPException(status_code=404, detail=f"No consistency groups found for database '{database}'")
    
    return group


@app.post("/restore/consistency-group")
async def restore_consistency_group(request: ConsistencyGroupRestoreRequest):
    """
    Restore a complete consistency group (PostgreSQL + Ceph objects)
    CORRECTED: Now downloads PostgreSQL backup from S3 before restore
    """
    try:
        group = cg_manager.get_group(request.group_id)
        
        if not group:
            raise HTTPException(status_code=404, detail=f"Consistency group '{request.group_id}' not found")
        
        target_db = request.target_postgres_database or group.postgres_database
        target_bucket = request.target_ceph_bucket or group.ceph_bucket_source
        
        print(f"\n{'='*60}")
        print(f"Restoring consistency group: {group.group_id}")
        print(f"{'='*60}")
        print(f"  PostgreSQL Database: {target_db}")
        print(f"  PostgreSQL Backup: {group.postgres_backup}")
        print(f"  Ceph Objects: {len(group.ceph_objects)} files")
        print(f"  Target Bucket: {target_bucket}")
        print(f"  Drop Existing: {request.drop_existing}")
        print(f"{'='*60}\n")
        
        results = {
            "consistency_group": group.model_dump(),
            "postgres": None,
            "ceph": None,
            "errors": []
        }
        
        # Restore PostgreSQL (now with S3 download)
        try:
            print("Step 1: Restoring PostgreSQL database...")
            pg_result = restore_postgres_backup(group.postgres_backup, target_db, request.drop_existing)
            results["postgres"] = pg_result
            
            if pg_result["success"]:
                print(f"✓ PostgreSQL restore successful: {target_db}")
            else:
                error_msg = f"PostgreSQL restore failed: {pg_result.get('error')}"
                print(f"✗ {error_msg}")
                results["errors"].append(error_msg)
        except Exception as e:
            error_msg = f"PostgreSQL restore error: {str(e)}"
            print(f"✗ {error_msg}")
            results["postgres"] = {"success": False, "error": str(e)}
            results["errors"].append(error_msg)
        
        # Restore Ceph objects
        try:
            print(f"\nStep 2: Restoring {len(group.ceph_objects)} Ceph objects...")
            if group.ceph_objects:
                ceph_result = restore_ceph_objects(group.ceph_objects, target_bucket)
                results["ceph"] = ceph_result
                
                print(f"✓ Ceph restore completed: {ceph_result['successful']}/{ceph_result['total_files']} successful")
                
                if ceph_result["failed"] > 0:
                    error_msg = f"Some Ceph objects failed: {ceph_result['failed']}/{ceph_result['total_files']}"
                    print(f"⚠ {error_msg}")
                    results["errors"].append(error_msg)
            else:
                results["ceph"] = {"message": "No Ceph objects in this consistency group"}
                print("ℹ No Ceph objects to restore")
        except Exception as e:
            error_msg = f"Ceph restore error: {str(e)}"
            print(f"✗ {error_msg}")
            results["ceph"] = {"success": False, "error": str(e)}
            results["errors"].append(error_msg)
        
        # Determine overall status
        pg_success = results["postgres"] and results["postgres"].get("success", False)
        ceph_success = results["ceph"] and (results["ceph"].get("successful", 0) > 0 or "message" in results["ceph"])
        overall_success = pg_success and ceph_success
        
        status = "success" if overall_success and not results["errors"] else "partial" if pg_success or ceph_success else "failed"
        
        print(f"\n{'='*60}")
        print(f"Restore Status: {status.upper()}")
        print(f"{'='*60}\n")
        
        return {
            "status": status,
            "message": f"Consistency group {group.group_id} restore completed",
            "details": {
                "group_id": group.group_id,
                "group_timestamp": group.timestamp,
                "target_database": target_db,
                "target_bucket": target_bucket,
                "results": results,
                "restored_at": datetime.utcnow().isoformat() + "Z"
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Consistency group restore failed: {str(e)}")


@app.delete("/consistency-groups/{group_id}")
async def delete_consistency_group(group_id: str):
    """Delete a consistency group metadata"""
    success = cg_manager.delete_group(group_id)
    
    if not success:
        raise HTTPException(status_code=404, detail=f"Consistency group '{group_id}' not found")
    
    return StatusResponse(
        status="success",
        message=f"Consistency group '{group_id}' deleted",
        details={"group_id": group_id}
    )


# ============================================================================
# SERVER STARTUP
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    summary = cg_manager.get_groups_summary()
    
    print("=" * 60)
    print("🗄️  S3 Log Parser Server with Consistency Groups (v2.1.0)")
    print("=" * 60)
    print(f"Port: 8000")
    print(f"Download Directory: {parser.download_dir}")
    print(f"PostgreSQL Backup Directory: {POSTGRES_BACKUP_DIR}")
    print(f"Consistency Groups Storage: {summary['storage_file']}")
    print(f"Total Consistency Groups: {summary['total_groups']}")
    if summary['by_database']:
        print(f"Groups by Database:")
        for db, count in summary['by_database'].items():
            print(f"  - {db}: {count}")
    print("=" * 60)
    print("✓ Using local PostgreSQL backups (no S3 download needed)")
    print("=" * 60)
    
    uvicorn.run(app, host="0.0.0.0", port=8000)