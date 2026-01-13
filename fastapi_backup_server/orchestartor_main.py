#!/usr/bin/env python3
"""
Orchestrator with proper backup directory structure:
/backup_runs/
   cg_lakehouse_main_2026-01-07_14-22-11/
       ├── metadata.json
       ├── postgres/
       │     └── users_db.sql
       └── ceph/
             └── src-slog-bkt1/
                   └── (objects)
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import requests
import json
import os
from datetime import datetime
from pathlib import Path

app = FastAPI()

# ============================================================================
# CONFIGURATION
# ============================================================================

POSTGRES_URL = "http://localhost:8001"
CEPH_URL = "http://localhost:8000"

# Base backup directory
BACKUP_BASE_DIR = "./backup_runs"

# Consistency Groups config file
CG_CONFIG_FILE = "consistency_groups_config.json"

# ============================================================================
# MODELS
# ============================================================================

class BackupRequest(BaseModel):
    cg_id: str
    backup_type: Optional[str] = None  # Override CG default if provided

class RestoreRequest(BaseModel):
    backup_id: str  # Format: cg_id_timestamp
    drop_existing: bool = False

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def load_cg_definitions() -> Dict[str, Any]:
    """Load consistency group definitions from config file"""
    try:
        with open(CG_CONFIG_FILE, 'r') as f:
            config = json.load(f)
            return config.get("consistency_groups", [])
    except FileNotFoundError:
        return []
    except json.JSONDecodeError as e:
        print(f"Error parsing CG config: {e}")
        return []

def get_cg_definition(cg_id: str) -> Optional[Dict[str, Any]]:
    """Get a specific CG definition"""
    cgs = load_cg_definitions()
    for cg in cgs:
        if cg.get("cg_id") == cg_id:
            return cg
    return None

def create_backup_id(cg_id: str) -> str:
    """Create backup ID: cg_id + timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return f"{cg_id}_{timestamp}"

def create_backup_directory_structure(backup_id: str) -> Dict[str, str]:
    """
    Create directory structure for a backup run
    Returns paths dict
    """
    base_path = Path(BACKUP_BASE_DIR) / backup_id
    postgres_path = base_path / "postgres"
    ceph_path = base_path / "ceph"
    
    # Create directories
    base_path.mkdir(parents=True, exist_ok=True)
    postgres_path.mkdir(exist_ok=True)
    ceph_path.mkdir(exist_ok=True)
    
    return {
        "base": str(base_path),
        "postgres": str(postgres_path),
        "ceph": str(ceph_path),
        "metadata": str(base_path / "metadata.json")
    }

def save_backup_metadata(paths: Dict[str, str], cg: Dict[str, Any], 
                         backup_id: str, results: Dict[str, Any]):
    """Save backup metadata to metadata.json"""
    metadata = {
        "backup_id": backup_id,
        "cg_id": cg.get("cg_id"),
        "cg_name": cg.get("name"),
        "timestamp": datetime.now().isoformat(),
        "backup_type": results.get("backup_type", "full"),
        "postgres_databases": cg.get("postgres_databases", []),
        "ceph_buckets": cg.get("ceph_buckets", []),
        "ceph_prefixes": cg.get("ceph_object_prefixes", []),
        "results": results,
        "paths": paths
    }
    
    with open(paths["metadata"], 'w') as f:
        json.dump(metadata, f, indent=2)

def list_all_backups() -> List[Dict[str, Any]]:
    """List all backup runs"""
    backup_base = Path(BACKUP_BASE_DIR)
    
    if not backup_base.exists():
        return []
    
    backups = []
    for backup_dir in backup_base.iterdir():
        if backup_dir.is_dir():
            metadata_file = backup_dir / "metadata.json"
            if metadata_file.exists():
                try:
                    with open(metadata_file, 'r') as f:
                        metadata = json.load(f)
                        backups.append(metadata)
                except:
                    pass
    
    # Sort by timestamp (newest first)
    backups.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    return backups

def get_backup_metadata(backup_id: str) -> Optional[Dict[str, Any]]:
    """Get metadata for a specific backup"""
    metadata_path = Path(BACKUP_BASE_DIR) / backup_id / "metadata.json"
    
    if not metadata_path.exists():
        return None
    
    try:
        with open(metadata_path, 'r') as f:
            return json.load(f)
    except:
        return None

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "orchestrator"}

@app.get("/consistency-groups/definitions")
async def list_cg_definitions():
    """List all pre-defined consistency groups"""
    cgs = load_cg_definitions()
    return {
        "consistency_groups": cgs,
        "total": len(cgs)
    }

@app.get("/consistency-groups/definitions/{cg_id}")
async def get_cg_definition_endpoint(cg_id: str):
    """Get a specific CG definition"""
    cg = get_cg_definition(cg_id)
    if not cg:
        raise HTTPException(status_code=404, detail=f"CG '{cg_id}' not found")
    return {"consistency_group": cg}

@app.get("/consistency-groups/backups")
async def list_backups():
    """List all backup runs"""
    backups = list_all_backups()
    return {
        "backups": backups,
        "total": len(backups)
    }

@app.get("/consistency-groups/backups/{backup_id}")
async def get_backup_details(backup_id: str):
    """Get details of a specific backup"""
    metadata = get_backup_metadata(backup_id)
    if not metadata:
        raise HTTPException(status_code=404, detail=f"Backup '{backup_id}' not found")
    return {"backup": metadata}

@app.post("/backup")
async def backup(request: BackupRequest):
    """
    Backup using pre-defined Consistency Group
    Creates: /backup_runs/cg_id_timestamp/
    """
    cg_id = request.cg_id
    
    # Get CG definition
    cg = get_cg_definition(cg_id)
    if not cg:
        raise HTTPException(status_code=404, detail=f"CG '{cg_id}' not found in config")
    
    if not cg.get("enabled", True):
        raise HTTPException(status_code=400, detail=f"CG '{cg_id}' is disabled")
    
    # Determine backup type
    backup_type = request.backup_type or cg.get("backup_type", "full")
    
    # Create backup ID and directory structure
    backup_id = create_backup_id(cg_id)
    paths = create_backup_directory_structure(backup_id)
    
    print(f"Starting backup: {backup_id}")
    print(f"Backup directory: {paths['base']}")
    
    errors = []
    results = {
        "backup_id": backup_id,
        "backup_type": backup_type,
        "postgres_backups": [],
        "ceph_backups": []
    }
    
    # ========================================================================
    # BACKUP POSTGRESQL DATABASES
    # ========================================================================
    
    postgres_dbs = cg.get("postgres_databases", [])
    print(f"Backing up PostgreSQL databases: {postgres_dbs}")
    
    for db_name in postgres_dbs:
        try:
            # Call PostgreSQL backup API with target directory
            response = requests.post(
                f"{POSTGRES_URL}/backup/full",
                json={
                    "db_name": db_name,
                    "backup_dir": paths["postgres"]  # Pass the postgres subdirectory
                },
                timeout=120
            )
            
            if response.status_code == 200:
                data = response.json()
                results["postgres_backups"].append({
                    "database": db_name,
                    "success": True,
                    "backup_file": data.get("backup_file"),
                    "size": data.get("size")
                })
                print(f"  ✓ {db_name} backed up")
            else:
                error_msg = f"Failed to backup {db_name}: {response.text}"
                errors.append(error_msg)
                results["postgres_backups"].append({
                    "database": db_name,
                    "success": False,
                    "error": response.text
                })
                print(f"  ✗ {db_name} failed")
        
        except Exception as e:
            error_msg = f"Error backing up {db_name}: {str(e)}"
            errors.append(error_msg)
            results["postgres_backups"].append({
                "database": db_name,
                "success": False,
                "error": str(e)
            })
            print(f"  ✗ {db_name} error: {e}")
    
    # ========================================================================
    # BACKUP CEPH/S3 OBJECTS
    # ========================================================================
    
    ceph_buckets = cg.get("ceph_buckets", [])
    ceph_prefixes = cg.get("ceph_object_prefixes", ["*"])
    
    print(f"Backing up Ceph buckets: {ceph_buckets} with prefixes: {ceph_prefixes}")
    
    for bucket in ceph_buckets:
        try:
            # Create bucket subdirectory
            bucket_path = Path(paths["ceph"]) / bucket
            bucket_path.mkdir(exist_ok=True)
            
            # Call Ceph backup API with target directory and prefixes
            response = requests.post(
                f"{CEPH_URL}/backup",
                json={
                    "bucket_name": bucket,
                    "prefixes": ceph_prefixes,
                    "backup_dir": str(bucket_path)  # Pass the bucket subdirectory
                },
                timeout=180
            )
            
            if response.status_code == 200:
                data = response.json()
                results["ceph_backups"].append({
                    "bucket": bucket,
                    "success": True,
                    "objects_count": data.get("objects_backed_up", 0),
                    "total_size": data.get("total_size", 0)
                })
                print(f"  ✓ {bucket} backed up ({data.get('objects_backed_up', 0)} objects)")
            else:
                error_msg = f"Failed to backup bucket {bucket}: {response.text}"
                errors.append(error_msg)
                results["ceph_backups"].append({
                    "bucket": bucket,
                    "success": False,
                    "error": response.text
                })
                print(f"  ✗ {bucket} failed")
        
        except Exception as e:
            error_msg = f"Error backing up bucket {bucket}: {str(e)}"
            errors.append(error_msg)
            results["ceph_backups"].append({
                "bucket": bucket,
                "success": False,
                "error": str(e)
            })
            print(f"  ✗ {bucket} error: {e}")
    
    # Save metadata
    save_backup_metadata(paths, cg, backup_id, results)
    
    print(f"Backup completed: {backup_id}")
    
    return {
        "status": "success" if not errors else "partial",
        "backup_id": backup_id,
        "backup_path": paths["base"],
        "details": {
            "results": results,
            "errors": errors
        }
    }

@app.post("/restore")
async def restore(request: RestoreRequest):
    """
    Restore from a backup ID
    """
    backup_id = request.backup_id
    
    # Get backup metadata
    metadata = get_backup_metadata(backup_id)
    if not metadata:
        raise HTTPException(status_code=404, detail=f"Backup '{backup_id}' not found")
    
    paths = metadata.get("paths", {})
    results = {"postgres": [], "ceph": []}
    errors = []
    
    print(f"Starting restore from: {backup_id}")
    
    # ========================================================================
    # RESTORE POSTGRESQL
    # ========================================================================
    
    postgres_backups = metadata.get("results", {}).get("postgres_backups", [])
    
    for pg_backup in postgres_backups:
        if not pg_backup.get("success"):
            continue
        
        db_name = pg_backup.get("database")
        backup_file = pg_backup.get("backup_file")
        
        if not backup_file:
            continue
        
        # Construct full path to backup file
        backup_file_path = os.path.join(paths.get("postgres", ""), 
                                        os.path.basename(backup_file))
        
        try:
            response = requests.post(
                f"{POSTGRES_URL}/restore",
                json={
                    "backup_file": backup_file_path,
                    "db_name": db_name,
                    "drop_existing": request.drop_existing
                },
                timeout=180
            )
            
            if response.status_code == 200:
                results["postgres"].append({
                    "database": db_name,
                    "success": True
                })
                print(f"  ✓ {db_name} restored")
            else:
                error_msg = f"Failed to restore {db_name}: {response.text}"
                errors.append(error_msg)
                results["postgres"].append({
                    "database": db_name,
                    "success": False,
                    "error": response.text
                })
                print(f"  ✗ {db_name} failed")
        
        except Exception as e:
            error_msg = f"Error restoring {db_name}: {str(e)}"
            errors.append(error_msg)
            results["postgres"].append({
                "database": db_name,
                "success": False,
                "error": str(e)
            })
    
    # ========================================================================
    # RESTORE CEPH OBJECTS
    # ========================================================================
    
    ceph_backups = metadata.get("results", {}).get("ceph_backups", [])
    
    for ceph_backup in ceph_backups:
        if not ceph_backup.get("success"):
            continue
        
        bucket = ceph_backup.get("bucket")
        bucket_backup_path = os.path.join(paths.get("ceph", ""), bucket)
        
        try:
            response = requests.post(
                f"{CEPH_URL}/restore",
                json={
                    "bucket_name": bucket,
                    "backup_dir": bucket_backup_path
                },
                timeout=180
            )
            
            if response.status_code == 200:
                data = response.json()
                results["ceph"].append({
                    "bucket": bucket,
                    "success": True,
                    "objects_restored": data.get("objects_restored", 0)
                })
                print(f"  ✓ {bucket} restored")
            else:
                error_msg = f"Failed to restore bucket {bucket}: {response.text}"
                errors.append(error_msg)
                results["ceph"].append({
                    "bucket": bucket,
                    "success": False,
                    "error": response.text
                })
        
        except Exception as e:
            error_msg = f"Error restoring bucket {bucket}: {str(e)}"
            errors.append(error_msg)
            results["ceph"].append({
                "bucket": bucket,
                "success": False,
                "error": str(e)
            })
    
    print(f"Restore completed from: {backup_id}")
    
    return {
        "status": "success" if not errors else "partial",
        "backup_id": backup_id,
        "details": {
            "results": results,
            "errors": errors
        }
    }

if __name__ == "__main__":
    import uvicorn
    
    # Create backup base directory if it doesn't exist
    Path(BACKUP_BASE_DIR).mkdir(exist_ok=True)
    
    print("="*70)
    print("Orchestrator Server - CG-Based Backup with Directory Structure")
    print("="*70)
    print(f"Backup base directory: {BACKUP_BASE_DIR}")
    print(f"CG config file: {CG_CONFIG_FILE}")
    print("="*70)
    
    uvicorn.run(app, host="0.0.0.0", port=8002)