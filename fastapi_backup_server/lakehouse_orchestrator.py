"""
Lakehouse Orchestrator Server v3.0 (Port 8002)
CORRECTED: Now fetches Ceph objects directly from S3 via Ceph server
Centralized backup management with S3 direct fetch
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
import httpx
from datetime import datetime
import json
import os
from pathlib import Path
import shutil

app = FastAPI(
    title="Lakehouse Orchestrator API v3.0",
    description="CG-based backup system with centralized storage and S3 fetch",
    version="3.0.0"
)

# ==============================
# CONFIGURATION
# ==============================

POSTGRES_SERVER = "http://localhost:8001"
CEPH_SERVER = "http://localhost:8000"

# Orchestrator centralized storage
BASE_DIR = Path("/root/sp-lakehouse-backup/fastapi_backup_server")
BACKUP_BASE_DIR = BASE_DIR / "backups"
POSTGRES_BACKUP_DIR = BACKUP_BASE_DIR / "postgres"
CEPH_BACKUP_DIR = BACKUP_BASE_DIR / "ceph"
METADATA_DIR = BACKUP_BASE_DIR / "metadata"
CG_CONFIG_FILE = BASE_DIR / "consistency_groups_config.json"

# In-memory cache for CG definitions
CG_DEFINITIONS = {}

# ==============================
# INITIALIZE DIRECTORIES
# ==============================

def init_directories():
    """Create all required backup directories"""
    dirs = [BACKUP_BASE_DIR, POSTGRES_BACKUP_DIR, CEPH_BACKUP_DIR, METADATA_DIR]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
    
    print(f"   📁 Base: {BACKUP_BASE_DIR}")
    print(f"   📁 PostgreSQL: {POSTGRES_BACKUP_DIR}")
    print(f"   📁 Ceph: {CEPH_BACKUP_DIR}")
    print(f"   📁 Metadata: {METADATA_DIR}")

# ==============================
# MODELS
# ==============================

class BackupByCGRequest(BaseModel):
    cg_id: str = Field(..., description="Consistency Group ID to backup")
    backup_type: Optional[str] = Field(None, description="Override backup type: full, base, incremental")


class RestoreByBackupIDRequest(BaseModel):
    backup_id: str = Field(..., description="Backup ID to restore from")
    drop_existing: bool = Field(False, description="Drop existing database before restore")


class UnifiedResponse(BaseModel):
    status: str
    message: str
    details: Optional[Dict[str, Any]] = None


# ==============================
# CG CONFIGURATION LOADER
# ==============================

def load_cg_definitions():
    """Load consistency group definitions from JSON config file"""
    global CG_DEFINITIONS
    
    if not CG_CONFIG_FILE.exists():
        print(f"❌ ERROR: {CG_CONFIG_FILE} not found!")
        return False
    
    try:
        with open(CG_CONFIG_FILE, 'r') as f:
            data = json.load(f)
            cgs = data.get("consistency_groups", [])
            
            if not cgs:
                print(f"⚠️ Warning: No consistency groups found")
                return False
            
            CG_DEFINITIONS = {cg["cg_id"]: cg for cg in cgs}
            
            print(f"✅ Loaded {len(CG_DEFINITIONS)} consistency group definition(s)")
            for cg_id, cg in CG_DEFINITIONS.items():
                enabled = "✓" if cg.get("enabled", True) else "✗"
                print(f"   {enabled} {cg_id}: {cg.get('name')}")
            
            return True
    except Exception as e:
        print(f"❌ Error loading CG config: {str(e)}")
        return False


def get_cg_definition(cg_id: str) -> Optional[Dict[str, Any]]:
    """Get a specific CG definition"""
    return CG_DEFINITIONS.get(cg_id)


# ==============================
# BACKUP METADATA MANAGEMENT
# ==============================

def save_backup_metadata(backup_id: str, metadata: Dict[str, Any]):
    """Save backup metadata to local JSON file"""
    metadata_file = METADATA_DIR / f"{backup_id}.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"   💾 Metadata saved: {metadata_file}")
    return metadata_file


def load_backup_metadata(backup_id: str) -> Optional[Dict[str, Any]]:
    """Load backup metadata from local JSON file"""
    metadata_file = METADATA_DIR / f"{backup_id}.json"
    if not metadata_file.exists():
        return None
    
    with open(metadata_file, 'r') as f:
        return json.load(f)


def list_all_backups() -> List[Dict[str, Any]]:
    """List all backup metadata files"""
    backups = []
    for metadata_file in METADATA_DIR.glob("*.json"):
        try:
            with open(metadata_file, 'r') as f:
                backups.append(json.load(f))
        except Exception as e:
            print(f"Warning: Failed to load {metadata_file}: {e}")
    
    backups.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    return backups


# ==============================
# FILE DOWNLOAD HELPERS
# ==============================

async def download_postgres_backup(backup_file: str) -> Optional[Path]:
    """Download PostgreSQL backup file from postgres server"""
    try:
        local_file = POSTGRES_BACKUP_DIR / backup_file
        
        # Download from PostgreSQL server
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.get(
                f"{POSTGRES_SERVER}/download/backup/{backup_file}",
                follow_redirects=True
            )
            
            if response.status_code == 200:
                with open(local_file, 'wb') as f:
                    f.write(response.content)
                
                file_size_mb = local_file.stat().st_size / (1024 * 1024)
                print(f"   ⬇️  Downloaded: {backup_file} ({file_size_mb:.2f} MB)")
                return local_file
            else:
                print(f"   ⚠️  Download failed: {response.status_code}")
                return None
    except Exception as e:
        print(f"   ❌ Download error: {str(e)}")
        return None


async def download_ceph_object(filename: str) -> Optional[Path]:
    """Download Ceph object from ceph server"""
    try:
        # Preserve directory structure
        local_file = CEPH_BACKUP_DIR / filename
        local_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Download from Ceph server
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.get(
                f"{CEPH_SERVER}/download/{filename}",
                follow_redirects=True
            )
            
            if response.status_code == 200:
                with open(local_file, 'wb') as f:
                    f.write(response.content)
                
                file_size_kb = local_file.stat().st_size / 1024
                print(f"   ⬇️  {filename} ({file_size_kb:.1f} KB)")
                return local_file
            else:
                print(f"   ⚠️  Failed: {filename}")
                return None
    except Exception as e:
        print(f"   ❌ Error {filename}: {str(e)}")
        return None


# ==============================
# STARTUP
# ==============================

@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    print("\n" + "=" * 70)
    print("🏠 LAKEHOUSE ORCHESTRATOR v3.0 - WITH S3 DIRECT FETCH")
    print("=" * 70)
    print("📁 Initializing backup directories:")
    init_directories()
    print("\n📋 Loading CG definitions:")
    load_cg_definitions()
    print("=" * 70 + "\n")


# ==============================
# HEALTH & INFO
# ==============================

@app.get("/", response_model=UnifiedResponse)
async def root():
    """Root endpoint with API information"""
    return UnifiedResponse(
        status="online",
        message="Lakehouse Orchestrator v3.0 - S3 Direct Fetch",
        details={
            "version": "3.0.0",
            "port": 8002,
            "approach": "Download & centralize backups with S3 direct fetch",
            "storage_location": str(BACKUP_BASE_DIR),
            "directories": {
                "postgres": str(POSTGRES_BACKUP_DIR),
                "ceph": str(CEPH_BACKUP_DIR),
                "metadata": str(METADATA_DIR)
            },
            "loaded_cgs": len(CG_DEFINITIONS)
        }
    )


@app.get("/health")
async def health():
    """Check health of orchestrator and backend servers"""
    async def check_server(url: str, name: str):
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                endpoint = "/" if "8000" in url else "/health"
                response = await client.get(f"{url}{endpoint}")
                return {"status": "healthy" if response.status_code == 200 else "unhealthy", "name": name}
        except Exception:
            return {"status": "unreachable", "name": name}
    
    postgres_health = await check_server(POSTGRES_SERVER, "PostgreSQL")
    ceph_health = await check_server(CEPH_SERVER, "Ceph")
    
    # Check local storage
    storage_health = {
        "base_dir": BASE_DIR.exists(),
        "postgres_dir": POSTGRES_BACKUP_DIR.exists(),
        "ceph_dir": CEPH_BACKUP_DIR.exists(),
        "metadata_dir": METADATA_DIR.exists()
    }
    
    all_healthy = (
        postgres_health["status"] == "healthy" and 
        ceph_health["status"] == "healthy" and
        all(storage_health.values())
    )
    
    return {
        "status": "healthy" if all_healthy else "degraded",
        "orchestrator": {
            "status": "running",
            "port": 8002,
            "version": "3.0.0",
            "storage": str(BACKUP_BASE_DIR),
            "cg_definitions": len(CG_DEFINITIONS)
        },
        "backend_servers": {
            "postgres": postgres_health,
            "ceph": ceph_health
        },
        "local_storage": storage_health
    }


# ==============================
# CG DEFINITION ENDPOINTS
# ==============================

@app.get("/consistency-groups/definitions")
async def list_cg_definitions():
    """List all pre-defined consistency group definitions"""
    cgs = list(CG_DEFINITIONS.values())
    return {
        "status": "success",
        "message": f"Found {len(cgs)} consistency group definition(s)",
        "config_file": str(CG_CONFIG_FILE),
        "consistency_groups": cgs
    }


@app.get("/consistency-groups/definitions/{cg_id}")
async def get_cg_definition_endpoint(cg_id: str):
    """Get specific consistency group definition"""
    cg = CG_DEFINITIONS.get(cg_id)
    
    if not cg:
        available_cgs = list(CG_DEFINITIONS.keys())
        raise HTTPException(
            status_code=404,
            detail=f"CG '{cg_id}' not found. Available: {available_cgs}"
        )
    
    return {
        "status": "success",
        "message": f"Consistency group: {cg_id}",
        "consistency_group": cg
    }


# ==============================
# BACKUP BY CG ID (CORRECTED)
# ==============================

@app.post("/backup", response_model=UnifiedResponse)
async def backup_by_cg_id(req: BackupByCGRequest):
    """
    Backup using pre-defined CG ID.
    CORRECTED: Now fetches Ceph objects directly from S3 via Ceph server.
    Downloads all data to orchestrator storage.
    """
    cg = get_cg_definition(req.cg_id)
    
    if not cg:
        available_cgs = list(CG_DEFINITIONS.keys())
        raise HTTPException(
            status_code=404,
            detail=f"CG '{req.cg_id}' not found. Available: {available_cgs}"
        )
    
    if not cg.get("enabled", True):
        raise HTTPException(status_code=400, detail=f"CG '{req.cg_id}' is disabled")
    
    backup_type = req.backup_type or cg.get("backup_type", "full")
    timestamp = datetime.now()
    backup_id = f"cg_{timestamp.strftime('%Y%m%d_%H%M%S')}"
    
    print(f"\n{'='*70}")
    print(f"🔄 Starting backup: {backup_id}")
    print(f"   CG: {req.cg_id} ({cg.get('name')})")
    print(f"   Type: {backup_type}")
    print(f"   Storage: {BACKUP_BASE_DIR}")
    print(f"{'='*70}")
    
    results = {
        "backup_id": backup_id,
        "cg_id": req.cg_id,
        "cg_name": cg.get("name"),
        "backup_type": backup_type,
        "postgres_backups": [],
        "ceph_objects": [],
        "downloaded_files": {
            "postgres": [],
            "ceph": []
        }
    }
    errors = []
    
    # ========== BACKUP & DOWNLOAD POSTGRESQL ==========
    pg_databases = cg.get("postgres_databases", [])
    
    for db_name in pg_databases:
        print(f"\n📊 PostgreSQL: {db_name}")
        try:
            # Step 1: Create backup on PostgreSQL server
            async with httpx.AsyncClient(timeout=60.0) as client:
                if backup_type == "full":
                    pg_response = await client.post(
                        f"{POSTGRES_SERVER}/backup/full",
                        json={"db_name": db_name}
                    )
                elif backup_type == "base":
                    pg_response = await client.post(f"{POSTGRES_SERVER}/backup/base")
                elif backup_type == "incremental":
                    pg_response = await client.post(f"{POSTGRES_SERVER}/backup/incremental")
                else:
                    raise HTTPException(status_code=400, detail=f"Invalid backup type")
                
                if pg_response.status_code == 200:
                    pg_data = pg_response.json()
                    backup_file = pg_data.get("backup_file") or pg_data.get("file")
                    
                    print(f"   ✅ Created: {backup_file}")
                    
                    # Step 2: Download backup to orchestrator
                    local_file = await download_postgres_backup(backup_file)
                    
                    results["postgres_backups"].append({
                        "database": db_name,
                        "success": True,
                        "backup_file": backup_file,
                        "local_path": str(local_file) if local_file else None
                    })
                    
                    if local_file:
                        results["downloaded_files"]["postgres"].append(str(local_file))
                else:
                    error_msg = f"Backup failed for {db_name}"
                    print(f"   ❌ {error_msg}")
                    errors.append(error_msg)
        except Exception as e:
            error_msg = f"Error for {db_name}: {str(e)}"
            print(f"   ❌ {error_msg}")
            errors.append(error_msg)
    
    # ========== FETCH & DOWNLOAD CEPH OBJECTS (CORRECTED) ==========
    ceph_buckets = cg.get("ceph_buckets", [])
    
    if ceph_buckets:
        for bucket in ceph_buckets:
            print(f"\n🗄️  Processing Ceph bucket: {bucket}")
            
            try:
                # Step 1: Tell Ceph server to fetch objects from S3
                prefixes = cg.get("ceph_object_prefixes", ["*"])
                
                print(f"   📥 Requesting Ceph server to fetch from S3...")
                print(f"   Bucket: {bucket}")
                print(f"   Prefixes: {prefixes}")
                
                async with httpx.AsyncClient(timeout=180.0) as client:
                    # Build URL with query parameters for prefixes
                    params = {"bucket": bucket}
                    # Add multiple prefix parameters
                    url = f"{CEPH_SERVER}/fetch-from-s3"
                    
                    # Create params dict with list of prefixes
                    # CORRECTED CODE for orchestrator.py
                    fetch_response = await client.post(
                        f"{CEPH_SERVER}/fetch-from-s3",
                        json={  # <--- Changed 'params' to 'json' to send a proper Body
                            "bucket": bucket,
                            "prefixes": prefixes
                        }
                    )
                    
                    if fetch_response.status_code == 200:
                        fetch_data = fetch_response.json()
                        fetched_objects = fetch_data.get("downloaded", [])
                        
                        print(f"   ✅ Ceph server fetched {len(fetched_objects)} object(s) from S3")
                        
                        # Step 2: Now download those objects from Ceph server to orchestrator
                        if fetched_objects:
                            print(f"   ⬇️  Downloading to orchestrator storage...")
                            downloaded_count = 0
                            
                            for obj_info in fetched_objects:
                                object_key = obj_info.get("object_key")
                                
                                # Download from Ceph server
                                local_file = await download_ceph_object(object_key)
                                
                                if local_file:
                                    downloaded_count += 1
                                    results["ceph_objects"].append(object_key)
                                    results["downloaded_files"]["ceph"].append(str(local_file))
                            
                            print(f"   ✅ Downloaded to orchestrator: {downloaded_count}/{len(fetched_objects)}")
                        else:
                            print(f"   ℹ️  No objects matched prefixes")
                    else:
                        error_msg = f"Ceph S3 fetch failed: {fetch_response.text}"
                        print(f"   ❌ {error_msg}")
                        errors.append(error_msg)
                        
            except Exception as e:
                error_msg = f"Ceph bucket '{bucket}' error: {str(e)}"
                print(f"   ❌ {error_msg}")
                errors.append(error_msg)
    
    # ========== SAVE METADATA ==========
    metadata = {
        "backup_id": backup_id,
        "timestamp": timestamp.isoformat(),
        "cg_id": req.cg_id,
        "cg_name": cg.get("name"),
        "backup_type": backup_type,
        "postgres_databases": pg_databases,
        "postgres_backups": results["postgres_backups"],
        "ceph_buckets": ceph_buckets,
        "ceph_objects": results["ceph_objects"],
        "storage": {
            "base_dir": str(BACKUP_BASE_DIR),
            "postgres_dir": str(POSTGRES_BACKUP_DIR),
            "ceph_dir": str(CEPH_BACKUP_DIR),
            "downloaded_files": results["downloaded_files"]
        },
        "errors": errors if errors else None
    }
    
    save_backup_metadata(backup_id, metadata)
    
    # ========== SUMMARY ==========
    pg_success = len([b for b in results["postgres_backups"] if b.get("success")])
    
    print(f"\n{'='*70}")
    print(f"✅ BACKUP COMPLETED: {backup_id}")
    print(f"   PostgreSQL: {pg_success}/{len(pg_databases)} databases")
    print(f"   Ceph: {len(results['ceph_objects'])} objects")
    print(f"   Storage: {BACKUP_BASE_DIR}")
    print(f"{'='*70}\n")
    
    status = "success" if not errors else "partial_success"
    message = f"✅ Backup completed: {backup_id}" if not errors else f"⚠️ Backup with issues"
    
    return UnifiedResponse(
        status=status,
        message=message,
        details={
            "results": results,
            "metadata_file": str(METADATA_DIR / f"{backup_id}.json"),
            "errors": errors if errors else None,
            "timestamp": timestamp.isoformat()
        }
    )


# ==============================
# LIST BACKUPS
# ==============================

@app.get("/consistency-groups/backups")
async def list_backup_instances():
    """List all backup instances"""
    backups = list_all_backups()
    
    return {
        "status": "success",
        "message": f"Found {len(backups)} backup instance(s)",
        "storage_location": str(BACKUP_BASE_DIR),
        "groups": backups,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/consistency-groups/backups/{backup_id}")
async def get_backup_instance(backup_id: str):
    """Get specific backup instance details"""
    metadata = load_backup_metadata(backup_id)
    
    if not metadata:
        raise HTTPException(
            status_code=404,
            detail=f"Backup '{backup_id}' not found"
        )
    
    return {
        "status": "success",
        "backup": metadata,
        "timestamp": datetime.now().isoformat()
    }


# ==============================
# RESTORE BY BACKUP ID
# ==============================

@app.post("/restore", response_model=UnifiedResponse)
async def restore_by_backup_id(req: RestoreByBackupIDRequest):
    """
    Restore from backup ID.
    Uses downloaded files from orchestrator storage.
    """
    metadata = load_backup_metadata(req.backup_id)
    
    if not metadata:
        raise HTTPException(
            status_code=404,
            detail=f"Backup '{req.backup_id}' not found"
        )
    
    print(f"\n{'='*70}")
    print(f"🔄 Starting restore: {req.backup_id}")
    print(f"   Timestamp: {metadata.get('timestamp')}")
    print(f"{'='*70}")
    
    results = {
        "backup_id": req.backup_id,
        "postgres": [],
        "ceph": []
    }
    errors = []
    
    # ========== RESTORE POSTGRESQL ==========
    postgres_backups = metadata.get("postgres_backups", [])
    
    for backup_info in postgres_backups:
        db_name = backup_info.get("database")
        backup_file = backup_info.get("backup_file")
        local_path = backup_info.get("local_path")
        
        print(f"\n📊 Restoring: {db_name}")
        print(f"   From: {backup_file}")
        
        if not local_path or not Path(local_path).exists():
            error_msg = f"Backup file not found locally: {local_path}"
            print(f"   ❌ {error_msg}")
            errors.append(error_msg)
            continue
        
        try:
            # Upload to PostgreSQL server for restore
            async with httpx.AsyncClient(timeout=120.0) as client:
                restore_response = await client.post(
                    f"{POSTGRES_SERVER}/restore/logical",
                    json={
                        "db_name": db_name,
                        "backup_file": backup_file
                    }
                )
                
                if restore_response.status_code == 200:
                    print(f"   ✅ Restored successfully")
                    results["postgres"].append({
                        "database": db_name,
                        "success": True
                    })
                else:
                    error_msg = f"Restore failed: {restore_response.text}"
                    print(f"   ❌ {error_msg}")
                    errors.append(error_msg)
        except Exception as e:
            error_msg = f"Error: {str(e)}"
            print(f"   ❌ {error_msg}")
            errors.append(error_msg)
    
    # ========== RESTORE CEPH OBJECTS ==========
    ceph_objects = metadata.get("ceph_objects", [])
    ceph_bucket = metadata.get("ceph_buckets", ["src-slog-bkt1"])[0]
    
    if ceph_objects:
        print(f"\n🗄️  Restoring {len(ceph_objects)} Ceph objects to {ceph_bucket}")
        
        restored_count = 0
        for obj in ceph_objects:
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    restore_response = await client.post(
                        f"{CEPH_SERVER}/restore",
                        json={
                            "filename": obj,
                            "bucket": ceph_bucket
                        }
                    )
                    
                    if restore_response.status_code == 200:
                        restored_count += 1
            except Exception as e:
                print(f"   ⚠️  Failed: {obj}")
        
        print(f"   ✅ Restored: {restored_count}/{len(ceph_objects)}")
        
        results["ceph"] = {
            "total_files": len(ceph_objects),
            "successful": restored_count
        }
    
    # ========== SUMMARY ==========
    pg_success = len([r for r in results["postgres"] if r.get("success")])
    
    print(f"\n{'='*70}")
    print(f"✅ RESTORE COMPLETED")
    print(f"   PostgreSQL: {pg_success}/{len(postgres_backups)}")
    print(f"{'='*70}\n")
    
    status = "success" if not errors else "partial_success"
    message = f"✅ Restore completed" if not errors else f"⚠️ Restore with issues"
    
    return UnifiedResponse(
        status=status,
        message=message,
        details={
            "backup_id": req.backup_id,
            "results": results,
            "errors": errors if errors else None,
            "timestamp": datetime.now().isoformat()
        }
    )


# ==============================
# STARTUP
# ==============================

if __name__ == "__main__":
    import uvicorn
    
    print("\n" + "=" * 70)
    print("🏠 LAKEHOUSE ORCHESTRATOR v3.0 - S3 DIRECT FETCH")
    print("=" * 70)
    print(f"Version: 3.0.0")
    print(f"Port: 8002")
    print(f"Storage: {BACKUP_BASE_DIR}")
    print("=" * 70)
    print("DIRECTORY STRUCTURE:")
    print(f"  {BACKUP_BASE_DIR}/")
    print(f"  ├── postgres/    (PostgreSQL backups)")
    print(f"  ├── ceph/        (Ceph objects)")
    print(f"  └── metadata/    (Backup IDs)")
    print("=" * 70)
    print("FEATURES:")
    print("  ✓ S3 Direct Fetch via Ceph server")
    print("  ✓ Centralized backup storage")
    print("  ✓ Consistency group management")
    print("=" * 70 + "\n")
    
    uvicorn.run(app, host="0.0.0.0", port=8002)