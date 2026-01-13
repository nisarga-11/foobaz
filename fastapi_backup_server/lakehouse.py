"""
Lakehouse Orchestrator Server (Port 8002)
FILE: lakehouse_orchestrator.py
CORRECTED: Proper separation of Consistency Groups and Backups
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
import httpx
from datetime import datetime
import uuid

app = FastAPI(
    title="Lakehouse Orchestrator API",
    description="Unified Backup/Restore with Consistency Groups",
    version="3.1.0"
)

# ==============================
# CONFIGURATION
# ==============================
POSTGRES_SERVER = "http://localhost:8001"
CEPH_SERVER = "http://localhost:8000"

DATABASES = {
    "users_db": "users_db",
    "products_db": "products_db", 
    "reports_db": "reports_db",
}

# IN-MEMORY STORAGE
CONSISTENCY_GROUPS: Dict[str, Dict] = {}
BACKUPS: Dict[str, Dict] = {}

# ==============================
# REQUEST MODELS
# ==============================
class ConsistencyGroupCreateRequest(BaseModel):
    cg_name: str = Field(..., description="CG name (e.g., 'lakehouse')")
    databases: List[str] = Field(..., description="PostgreSQL database names")
    include_ceph: bool = Field(True, description="Include Ceph storage")
    description: Optional[str] = None

class BackupRequest(BaseModel):
    cg_id: str = Field(..., description="Consistency group ID")
    backup_type: str = Field("full", description="full, incremental, base")
    start_time: Optional[str] = Field(None, description="Ceph start (DD/MMM/YYYY:HH:MM:SS)")
    end_time: Optional[str] = Field(None, description="Ceph end (DD/MMM/YYYY:HH:MM:SS)")
    backup_name: Optional[str] = None

class RestoreRequest(BaseModel):
    backup_id: str = Field(..., description="Backup ID to restore")
    target_cg_id: Optional[str] = None
    drop_existing: bool = Field(False, description="Drop existing databases")

class UnifiedResponse(BaseModel):
    status: str
    message: str
    details: Optional[Dict[str, Any]] = None

# ==============================
# CONSISTENCY GROUPS
# ==============================
@app.post("/consistency-groups/create", response_model=UnifiedResponse)
async def create_consistency_group(req: ConsistencyGroupCreateRequest):
    for db in req.databases:
        if db not in DATABASES.values():
            raise HTTPException(400, f"Invalid database: {db}")
    
    cg_id = f"cg_{uuid.uuid4().hex[:8]}"
    cg_data = {
        "cg_id": cg_id,
        "cg_name": req.cg_name,
        "databases": req.databases,
        "include_ceph": req.include_ceph,
        "description": req.description or f"Consistency group: {req.cg_name}",
        "created_at": datetime.now().isoformat(),
        "backup_count": 0,
        "last_backup": None
    }
    CONSISTENCY_GROUPS[cg_id] = cg_data
    
    return UnifiedResponse(
        status="success",
        message=f"✅ Consistency group '{req.cg_name}' created",
        details={"consistency_group": cg_data}
    )

@app.get("/consistency-groups")
async def list_consistency_groups():
    groups = sorted(CONSISTENCY_GROUPS.values(), key=lambda x: x["created_at"], reverse=True)
    return {"status": "success", "total": len(groups), "consistency_groups": groups}

@app.get("/consistency-groups/{cg_id}")
async def get_consistency_group(cg_id: str):
    if cg_id not in CONSISTENCY_GROUPS:
        raise HTTPException(404, f"CG '{cg_id}' not found")
    cg = CONSISTENCY_GROUPS[cg_id]
    cg_backups = [{"backup_id": b["backup_id"], "backup_name": b["backup_name"], 
                   "timestamp": b["timestamp"]} for b in BACKUPS.values() if b["cg_id"] == cg_id]
    return {"consistency_group": cg, "backups": sorted(cg_backups, key=lambda x: x["timestamp"], reverse=True)}

# ==============================
# BACKUP OPERATIONS
# ==============================
@app.post("/backup", response_model=UnifiedResponse)
async def create_backup(req: BackupRequest):
    if req.cg_id not in CONSISTENCY_GROUPS:
        raise HTTPException(404, f"CG '{req.cg_id}' not found")
    
    cg = CONSISTENCY_GROUPS[req.cg_id]
    backup_id = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    backup_name = req.backup_name or f"{cg['cg_name']}_{req.backup_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print(f"\n{'='*70}\n🔄 CREATING BACKUP: {backup_id}\nCG: {cg['cg_name']}\n{'='*70}\n")
    
    backup_components = {"postgres": {}, "ceph": None}
    errors = []
    
    # Backup PostgreSQL
    print("📦 Backing up PostgreSQL...")
    for db_name in cg["databases"]:
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                endpoint = f"{POSTGRES_SERVER}/backup/{req.backup_type}"
                payload = {"db_name": db_name, "cg_id": req.cg_id, "cg_name": cg["cg_name"], "backup_id": backup_id}
                pg_response = await client.post(endpoint, json=payload)
                
                if pg_response.status_code == 200:
                    pg_data = pg_response.json()
                    backup_components["postgres"][db_name] = {
                        "success": True,
                        "backup_file": pg_data.get("backup_file") or pg_data.get("base_backup_name"),
                        "timestamp": pg_data.get("timestamp")
                    }
                    print(f"  ✓ {db_name}")
                else:
                    errors.append(f"PG backup failed for {db_name}")
                    backup_components["postgres"][db_name] = {"success": False, "error": pg_response.text}
        except Exception as e:
            errors.append(f"PG error {db_name}: {str(e)}")
            backup_components["postgres"][db_name] = {"success": False, "error": str(e)}
    
    # Backup Ceph
    if cg["include_ceph"] and req.start_time and req.end_time:
        print("\n📦 Backing up Ceph...")
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                parse_resp = await client.post(f"{CEPH_SERVER}/parse", 
                    json={"start_time": req.start_time, "end_time": req.end_time, "operation_filter": "ALL"})
                
                if parse_resp.status_code == 200:
                    files_resp = await client.get(f"{CEPH_SERVER}/files")
                    if files_resp.status_code == 200:
                        ceph_files = files_resp.json().get("files", [])
                        backup_components["ceph"] = {"success": True, "files_count": len(ceph_files), 
                                                     "files": ceph_files, "time_range": {"start": req.start_time, "end": req.end_time}}
                        print(f"  ✓ {len(ceph_files)} objects")
        except Exception as e:
            errors.append(f"Ceph error: {str(e)}")
            backup_components["ceph"] = {"success": False, "error": str(e)}
    
    # Save backup
    backup_data = {
        "backup_id": backup_id,
        "backup_name": backup_name,
        "cg_id": req.cg_id,
        "cg_name": cg["cg_name"],
        "backup_type": req.backup_type,
        "timestamp": datetime.now().isoformat(),
        "components": backup_components,
        "status": "completed" if not errors else "completed_with_errors",
        "errors": errors or None
    }
    BACKUPS[backup_id] = backup_data
    CONSISTENCY_GROUPS[req.cg_id]["backup_count"] += 1
    CONSISTENCY_GROUPS[req.cg_id]["last_backup"] = backup_id
    
    print(f"\n✅ BACKUP COMPLETED: {backup_id}\n{'='*70}\n")
    
    return UnifiedResponse(
        status="success" if not errors else "partial_success",
        message=f"✅ Backup '{backup_name}' created",
        details={"backup": backup_data}
    )

@app.get("/backups")
async def list_backups(cg_id: Optional[str] = None):
    if cg_id and cg_id not in CONSISTENCY_GROUPS:
        raise HTTPException(404, f"CG '{cg_id}' not found")
    
    backups = [b for b in BACKUPS.values() if not cg_id or b["cg_id"] == cg_id]
    return {"total": len(backups), "backups": sorted(backups, key=lambda x: x["timestamp"], reverse=True)}

@app.get("/backups/{backup_id}")
async def get_backup(backup_id: str):
    if backup_id not in BACKUPS:
        raise HTTPException(404, f"Backup '{backup_id}' not found")
    return {"backup": BACKUPS[backup_id]}

# ==============================
# RESTORE
# ==============================
@app.post("/restore", response_model=UnifiedResponse)
async def restore_backup(req: RestoreRequest):
    if req.backup_id not in BACKUPS:
        raise HTTPException(404, f"Backup '{req.backup_id}' not found")
    
    backup = BACKUPS[req.backup_id]
    target_cg_id = req.target_cg_id or backup["cg_id"]
    
    if target_cg_id not in CONSISTENCY_GROUPS:
        raise HTTPException(404, f"Target CG '{target_cg_id}' not found")
    
    print(f"\n{'='*70}\n🔄 RESTORING: {req.backup_id}\n{'='*70}\n")
    
    results = {"postgres": {}, "ceph": None}
    errors = []
    
    # Restore PostgreSQL
    print("📥 Restoring PostgreSQL...")
    for db_name, pg_backup in backup["components"]["postgres"].items():
        if not pg_backup.get("success"):
            continue
        
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                restore_resp = await client.post(f"{POSTGRES_SERVER}/restore/logical",
                    json={"db_name": db_name, "backup_file": pg_backup["backup_file"]})
                
                if restore_resp.status_code == 200:
                    results["postgres"][db_name] = {"success": True}
                    print(f"  ✓ {db_name}")
                else:
                    errors.append(f"Restore failed: {db_name}")
                    results["postgres"][db_name] = {"success": False, "error": restore_resp.text}
        except Exception as e:
            errors.append(f"Restore error {db_name}: {str(e)}")
            results["postgres"][db_name] = {"success": False, "error": str(e)}
    
    # Restore Ceph
    ceph_backup = backup["components"].get("ceph")
    if ceph_backup and ceph_backup.get("success"):
        ceph_files = ceph_backup.get("files", [])
        if ceph_files:
            print(f"\n📥 Restoring {len(ceph_files)} Ceph objects...")
            try:
                async with httpx.AsyncClient(timeout=180.0) as client:
                    restore_results = []
                    for obj_file in ceph_files:
                        try:
                            resp = await client.post(f"{CEPH_SERVER}/restore",
                                json={"filename": obj_file, "bucket": "restored-objects"})
                            restore_results.append({"file": obj_file, "success": resp.status_code == 200})
                        except Exception as e:
                            restore_results.append({"file": obj_file, "success": False, "error": str(e)})
                    
                    successful = sum(1 for r in restore_results if r["success"])
                    results["ceph"] = {"total": len(restore_results), "successful": successful,
                                      "failed": len(restore_results) - successful}
                    print(f"  ✓ {successful}/{len(restore_results)}")
            except Exception as e:
                errors.append(f"Ceph restore error: {str(e)}")
    
    print(f"\n✅ RESTORE COMPLETED\n{'='*70}\n")
    
    return UnifiedResponse(
        status="success" if not errors else "partial_success",
        message=f"✅ Backup '{backup['backup_name']}' restored",
        details={"backup_id": req.backup_id, "results": results, "errors": errors or None}
    )

# ==============================
# HEALTH
# ==============================
@app.get("/health")
async def health():
    backend_status = {}
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            pg_resp = await client.get(f"{POSTGRES_SERVER}/health")
            backend_status["postgres"] = "healthy" if pg_resp.status_code == 200 else "unhealthy"
    except:
        backend_status["postgres"] = "unreachable"
    
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            ceph_resp = await client.get(f"{CEPH_SERVER}/health")
            backend_status["ceph"] = "healthy" if ceph_resp.status_code == 200 else "unhealthy"
    except:
        backend_status["ceph"] = "unreachable"
    
    return {"status": "healthy", "version": "3.1.0", "backends": backend_status,
            "storage": {"consistency_groups": len(CONSISTENCY_GROUPS), "backups": len(BACKUPS)}}

@app.get("/")
async def root():
    return {
        "service": "Lakehouse Orchestrator v3.1.0",
        "workflow": {
            "1_create_cg": "POST /consistency-groups/create",
            "2_create_backup": "POST /backup (with cg_id)",
            "3_restore": "POST /restore (with backup_id)"
        }
    }

if __name__ == "__main__":
    import uvicorn
    print("="*70)
    print("🏠 LAKEHOUSE ORCHESTRATOR v3.1.0")
    print("="*70)
    print("Port: 8002")
    print("PostgreSQL: localhost:8001")
    print("Ceph: localhost:8000")
    print("="*70)
    uvicorn.run(app, host="0.0.0.0", port=8002)