from fastapi import FastAPI, Request, HTTPException
from agents.backup_restore_agent1 import BackupRestoreAgent1
from agents.backup_restore_agent2 import BackupRestoreAgent2
from backup_utils.pg_utils import list_backups, restore_backup
from ba_client_storage.sp_rest_client import StorageProtectClient
from ba_client_storage.ba_client_manager import BAClientManager

app = FastAPI(title="CrewAI Backup-BAClient API", version="2.0")

# Initialize components
agent1 = BackupRestoreAgent1()
agent2 = BackupRestoreAgent2()
ba_client = BAClientManager()

# IBM Storage Protect REST config
SP_BASE_URL = "https://9.11.52.248:9081"
SP_USERNAME = "storageprotect"
SP_PASSWORD = "admin_12345_admin"


@app.get("/")
def index():
    return {"message": "CrewAI Backup API with BAClient + IBM SP REST is running 🚀"}


# ----------------------
# BACKUP / RESTORE APIs
# ----------------------
@app.post("/backup/{pg}")
def perform_backup(pg: str, mode: str = "cli"):
    """Trigger pgBackRest + BAClient or IBM SP REST archive backup."""
    if pg == "pg1":
        agent = agent1
    elif pg == "pg2":
        agent = agent2
    else:
        raise HTTPException(status_code=400, detail="Invalid cluster ID")

    # Perform local backup first
    result = agent.perform_backup("full")

    if mode == "cli":
        ba_result = ba_client.backup_directory(agent.backup_dir)
        return {"cluster": pg, "pgbackrest": result, "ba_client": ba_result}

    elif mode == "rest":
        sp = StorageProtectClient(SP_BASE_URL, SP_USERNAME, SP_PASSWORD)
        sp.signon()
        rest_result = sp.start_backup(agent.backup_dir, f"REST API Backup for {pg}")
        return {"cluster": pg, "pgbackrest": result, "ibm_storage_protect": rest_result}

    else:
        raise HTTPException(status_code=400, detail="Invalid mode. Use cli or rest.")


@app.post("/restore/{pg}")
def perform_restore(pg: str):
    """Trigger restore for given cluster."""
    if pg == "pg1":
        result = agent1.perform_restore(recent=True)
        return {"cluster": "pg1", "restore": result}
    elif pg == "pg2":
        result = agent2.perform_restore(recent=True)
        return {"cluster": "pg2", "restore": result}
    return {"error": "Invalid cluster ID"}


@app.get("/list/{pg}")
def list_backups(pg: str):
    """List all backups for a cluster."""
    if pg == "pg1":
        return agent1.list_backups()
    elif pg == "pg2":
        return agent2.list_backups()
    return {"error": "Invalid cluster ID"}


# ----------------------
# DSMCAD APIs
# ----------------------
@app.get("/dsmcad/status")
def dsmcad_status():
    return {"status": ba_client.check_status()}


@app.get("/dsmcad/start")
def start_dsmcad():
    return {"result": ba_client.start_dsmcad()}


# ----------------------
# CrewAI / Ollama JSON Task API
# ----------------------
@app.post("/agent1/task")
async def agent1_task(request: Request):
    data = await request.json()
    result = agent1.execute(data)
    return result


@app.post("/agent2/task")
async def agent2_task(request: Request):
    data = await request.json()
    result = agent2.execute(data)
    return result
