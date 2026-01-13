# Consistency Group Integration Instructions

## Files Created

1. **consistency_group_manager.py**
   - Location: `ceph/consistency_group_manager.py`
   - Purpose: Core consistency group logic

2. **consistency_group_restore_cli.py**
   - Location: `project/consistency_group_restore_cli.py`
   - Purpose: Interactive CLI for restore
   - Usage: `python consistency_group_restore_cli.py`

## Next Steps

### Step 1: Add to main.py

Add these imports to `ceph/main.py`:

```python
from consistency_group_manager import ConsistencyGroupManager, ConsistencyGroup

# Initialize manager
cg_manager = ConsistencyGroupManager()
```

### Step 2: Add Endpoints to main.py

Add these endpoints after your existing endpoints:

```python
@app.post("/backup/lakehouse/create-consistency-group")
async def create_cg(postgres_backup: str, postgres_database: str, 
                    ceph_objects: List[str], ceph_bucket_source: str):
    group = cg_manager.create_consistency_group(
        postgres_backup, postgres_database, ceph_objects, ceph_bucket_source
    )
    return group

@app.get("/consistency-groups")
async def list_cgs(database: Optional[str] = None):
    if database:
        groups = cg_manager.list_groups_by_database(database)
    else:
        groups = [ConsistencyGroup(**g) for g in cg_manager.list_all_groups()]
    return {"total_groups": len(groups), "groups": groups}

@app.get("/consistency-groups/{group_id}")
async def get_cg(group_id: str):
    group = cg_manager.get_group(group_id)
    if not group:
        raise HTTPException(404, f"Group {group_id} not found")
    return group
```

### Step 3: Update Your Backup Function

In your lakehouse backup function, add after backup completes:

```python
# After PostgreSQL and Ceph backup
ceph_files = [f['name'] for f in ceph_status['details']['files']]

# Create consistency group
cg = cg_manager.create_consistency_group(
    postgres_backup=pg_backup_filename,
    postgres_database="users_db",
    ceph_objects=ceph_files,
    ceph_bucket_source="lakehouse"
)

print(f"✓ Consistency Group created: {cg.group_id}")
```

### Step 4: Add to Orchestrator Menu

In `fastapi_backup_server/lakehouse_orchestrator.py`:

```python
# Add import
import sys
sys.path.append('../project')
from consistency_group_restore_cli import consistency_group_restore_menu

# Add menu option
def manual_restore_menu():
    print("1. Restore Specific Backup")
    print("2. Restore from Consistency Group (New)")
    
    if choice == "2":
        consistency_group_restore_menu()
```

## Testing

1. Start your FastAPI server
2. Run a backup
3. Check consistency groups:
   ```bash
   curl http://localhost:8000/consistency-groups | jq
   ```

4. Test CLI:
   ```bash
   python project/consistency_group_restore_cli.py
   ```

