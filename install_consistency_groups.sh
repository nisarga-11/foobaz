#!/bin/bash
################################################################################
# FILE: install_consistency_groups.sh
# LOCATION: /root/sp-lakehouse-backup/install_consistency_groups.sh
# PURPOSE: Install consistency group system
# USAGE: bash install_consistency_groups.sh
################################################################################

set -e  # Exit on error

echo "=============================================================================="
echo "  CONSISTENCY GROUP SYSTEM INSTALLATION"
echo "=============================================================================="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Base directories
BASE_DIR="/root/sp-lakehouse-backup"
CEPH_DIR="$BASE_DIR/crew_mcp_project/crew_mcp_project/ceph"
PROJECT_DIR="$BASE_DIR/project"
FASTAPI_SERVER_DIR="$BASE_DIR/fastapi_backup_server"

echo -e "${BLUE}Step 1: Checking directory structure...${NC}"
echo "  Base directory: $BASE_DIR"
echo "  Ceph directory: $CEPH_DIR"
echo "  Project directory: $PROJECT_DIR"
echo "  FastAPI server directory: $FASTAPI_SERVER_DIR"
echo ""

# Verify directories exist
if [ ! -d "$CEPH_DIR" ]; then
    echo -e "${RED}Error: Ceph directory not found: $CEPH_DIR${NC}"
    exit 1
fi

if [ ! -d "$PROJECT_DIR" ]; then
    echo -e "${YELLOW}Creating project directory: $PROJECT_DIR${NC}"
    mkdir -p "$PROJECT_DIR"
fi

echo -e "${GREEN}✓ Directory structure verified${NC}"
echo ""

# ============================================================================
# Step 2: Create backups directory structure
# ============================================================================

echo -e "${BLUE}Step 2: Creating backup directory structure...${NC}"

mkdir -p "$CEPH_DIR/backups/full"
mkdir -p "$CEPH_DIR/backups/incremental"

echo -e "${GREEN}✓ Backup directories created${NC}"
echo "  - $CEPH_DIR/backups/full"
echo "  - $CEPH_DIR/backups/incremental"
echo ""

# ============================================================================
# Step 3: Create consistency_group_manager.py
# ============================================================================

echo -e "${BLUE}Step 3: Creating consistency_group_manager.py...${NC}"

cat > "$CEPH_DIR/consistency_group_manager.py" << 'EOFMANAGER'
################################################################################
# FILE: consistency_group_manager.py
# LOCATION: ceph/consistency_group_manager.py
# PURPOSE: Core consistency group management
################################################################################

import json
import os
from datetime import datetime
from typing import List, Optional, Dict
from pydantic import BaseModel, Field

CONSISTENCY_GROUP_FILE = "backups/consistency_groups.json"

class ConsistencyGroup(BaseModel):
    group_id: str
    timestamp: str
    postgres_backup: str
    postgres_database: str
    ceph_objects: List[str]
    ceph_bucket_source: str
    backup_type: str = "full"
    status: str = "completed"
    metadata: Optional[Dict] = Field(default_factory=dict)

class ConsistencyGroupManager:
    def __init__(self, metadata_file: str = CONSISTENCY_GROUP_FILE):
        self.metadata_file = metadata_file
        self._ensure_metadata_file()
    
    def _ensure_metadata_file(self):
        os.makedirs(os.path.dirname(self.metadata_file), exist_ok=True)
        if not os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'w') as f:
                json.dump([], f)
    
    def create_consistency_group(self, postgres_backup: str, postgres_database: str,
                                ceph_objects: List[str], ceph_bucket_source: str,
                                backup_type: str = "full", metadata: Optional[Dict] = None) -> ConsistencyGroup:
        timestamp = datetime.utcnow()
        group_id = f"CG_{timestamp.strftime('%Y%m%d_%H%M%S')}"
        
        consistency_group = ConsistencyGroup(
            group_id=group_id,
            timestamp=timestamp.isoformat() + "Z",
            postgres_backup=postgres_backup,
            postgres_database=postgres_database,
            ceph_objects=ceph_objects,
            ceph_bucket_source=ceph_bucket_source,
            backup_type=backup_type,
            status="completed",
            metadata=metadata or {}
        )
        
        self._save_consistency_group(consistency_group)
        return consistency_group
    
    def _save_consistency_group(self, group: ConsistencyGroup):
        groups = self.list_all_groups()
        groups.append(group.dict())
        with open(self.metadata_file, 'w') as f:
            json.dump(groups, f, indent=2)
    
    def list_all_groups(self) -> List[Dict]:
        try:
            with open(self.metadata_file, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return []
    
    def get_group(self, group_id: str) -> Optional[ConsistencyGroup]:
        groups = self.list_all_groups()
        for group_dict in groups:
            if group_dict.get('group_id') == group_id:
                return ConsistencyGroup(**group_dict)
        return None
    
    def list_groups_by_database(self, database: str) -> List[ConsistencyGroup]:
        groups = self.list_all_groups()
        return [ConsistencyGroup(**g) for g in groups if g.get('postgres_database') == database]
    
    def get_latest_group(self, database: Optional[str] = None) -> Optional[ConsistencyGroup]:
        groups = self.list_all_groups()
        if database:
            groups = [g for g in groups if g.get('postgres_database') == database]
        if not groups:
            return None
        groups.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        return ConsistencyGroup(**groups[0])
EOFMANAGER

echo -e "${GREEN}✓ consistency_group_manager.py created${NC}"
echo "  Location: $CEPH_DIR/consistency_group_manager.py"
echo ""

# ============================================================================
# Step 4: Create consistency_group_restore_cli.py
# ============================================================================

echo -e "${BLUE}Step 4: Creating consistency_group_restore_cli.py...${NC}"

cat > "$PROJECT_DIR/consistency_group_restore_cli.py" << 'EOFCLI'
#!/usr/bin/env python3
################################################################################
# FILE: consistency_group_restore_cli.py
# LOCATION: project/consistency_group_restore_cli.py
# USAGE: python consistency_group_restore_cli.py
################################################################################

import requests
from datetime import datetime
from typing import Optional

API_BASE_URL = "http://localhost:8000"

class Colors:
    BOLD = '\033[1m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    RED = '\033[0;31m'
    CYAN = '\033[0;36m'
    NC = '\033[0m'

def print_header(text):
    print(f"\n{Colors.BOLD}{'─' * 70}\n{text}\n{'─' * 70}{Colors.NC}")

def print_success(text):
    print(f"{Colors.GREEN}✓ {text}{Colors.NC}")

def print_error(text):
    print(f"{Colors.RED}✗ {text}{Colors.NC}")

def print_info(text):
    print(f"{Colors.CYAN}ℹ {text}{Colors.NC}")

def list_consistency_groups(database=None):
    try:
        url = f"{API_BASE_URL}/consistency-groups"
        if database:
            url += f"?database={database}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print_error(f"Failed to fetch: {e}")
        return None

def consistency_group_restore_menu():
    print_header("LAKEHOUSE RESTORE - CONSISTENCY GROUPS")
    database = input("Filter by database (blank=all): ").strip()
    
    print_info("Fetching consistency groups...")
    result = list_consistency_groups(database if database else None)
    
    if not result:
        return
    
    groups = result.get('groups', [])
    if not groups:
        print_error("No consistency groups found")
        return
    
    print_success(f"Found {len(groups)} group(s)")
    
    for i, g in enumerate(groups, 1):
        print(f"\n{i}. {g['group_id']}")
        print(f"   Database: {g['postgres_database']}")
        print(f"   PG Backup: {g['postgres_backup']}")
        print(f"   Ceph Objects: {len(g['ceph_objects'])}")
    
    choice = input(f"\nSelect [1-{len(groups)}] or 0 to cancel: ").strip()
    if choice == "0":
        return
    
    try:
        selected = groups[int(choice) - 1]
        print(f"\nSelected: {selected['group_id']}")
        print("Ready to restore (implement restore logic)")
    except:
        print_error("Invalid choice")

if __name__ == "__main__":
    consistency_group_restore_menu()
EOFCLI

chmod +x "$PROJECT_DIR/consistency_group_restore_cli.py"

echo -e "${GREEN}✓ consistency_group_restore_cli.py created${NC}"
echo "  Location: $PROJECT_DIR/consistency_group_restore_cli.py"
echo ""

# ============================================================================
# Step 5: Create instructions file
# ============================================================================

echo -e "${BLUE}Step 5: Creating integration instructions...${NC}"

cat > "$BASE_DIR/INTEGRATION_INSTRUCTIONS.md" << 'EOFINST'
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

EOFINST

echo -e "${GREEN}✓ Instructions created${NC}"
echo "  Location: $BASE_DIR/INTEGRATION_INSTRUCTIONS.md"
echo ""

# ============================================================================
# Summary
# ============================================================================

echo ""
echo "=============================================================================="
echo -e "${GREEN}  INSTALLATION COMPLETE!${NC}"
echo "=============================================================================="
echo ""
echo "Files created:"
echo "  ✓ $CEPH_DIR/consistency_group_manager.py"
echo "  ✓ $PROJECT_DIR/consistency_group_restore_cli.py"
echo "  ✓ $BASE_DIR/INTEGRATION_INSTRUCTIONS.md"
echo ""
echo "Directories created:"
echo "  ✓ $CEPH_DIR/backups/full"
echo "  ✓ $CEPH_DIR/backups/incremental"
echo ""
echo -e "${YELLOW}Next Steps:${NC}"
echo "  1. Read: $BASE_DIR/INTEGRATION_INSTRUCTIONS.md"
echo "  2. Add consistency group code to main.py"
echo "  3. Update your backup function to create consistency groups"
echo "  4. Test with: python $PROJECT_DIR/consistency_group_restore_cli.py"
echo ""
echo "=============================================================================="
