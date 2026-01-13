#!/usr/bin/env python3
"""
Lakehouse CLI v3.0 - Consistency Group Based
Pre-defined CG approach: Define CGs first, then backup/restore by CG ID
"""
import sys
import requests
import json
from datetime import datetime
from typing import Optional, Dict, Any

# ============================================================================
# CONFIGURATION
# ============================================================================

ORCHESTRATOR_URL = "http://localhost:8002"
POSTGRES_URL = "http://localhost:8001"
CEPH_URL = "http://localhost:8000"

# Color codes
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def print_header(text: str):
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'='*70}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}{text.center(70)}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'='*70}{Colors.END}\n")

def print_success(text: str):
    print(f"{Colors.GREEN}✓ {text}{Colors.END}")

def print_error(text: str):
    print(f"{Colors.RED}✗ {text}{Colors.END}")

def print_warning(text: str):
    print(f"{Colors.YELLOW}⚠ {text}{Colors.END}")

def print_info(text: str):
    print(f"{Colors.BLUE}ℹ {text}{Colors.END}")

def print_json(data: Dict[Any, Any]):
    print(json.dumps(data, indent=2))

def get_user_input(prompt: str, default: Optional[str] = None) -> str:
    if default:
        user_input = input(f"{prompt} [{default}]: ").strip()
        return user_input if user_input else default
    return input(f"{prompt}: ").strip()

def confirm_action(message: str) -> bool:
    response = input(f"{Colors.YELLOW}{message} (y/N): {Colors.END}").strip().lower()
    return response == 'y'

def check_health():
    print_info("Checking server health...")
    
    servers = {
        "Orchestrator (8002)": ORCHESTRATOR_URL,
        "PostgreSQL (8001)": POSTGRES_URL,
        "Ceph (8000)": CEPH_URL
    }
    
    all_healthy = True
    for name, url in servers.items():
        try:
            response = requests.get(f"{url}/health", timeout=5)
            if response.status_code == 200:
                print_success(f"{name}: Healthy")
            else:
                print_error(f"{name}: Unhealthy")
                all_healthy = False
        except Exception as e:
            print_error(f"{name}: Unreachable")
            all_healthy = False
    
    return all_healthy

# ============================================================================
# CG DEFINITION MANAGEMENT
# ============================================================================

def list_cg_definitions():
    """List all pre-defined consistency groups"""
    print_header("PRE-DEFINED CONSISTENCY GROUPS")
    
    try:
        response = requests.get(
            f"{ORCHESTRATOR_URL}/consistency-groups/definitions",
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            cgs = data.get("consistency_groups", [])
            
            if not cgs:
                print_warning("No consistency groups defined")
                print_info("Edit consistency_groups_config.json to add CGs")
                return
            
            print(f"Total Consistency Groups: {len(cgs)}\n")
            
            for idx, cg in enumerate(cgs, 1):
                enabled = "✓" if cg.get("enabled", True) else "✗"
                print(f"{idx}. {Colors.BOLD}{cg['cg_id']}{Colors.END} {enabled}")
                print(f"   Name: {cg['name']}")
                print(f"   Description: {cg['description']}")
                print(f"   PostgreSQL DBs: {', '.join(cg['postgres_databases'])}")
                print(f"   Ceph Buckets: {', '.join(cg['ceph_buckets'])}")
                print(f"   Prefixes: {', '.join(cg['ceph_object_prefixes'])}")
                print(f"   Backup Type: {cg.get('backup_type', 'full')}")
                print()
        else:
            print_error(f"Failed to list CG definitions: {response.text}")
    
    except Exception as e:
        print_error(f"Error: {str(e)}")

def view_cg_definition():
    """View specific CG definition details"""
    print_header("VIEW CONSISTENCY GROUP DEFINITION")
    
    cg_id = get_user_input("\nEnter CG ID", "cg_lakehouse_main")
    
    try:
        response = requests.get(
            f"{ORCHESTRATOR_URL}/consistency-groups/definitions/{cg_id}",
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            cg = data.get("consistency_group", {})
            
            print(f"\n{Colors.BOLD}Consistency Group: {cg['cg_id']}{Colors.END}")
            print(f"Name: {cg['name']}")
            print(f"Description: {cg['description']}")
            print(f"Enabled: {cg.get('enabled', True)}")
            print(f"\nPostgreSQL Databases:")
            for db in cg['postgres_databases']:
                print(f"  - {db}")
            print(f"\nCeph Buckets:")
            for bucket in cg['ceph_buckets']:
                print(f"  - {bucket}")
            print(f"\nObject Prefixes:")
            for prefix in cg['ceph_object_prefixes']:
                print(f"  - {prefix}")
            print(f"\nBackup Type: {cg.get('backup_type', 'full')}")
        else:
            print_error(f"CG not found: {cg_id}")
    
    except Exception as e:
        print_error(f"Error: {str(e)}")

# ============================================================================
# BACKUP OPERATIONS
# ============================================================================

def backup_by_cg_id():
    """Backup using pre-defined Consistency Group"""
    print_header("BACKUP BY CONSISTENCY GROUP ID")
    
    print_info("Fetching available consistency groups...")
    
    try:
        # List available CGs
        response = requests.get(
            f"{ORCHESTRATOR_URL}/consistency-groups/definitions",
            timeout=10
        )
        
        if response.status_code != 200:
            print_error("Failed to fetch CG definitions")
            return
        
        data = response.json()
        cgs = data.get("consistency_groups", [])
        
        if not cgs:
            print_warning("No consistency groups defined!")
            print_info("Edit consistency_groups_config.json to add CGs")
            return
        
        # Show enabled CGs only
        enabled_cgs = [cg for cg in cgs if cg.get("enabled", True)]
        
        if not enabled_cgs:
            print_warning("No enabled consistency groups found")
            return
        
        print(f"\nAvailable Consistency Groups ({len(enabled_cgs)}):\n")
        
        for idx, cg in enumerate(enabled_cgs, 1):
            print(f"{idx}. {Colors.BOLD}{cg['cg_id']}{Colors.END}")
            print(f"   {cg['name']} - {cg['description']}")
            print(f"   DBs: {', '.join(cg['postgres_databases'])}")
            print(f"   Ceph: {', '.join(cg['ceph_buckets'])}")
            print()
        
        # Get user selection
        try:
            choice = int(get_user_input("Select CG number", "1"))
            if choice < 1 or choice > len(enabled_cgs):
                print_error("Invalid selection")
                return
            
            selected_cg = enabled_cgs[choice - 1]
            cg_id = selected_cg['cg_id']
        except ValueError:
            print_error("Invalid input")
            return
        
        print(f"\n{Colors.BOLD}Selected: {cg_id}{Colors.END}")
        print(f"Will backup:")
        print(f"  - PostgreSQL: {', '.join(selected_cg['postgres_databases'])}")
        print(f"  - Ceph: Objects from {', '.join(selected_cg['ceph_buckets'])}")
        
        # Option to override backup type
        current_type = selected_cg.get('backup_type', 'full')
        print(f"\nCurrent backup type: {current_type}")
        override = input(f"Override backup type? (Enter for {current_type}, or type: full/base/incremental): ").strip()
        backup_type = override if override else None
        
        if not confirm_action(f"\nStart backup for CG '{cg_id}'?"):
            print_warning("Backup cancelled")
            return
        
        print_info(f"Starting backup for CG: {cg_id}...")
        
        # Execute backup
        payload = {"cg_id": cg_id}
        if backup_type:
            payload["backup_type"] = backup_type
        
        backup_response = requests.post(
            f"{ORCHESTRATOR_URL}/backup",
            json=payload,
            timeout=180
        )
        
        if backup_response.status_code == 200:
            result = backup_response.json()
            print_success("Backup completed!")
            
            details = result.get("details", {})
            results_data = details.get("results", {})
            
            # Show PostgreSQL results
            pg_backups = results_data.get("postgres_backups", [])
            print(f"\nPostgreSQL Backups ({len(pg_backups)}):")
            for backup in pg_backups:
                status_icon = "✓" if backup.get("success") else "✗"
                print(f"  {status_icon} {backup['database']}: {backup.get('backup_file', 'N/A')}")
            
            # Show Ceph results
            ceph_count = len(results_data.get("ceph_objects", []))
            print(f"\nCeph Objects: {ceph_count} file(s)")
            
            # Show backup metadata
            metadata = results_data.get("backup_metadata", {})
            if metadata:
                backup_id = metadata.get("backup_id")
                print(f"\n{Colors.BOLD}Backup ID: {backup_id}{Colors.END}")
                print(f"Timestamp: {metadata.get('timestamp')}")
                print_success(f"Use this backup ID to restore: {backup_id}")
            
            # Show errors if any
            errors = details.get("errors", [])
            if errors:
                print_warning(f"\nWarnings/Errors ({len(errors)}):")
                for err in errors:
                    print(f"  - {err}")
        else:
            print_error(f"Backup failed: {backup_response.text}")
    
    except Exception as e:
        print_error(f"Error: {str(e)}")

def backup_single_postgres():
    """Backup single PostgreSQL database (without CG)"""
    print_header("BACKUP SINGLE POSTGRESQL DATABASE")
    
    print("Available databases:")
    print("  - users_db")
    print("  - products_db")
    print("  - reports_db")
    
    db_name = get_user_input("\nEnter database name", "users_db")
    backup_type = get_user_input("Backup type (full/base/incremental)", "full")
    
    if not confirm_action(f"Backup {db_name} ({backup_type})?"):
        print_warning("Backup cancelled")
        return
    
    print_info(f"Starting {backup_type} backup for {db_name}...")
    
    try:
        if backup_type == "full":
            response = requests.post(
                f"{POSTGRES_URL}/backup/full",
                json={"db_name": db_name},
                timeout=120
            )
        elif backup_type == "base":
            response = requests.post(f"{POSTGRES_URL}/backup/base", timeout=120)
        elif backup_type == "incremental":
            response = requests.post(f"{POSTGRES_URL}/backup/incremental", timeout=120)
        else:
            print_error("Invalid backup type")
            return
        
        if response.status_code == 200:
            data = response.json()
            print_success("Backup completed!")
            
            backup_file = data.get("backup_file") or data.get("base_backup_name")
            if backup_file:
                print_info(f"Backup file: {backup_file}")
        else:
            print_error(f"Backup failed: {response.text}")
    
    except Exception as e:
        print_error(f"Error: {str(e)}")

# ============================================================================
# RESTORE OPERATIONS
# ============================================================================

def restore_by_backup_id():
    """Restore from a specific backup ID"""
    print_header("RESTORE BY BACKUP ID")
    
    print_info("Fetching available backups...")
    
    try:
        # List available backups
        response = requests.get(
            f"{ORCHESTRATOR_URL}/consistency-groups/backups",
            timeout=10
        )
        
        if response.status_code != 200:
            print_error("Failed to fetch backups")
            return
        
        data = response.json()
        backups = data.get("groups", [])
        
        if not backups:
            print_warning("No backups found!")
            print_info("Create a backup first using option 1")
            return
        
        print(f"\nAvailable Backups ({len(backups)}):\n")
        
        for idx, backup in enumerate(backups[:20], 1):  # Show last 20
            print(f"{idx}. {Colors.BOLD}{backup.get('group_id')}{Colors.END}")
            print(f"   Timestamp: {backup.get('timestamp')}")
            print(f"   Database: {backup.get('postgres_database')}")
            print(f"   PostgreSQL Backup: {backup.get('postgres_backup')}")
            print(f"   Ceph Objects: {len(backup.get('ceph_objects', []))}")
            print()
        
        # Get user selection
        try:
            choice = int(get_user_input("\nSelect backup number", "1"))
            if choice < 1 or choice > len(backups[:20]):
                print_error("Invalid selection")
                return
            
            selected_backup = backups[choice - 1]
            backup_id = selected_backup.get("group_id")
        except ValueError:
            print_error("Invalid input")
            return
        
        print(f"\n{Colors.BOLD}Selected Backup: {backup_id}{Colors.END}")
        print(f"Timestamp: {selected_backup.get('timestamp')}")
        print(f"Database: {selected_backup.get('postgres_database')}")
        print(f"Ceph Objects: {len(selected_backup.get('ceph_objects', []))}")
        
        drop_existing = confirm_action("\nDrop existing database before restore?")
        
        if not confirm_action(f"Proceed with restore from '{backup_id}'?"):
            print_warning("Restore cancelled")
            return
        
        print_info("Starting restore...")
        
        # Execute restore
        restore_response = requests.post(
            f"{ORCHESTRATOR_URL}/restore",
            json={
                "backup_id": backup_id,
                "drop_existing": drop_existing
            },
            timeout=180
        )
        
        if restore_response.status_code == 200:
            result = restore_response.json()
            print_success("Restore completed!")
            
            details = result.get("details", {})
            results = details.get("results", {})
            
            # PostgreSQL result
            if results.get("postgres"):
                pg = results["postgres"]
                if pg.get("success"):
                    print_success(f"PostgreSQL: Restored {pg.get('database')}")
                else:
                    print_error(f"PostgreSQL: {pg.get('error', 'Failed')}")
            
            # Ceph result
            if results.get("ceph"):
                ceph = results["ceph"]
                successful = ceph.get("successful", 0)
                total = ceph.get("total_files", 0)
                print_success(f"Ceph: {successful}/{total} objects restored")
            
            # Errors
            errors = details.get("errors", [])
            if errors:
                print_warning(f"\nWarnings/Errors ({len(errors)}):")
                for err in errors:
                    print(f"  - {err}")
        else:
            print_error(f"Restore failed: {restore_response.text}")
    
    except Exception as e:
        print_error(f"Error: {str(e)}")

def list_backups():
    """List all backup instances"""
    print_header("ALL BACKUP INSTANCES")
    
    try:
        response = requests.get(
            f"{ORCHESTRATOR_URL}/consistency-groups/backups",
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            backups = data.get("groups", [])
            
            if not backups:
                print_warning("No backups found")
                return
            
            print(f"Total Backups: {len(backups)}\n")
            
            for backup in backups:
                print(f"Backup ID: {backup.get('group_id')}")
                print(f"Timestamp: {backup.get('timestamp')}")
                print(f"Database: {backup.get('postgres_database')}")
                print(f"PostgreSQL Backup: {backup.get('postgres_backup')}")
                print(f"Ceph Objects: {len(backup.get('ceph_objects', []))}")
                print(f"Bucket: {backup.get('ceph_bucket_source')}")
                print()
        else:
            print_error(f"Failed to list backups: {response.text}")
    
    except Exception as e:
        print_error(f"Error: {str(e)}")

# ============================================================================
# MAIN MENU
# ============================================================================

def show_menu():
    print_header("LAKEHOUSE CLI v3.0 - CG-BASED")
    
    print(f"{Colors.BOLD}CONSISTENCY GROUP MANAGEMENT:{Colors.END}")
    print("  1.  List Pre-defined CGs (from config)")
    print("  2.  View CG Definition Details")
    
    print(f"\n{Colors.BOLD}BACKUP OPERATIONS:{Colors.END}")
    print("  3.  Backup by CG ID (Full Workflow)")
    print("  4.  Backup Single PostgreSQL Database (No CG)")
    
    print(f"\n{Colors.BOLD}RESTORE OPERATIONS:{Colors.END}")
    print("  5.  Restore by Backup ID")
    print("  6.  List All Backup Instances")
    
    print(f"\n{Colors.BOLD}STATUS & MONITORING:{Colors.END}")
    print("  7.  Check Server Health")
    print("  8.  Show PostgreSQL Status")
    print("  9.  Show Ceph Status")
    
    print(f"\n{Colors.BOLD}OTHER:{Colors.END}")
    print("  0.  Exit")
    print()

def main():
    print_header("LAKEHOUSE CLI v3.0 - CG-BASED BACKUP SYSTEM")
    print_info("Pre-defined Consistency Group approach")
    
    # Check health on startup
    if not check_health():
        print_warning("\nSome servers are not healthy. Continue anyway?")
        if not confirm_action("Continue?"):
            print_info("Exiting...")
            sys.exit(0)
    
    while True:
        show_menu()
        
        choice = get_user_input("Select option", "0")
        
        try:
            if choice == "1":
                list_cg_definitions()
            elif choice == "2":
                view_cg_definition()
            elif choice == "3":
                backup_by_cg_id()
            elif choice == "4":
                backup_single_postgres()
            elif choice == "5":
                restore_by_backup_id()
            elif choice == "6":
                list_backups()
            elif choice == "7":
                check_health()
            elif choice == "8":
                print_info("PostgreSQL Status")
                response = requests.get(f"{POSTGRES_URL}/backups/PG1", timeout=10)
                if response.status_code == 200:
                    print_json(response.json())
            elif choice == "9":
                print_info("Ceph Status")
                response = requests.get(f"{CEPH_URL}/status", timeout=10)
                if response.status_code == 200:
                    print_json(response.json())
            elif choice == "0":
                print_success("\nGoodbye!")
                sys.exit(0)
            else:
                print_error("Invalid option")
        
        except KeyboardInterrupt:
            print_warning("\n\nOperation interrupted")
            if confirm_action("Exit CLI?"):
                print_success("Goodbye!")
                sys.exit(0)
        except Exception as e:
            print_error(f"Unexpected error: {str(e)}")
        
        input(f"\n{Colors.BOLD}Press Enter to continue...{Colors.END}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print_success("\n\nGoodbye!")
        sys.exit(0)