#!/usr/bin/env python3
"""
Lakehouse Orchestrator - Interactive CLI (Corrected v2.1)
Connects to PostgreSQL (8001), Ceph (8000), and Orchestrator (8002)
Aligned with actual orchestrator endpoints and functionality
FIXED: Lakehouse restore now lists consistency groups first
"""

import requests
import json
import sys
from typing import Dict, Any, Optional, List
from datetime import datetime

# ==============================
# CONFIGURATION
# ==============================

ORCHESTRATOR_URL = "http://localhost:8002"
POSTGRES_URL = "http://localhost:8001"
CEPH_URL = "http://localhost:8000"

# ==============================
# COLOR CODES
# ==============================

class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

# ==============================
# ENHANCED API WRAPPER
# ==============================

def api_call(method: str, endpoint: str, base_url: str = None, **kwargs) -> Dict[str, Any]:
    """
    Enhanced API call wrapper with better error handling
    """
    if base_url is None:
        base_url = ORCHESTRATOR_URL
    
    url = f"{base_url}{endpoint}"
    
    try:
        response = requests.request(method, url, **kwargs)
        
        # Handle different HTTP status codes
        if response.status_code == 404:
            return {
                "status": "error",
                "error": "endpoint_not_found",
                "message": f"Endpoint {endpoint} not found on server",
                "suggestion": "This endpoint may not be implemented yet"
            }
        elif response.status_code == 500:
            try:
                error_data = response.json()
                return {
                    "status": "error",
                    "error": "server_error",
                    "message": "Server encountered an error",
                    "details": error_data
                }
            except:
                return {
                    "status": "error",
                    "error": "server_error",
                    "message": f"Server error: {response.text}"
                }
        elif response.status_code >= 400:
            try:
                error_data = response.json()
                return {
                    "status": "error",
                    "error": "client_error",
                    "message": f"Request failed with status {response.status_code}",
                    "details": error_data
                }
            except:
                return {
                    "status": "error",
                    "error": "client_error",
                    "message": f"Request failed: {response.text}"
                }
        
        # Success case
        return response.json()
        
    except requests.exceptions.Timeout:
        return {
            "status": "error",
            "error": "timeout",
            "message": "Request timed out"
        }
    except requests.exceptions.ConnectionError:
        return {
            "status": "error",
            "error": "connection_error",
            "message": f"Cannot connect to {url}"
        }
    except Exception as e:
        return {
            "status": "error",
            "error": "unknown_error",
            "message": str(e)
        }

# ==============================
# API FUNCTIONS - HEALTH & INFO
# ==============================

def check_health() -> Dict[str, Any]:
    """Check system health for all services"""
    health_status = {
        "orchestrator": {"status": "unknown", "url": ORCHESTRATOR_URL},
        "postgresql": {"status": "unknown", "url": POSTGRES_URL},
        "ceph": {"status": "unknown", "url": CEPH_URL}
    }
    
    # Check Orchestrator
    try:
        response = requests.get(f"{ORCHESTRATOR_URL}/health", timeout=5)
        if response.status_code == 200:
            health_status["orchestrator"]["status"] = "healthy"
            health_status["orchestrator"]["details"] = response.json()
        else:
            health_status["orchestrator"]["status"] = "unhealthy"
    except:
        health_status["orchestrator"]["status"] = "unreachable"
    
    # Check PostgreSQL
    try:
        response = requests.get(f"{POSTGRES_URL}/health", timeout=5)
        if response.status_code == 200:
            health_status["postgresql"]["status"] = "healthy"
            health_status["postgresql"]["details"] = response.json()
        else:
            health_status["postgresql"]["status"] = "unhealthy"
    except:
        health_status["postgresql"]["status"] = "unreachable"
    
    # Check Ceph
    try:
        response = requests.get(f"{CEPH_URL}/", timeout=5)
        if response.status_code == 200:
            health_status["ceph"]["status"] = "healthy"
            health_status["ceph"]["details"] = response.json()
        else:
            health_status["ceph"]["status"] = "unhealthy"
    except:
        health_status["ceph"]["status"] = "unreachable"
    
    # Overall status
    all_healthy = all(
        svc["status"] == "healthy" 
        for svc in health_status.values()
    )
    
    return {
        "status": "healthy" if all_healthy else "degraded",
        "services": health_status
    }

def list_backups(target: str) -> Dict[str, Any]:
    """List available backups for a target"""
    return api_call("GET", f"/status/{target}", timeout=10)

def list_lakehouse_consistency_groups() -> Dict[str, Any]:
    """List lakehouse-specific consistency groups"""
    return api_call("GET", "/restore/lakehouse/list-consistency-groups", timeout=10)

def list_consistency_groups() -> Dict[str, Any]:
    """List all consistency groups"""
    return api_call("GET", "/consistency-groups", timeout=10)

def get_consistency_group(group_id: str) -> Dict[str, Any]:
    """Get a specific consistency group"""
    return api_call("GET", f"/consistency-groups/{group_id}", timeout=10)

# ==============================
# API FUNCTIONS - BACKUP
# ==============================

def backup_database(target: str, backup_type: str = "full") -> Dict[str, Any]:
    """Backup a database or lakehouse"""
    return api_call(
        "POST",
        "/backup",
        json={"target": target, "backup_type": backup_type},
        timeout=60
    )

# ==============================
# API FUNCTIONS - RESTORE
# ==============================

def restore_database(target: str, backup_file: str) -> Dict[str, Any]:
    """Restore a specific database from backup file"""
    return api_call(
        "POST",
        "/restore",
        json={"target": target, "backup_file": backup_file},
        timeout=60
    )

def auto_restore(target: str) -> Dict[str, Any]:
    """Auto restore from latest backup"""
    return api_call(
        "POST",
        "/restore/auto",
        json={"target": target},
        timeout=60
    )

def restore_lakehouse_from_cg(consistency_group_id: str, 
                               target_postgres_database: Optional[str] = None,
                               target_ceph_bucket: Optional[str] = None,
                               drop_existing: bool = False) -> Dict[str, Any]:
    """Restore lakehouse from consistency group"""
    payload = {
        "consistency_group_id": consistency_group_id,
        "drop_existing": drop_existing
    }
    if target_postgres_database:
        payload["target_postgres_database"] = target_postgres_database
    if target_ceph_bucket:
        payload["target_ceph_bucket"] = target_ceph_bucket
    
    return api_call(
        "POST",
        "/restore/lakehouse/from-consistency-group",
        json=payload,
        timeout=120
    )

def pitr_restore(base_backup_name: str, target_time: Optional[str] = None) -> Dict[str, Any]:
    """Point-in-time recovery restore"""
    payload = {"base_backup_name": base_backup_name}
    if target_time:
        payload["target_time"] = target_time
    
    return api_call(
        "POST",
        "/restore/pitr",
        json=payload,
        timeout=60
    )

# ==============================
# API FUNCTIONS - STATUS
# ==============================

def get_status(target: str) -> Dict[str, Any]:
    """Get status for a target"""
    return api_call("GET", f"/status/{target}", timeout=10)

# ==============================
# API FUNCTIONS - CEPH
# ==============================

def get_ceph_status() -> Dict[str, Any]:
    """Get Ceph status"""
    return api_call("GET", "/ceph/status", timeout=10)

def list_ceph_files() -> Dict[str, Any]:
    """List Ceph files"""
    return api_call("GET", "/ceph/files", timeout=10)

def parse_ceph_logs(start_time: str, end_time: str, operation_filter: str = "ALL") -> Dict[str, Any]:
    """Parse Ceph logs"""
    return api_call(
        "POST",
        "/ceph/parse",
        json={
            "start_time": start_time,
            "end_time": end_time,
            "operation_filter": operation_filter
        },
        timeout=120
    )

# ==============================
# DISPLAY HELPERS
# ==============================

def print_json(data: Dict[str, Any]):
    """Pretty print JSON data with color coding"""
    formatted = json.dumps(data, indent=2)
    
    # Color code based on status
    if data.get("status") == "success":
        print(f"{Colors.OKGREEN}{formatted}{Colors.ENDC}")
    elif data.get("status") == "error":
        print(f"{Colors.FAIL}{formatted}{Colors.ENDC}")
    else:
        print(formatted)

def print_banner():
    """Print application banner"""
    print(f"""
{Colors.HEADER}{'='*70}
🏠 LAKEHOUSE ORCHESTRATOR - INTERACTIVE CLI (v2.1.0)
{'='*70}{Colors.ENDC}
Orchestrator: {ORCHESTRATOR_URL}
PostgreSQL:   {POSTGRES_URL}
Ceph:         {CEPH_URL}
""")

def print_success(message: str):
    """Print success message"""
    print(f"{Colors.OKGREEN}✓ {message}{Colors.ENDC}")

def print_error(message: str):
    """Print error message"""
    print(f"{Colors.FAIL}✗ {message}{Colors.ENDC}")

def print_warning(message: str):
    """Print warning message"""
    print(f"{Colors.WARNING}⚠ {message}{Colors.ENDC}")

def print_info(message: str):
    """Print info message"""
    print(f"{Colors.OKBLUE}ℹ {message}{Colors.ENDC}")

def print_section(title: str):
    """Print section header"""
    print(f"\n{Colors.BOLD}{Colors.OKCYAN}{'─'*70}")
    print(f"{title}")
    print(f"{'─'*70}{Colors.ENDC}")

def format_consistency_group(cg: Dict[str, Any], index: int = None) -> str:
    """Format consistency group for display"""
    group_id = cg.get("group_id", "unknown")
    timestamp = cg.get("timestamp", "unknown")
    pg_backup = cg.get("postgres_backup", "unknown")
    pg_db = cg.get("postgres_database", "unknown")
    ceph_count = len(cg.get("ceph_objects", []))
    backup_type = cg.get("backup_type", "unknown")
    
    prefix = f"{Colors.BOLD}[{index}]{Colors.ENDC} " if index is not None else ""
    
    return f"""{prefix}{Colors.OKBLUE}Group ID:{Colors.ENDC} {group_id}
    {Colors.OKCYAN}Timestamp:{Colors.ENDC} {timestamp}
    {Colors.OKCYAN}Database:{Colors.ENDC} {pg_db}
    {Colors.OKCYAN}Backup Type:{Colors.ENDC} {backup_type}
    {Colors.OKCYAN}PostgreSQL Backup:{Colors.ENDC} {pg_backup}
    {Colors.OKCYAN}Ceph Objects:{Colors.ENDC} {ceph_count} files"""

# ==============================
# MENU SYSTEM
# ==============================

def show_main_menu():
    """Display main menu"""
    print(f"""
{Colors.BOLD}═══════════════════════════════════════════════════════════════════════
MAIN MENU
═══════════════════════════════════════════════════════════════════════{Colors.ENDC}

{Colors.OKGREEN}BACKUP OPERATIONS:{Colors.ENDC}
  {Colors.OKBLUE}1{Colors.ENDC}. Backup Lakehouse (Full with Consistency Group)
  {Colors.OKBLUE}2{Colors.ENDC}. Backup Database (Custom)

{Colors.OKGREEN}RESTORE OPERATIONS:{Colors.ENDC}
  {Colors.OKBLUE}3{Colors.ENDC}. Auto Restore (Latest Backup)
  {Colors.OKBLUE}4{Colors.ENDC}. Manual Restore (Specific Backup)
  {Colors.OKBLUE}5{Colors.ENDC}. Lakehouse Complete Restore (From Consistency Group)
  {Colors.OKBLUE}6{Colors.ENDC}. Point-in-Time Recovery (PITR)

{Colors.OKGREEN}STATUS & INFO:{Colors.ENDC}
  {Colors.OKBLUE}7{Colors.ENDC}. Check System Health
  {Colors.OKBLUE}8{Colors.ENDC}. Show Lakehouse Status
  {Colors.OKBLUE}9{Colors.ENDC}. Show Backups for Target
  {Colors.OKBLUE}10{Colors.ENDC}. List Lakehouse Consistency Groups

{Colors.OKGREEN}CONSISTENCY GROUPS:{Colors.ENDC}
  {Colors.OKBLUE}11{Colors.ENDC}. List All Consistency Groups
  {Colors.OKBLUE}12{Colors.ENDC}. View Consistency Group Details

{Colors.OKGREEN}CEPH OPERATIONS:{Colors.ENDC}
  {Colors.OKBLUE}13{Colors.ENDC}. Parse Ceph Logs
  {Colors.OKBLUE}14{Colors.ENDC}. List Ceph Files
  {Colors.OKBLUE}15{Colors.ENDC}. Get Ceph Status

{Colors.OKGREEN}OTHER:{Colors.ENDC}
  {Colors.OKBLUE}0{Colors.ENDC}. Exit

{Colors.BOLD}═══════════════════════════════════════════════════════════════════════{Colors.ENDC}
""")

# ==============================
# MENU HANDLERS
# ==============================

def handle_backup_lakehouse():
    """Handle lakehouse backup"""
    print_section("BACKUP LAKEHOUSE")
    print_info("This will backup PostgreSQL users_db and create a consistency group")
    print_info("Backup types: full, base, incremental")
    
    backup_type = input(f"{Colors.BOLD}Enter backup type (default: full): {Colors.ENDC}").strip() or "full"
    
    result = backup_database("lakehouse", backup_type)
    print_json(result)
    
    if result.get("status") == "success":
        print_success("Lakehouse backup completed successfully!")
        
        # Show consistency group info if available
        details = result.get("details", {})
        results = details.get("results", {})
        cg = results.get("consistency_group", {})
        
        if cg.get("success"):
            print_success(f"Consistency Group Created: {cg.get('group_id')}")
            print_info(f"PostgreSQL Backup: {cg.get('postgres_backup')}")
            print_info(f"Ceph Objects: {cg.get('ceph_objects_count', 0)} files")
    elif result.get("status") == "partial_success":
        print_warning("Backup completed with some errors - see details above")
    else:
        print_error("Backup failed - see details above")

def handle_backup_database():
    """Handle custom database backup"""
    print_section("BACKUP DATABASE")
    print_info("Available targets: lakehouse, users_db, products_db, reports_db")
    target = input(f"{Colors.BOLD}Enter target: {Colors.ENDC}").strip()
    
    print_info("Backup types: full, base, incremental")
    backup_type = input(f"{Colors.BOLD}Enter backup type (default: full): {Colors.ENDC}").strip() or "full"
    
    result = backup_database(target, backup_type)
    print_json(result)
    
    if result.get("status") == "success":
        print_success(f"Backup completed for {target}!")
    else:
        print_error("Backup failed - see details above")

def handle_auto_restore():
    """Handle auto restore from latest backup"""
    print_section("AUTO RESTORE (Latest Backup)")
    print_info("Available targets: lakehouse, users_db, products_db, reports_db")
    target = input(f"{Colors.BOLD}Enter target to restore: {Colors.ENDC}").strip()
    
    confirm = input(f"{Colors.WARNING}⚠ This will restore {target} from the latest backup. Continue? (yes/no): {Colors.ENDC}").strip().lower()
    
    if confirm != "yes":
        print_info("Restore cancelled")
        return
    
    result = auto_restore(target)
    print_json(result)
    
    if result.get("status") == "success":
        print_success(f"Auto restore completed for {target}!")
    else:
        print_error("Restore failed - see details above")

def handle_manual_restore():
    """Handle manual restore from specific backup"""
    print_section("MANUAL RESTORE (Specific Backup)")
    
    print_info("Available targets: lakehouse, users_db, products_db, reports_db")
    target = input(f"{Colors.BOLD}Enter target to restore: {Colors.ENDC}").strip()
    
    # Special handling for lakehouse - use consistency groups
    if target.lower() == "lakehouse":
        print_info("\n⚠️  For lakehouse restores, you should use consistency groups")
        print_info("This ensures PostgreSQL and Ceph data are from the same point in time")
        
        choice = input(f"\n{Colors.BOLD}Options:{Colors.ENDC}\n  1. Use Consistency Group (Recommended)\n  2. Manual PostgreSQL-only restore\n  3. Cancel\n\n{Colors.BOLD}Select option: {Colors.ENDC}").strip()
        
        if choice == "1":
            # Redirect to consistency group restore
            print_info("\nRedirecting to Consistency Group Restore...")
            input(f"\n{Colors.OKCYAN}Press Enter to continue...{Colors.ENDC}")
            handle_lakehouse_restore()
            return
        elif choice == "2":
            print_warning("\n⚠️  This will ONLY restore PostgreSQL database, NOT Ceph files!")
            print_warning("PostgreSQL and Ceph may be out of sync after this restore")
            
            proceed = input(f"\n{Colors.WARNING}Continue with PostgreSQL-only restore? (yes/no): {Colors.ENDC}").strip().lower()
            if proceed != "yes":
                print_info("Restore cancelled")
                return
            # Continue with manual restore below
        else:
            print_info("Restore cancelled")
            return
    
    # Show available backups for this target
    print_info(f"\nFetching available backups for {target}...")
    backups_result = get_status(target)
    
    if backups_result.get("status") == "success":
        print_json(backups_result)
    
    backup_file = input(f"\n{Colors.BOLD}Enter backup filename: {Colors.ENDC}").strip()
    
    if not backup_file:
        print_error("Backup filename is required")
        return
    
    confirm = input(f"{Colors.WARNING}⚠ This will restore {target} from {backup_file}. Continue? (yes/no): {Colors.ENDC}").strip().lower()
    
    if confirm != "yes":
        print_info("Restore cancelled")
        return
    
    result = restore_database(target, backup_file)
    print_json(result)
    
    if result.get("status") == "success":
        print_success(f"Manual restore completed for {target}!")
    else:
        print_error("Restore failed - see details above")

def handle_lakehouse_restore():
    """
    CORRECTED: Handle complete lakehouse restore from consistency group
    This now lists consistency groups FIRST and lets user select one
    """
    print_section("LAKEHOUSE COMPLETE RESTORE (From Consistency Group)")
    print_info("This will restore both PostgreSQL users_db and Ceph files from a consistency group")
    print_info("Consistency groups ensure PostgreSQL and Ceph data are from the same point in time")
    
    # Step 1: List available lakehouse consistency groups
    print_info("\nFetching available lakehouse consistency groups...")
    cg_result = list_lakehouse_consistency_groups()
    
    if cg_result.get("status") != "success":
        print_error("Failed to fetch consistency groups")
        print_json(cg_result)
        return
    
    groups = cg_result.get("details", {}).get("consistency_groups", [])
    
    if not groups:
        print_warning("No lakehouse consistency groups found!")
        print_info("You need to create a lakehouse backup first (Option 1)")
        return
    
    # Step 2: Display consistency groups with numbered list
    print(f"\n{Colors.BOLD}Available Lakehouse Consistency Groups:{Colors.ENDC}\n")
    
    for idx, cg in enumerate(groups, 1):
        print(format_consistency_group(cg, idx))
        print()
    
    print(f"{Colors.BOLD}Total: {len(groups)} consistency group(s){Colors.ENDC}\n")
    
    # Step 3: Let user select consistency group
    while True:
        selection = input(f"{Colors.BOLD}Select consistency group number (1-{len(groups)}) or 'q' to quit: {Colors.ENDC}").strip()
        
        if selection.lower() == 'q':
            print_info("Restore cancelled")
            return
        
        try:
            selection_idx = int(selection) - 1
            if 0 <= selection_idx < len(groups):
                selected_cg = groups[selection_idx]
                break
            else:
                print_error(f"Please enter a number between 1 and {len(groups)}")
        except ValueError:
            print_error("Please enter a valid number")
    
    # Step 4: Show selected consistency group details
    print(f"\n{Colors.OKGREEN}Selected Consistency Group:{Colors.ENDC}")
    print(format_consistency_group(selected_cg))
    print()
    
    group_id = selected_cg.get("group_id")
    
    # Step 5: Advanced options (optional)
    print(f"\n{Colors.BOLD}Advanced Options (press Enter to use defaults):{Colors.ENDC}")
    
    target_db = input(f"{Colors.OKCYAN}Target PostgreSQL database (default: {selected_cg.get('postgres_database')}): {Colors.ENDC}").strip()
    target_bucket = input(f"{Colors.OKCYAN}Target Ceph bucket (default: {selected_cg.get('ceph_bucket_source')}): {Colors.ENDC}").strip()
    
    drop_choice = input(f"{Colors.WARNING}Drop existing database before restore? (yes/no, default: no): {Colors.ENDC}").strip().lower()
    drop_existing = drop_choice == "yes"
    
    # Step 6: Final confirmation
    print(f"\n{Colors.BOLD}Restore Summary:{Colors.ENDC}")
    print(f"  Consistency Group: {group_id}")
    print(f"  From Timestamp: {selected_cg.get('timestamp')}")
    print(f"  PostgreSQL Database: {target_db or selected_cg.get('postgres_database')}")
    print(f"  Ceph Bucket: {target_bucket or selected_cg.get('ceph_bucket_source')}")
    print(f"  Drop Existing: {drop_existing}")
    print()
    
    confirm = input(f"{Colors.WARNING}⚠ Proceed with lakehouse restore? (yes/no): {Colors.ENDC}").strip().lower()
    
    if confirm != "yes":
        print_info("Restore cancelled")
        return
    
    # Step 7: Execute restore
    print_info(f"\nRestoring lakehouse from consistency group {group_id}...")
    
    result = restore_lakehouse_from_cg(
        consistency_group_id=group_id,
        target_postgres_database=target_db or None,
        target_ceph_bucket=target_bucket or None,
        drop_existing=drop_existing
    )
    
    print_json(result)
    
    # Step 8: Show result
    if result.get("status") == "success":
        print_success("✅ Lakehouse restore completed successfully!")
        
        details = result.get("details", {})
        results = details.get("results", {})
        
        # Show PostgreSQL restore status
        pg_result = results.get("postgres", {})
        if pg_result and pg_result.get("success"):
            print_success(f"  PostgreSQL: Restored to {details.get('target_database')}")
        
        # Show Ceph restore status
        ceph_result = results.get("ceph", {})
        if ceph_result:
            successful = ceph_result.get("successful", 0)
            failed = ceph_result.get("failed", 0)
            print_success(f"  Ceph: {successful} files restored, {failed} failed")
            
    elif result.get("status") == "partial_success":
        print_warning("⚠️ Lakehouse restore completed with some issues")
        print_info("Review the details above for more information")
    else:
        print_error("❌ Lakehouse restore failed")
        print_info("Review the error details above")

def handle_pitr_restore():
    """Handle point-in-time recovery restore"""
    print_section("POINT-IN-TIME RECOVERY (PITR)")
    base_backup = input(f"{Colors.BOLD}Enter base backup name: {Colors.ENDC}").strip()
    target_time = input(f"{Colors.BOLD}Enter target time (optional, format: YYYY-MM-DD HH:MM:SS): {Colors.ENDC}").strip() or None
    
    result = pitr_restore(base_backup, target_time)
    print_json(result)
    
    if result.get("status") == "success":
        print_success("PITR restore validated!")
    else:
        print_error("PITR restore failed - see details above")

def handle_health_check():
    """Handle health check"""
    print_section("SYSTEM HEALTH CHECK")
    
    result = check_health()
    
    # Display service status with colors
    print(f"\n{Colors.BOLD}Service Status:{Colors.ENDC}\n")
    
    for service_name, service_data in result.get("services", {}).items():
        status = service_data.get("status", "unknown")
        url = service_data.get("url", "")
        
        if status == "healthy":
            status_icon = f"{Colors.OKGREEN}✓{Colors.ENDC}"
            status_text = f"{Colors.OKGREEN}HEALTHY{Colors.ENDC}"
        elif status == "unreachable":
            status_icon = f"{Colors.FAIL}✗{Colors.ENDC}"
            status_text = f"{Colors.FAIL}UNREACHABLE{Colors.ENDC}"
        else:
            status_icon = f"{Colors.WARNING}⚠{Colors.ENDC}"
            status_text = f"{Colors.WARNING}UNHEALTHY{Colors.ENDC}"
        
        print(f"  {status_icon} {service_name.upper():15} {status_text:20} {url}")
    
    print(f"\n{Colors.BOLD}Overall Status:{Colors.ENDC} ", end="")
    if result.get("status") == "healthy":
        print(f"{Colors.OKGREEN}ALL SYSTEMS OPERATIONAL{Colors.ENDC}")
    else:
        print(f"{Colors.WARNING}DEGRADED - Some services unavailable{Colors.ENDC}")

def handle_lakehouse_status():
    """Handle lakehouse status"""
    print_section("LAKEHOUSE STATUS")
    
    result = get_status("lakehouse")
    print_json(result)

def handle_show_backups():
    """Handle show backups for target"""
    print_section("SHOW BACKUPS")
    
    print_info("Available targets: lakehouse, users_db, products_db, reports_db")
    target = input(f"{Colors.BOLD}Enter target: {Colors.ENDC}").strip()
    
    result = get_status(target)
    print_json(result)

def handle_list_lakehouse_cgs():
    """Handle list lakehouse consistency groups"""
    print_section("LAKEHOUSE CONSISTENCY GROUPS")
    
    result = list_lakehouse_consistency_groups()
    
    if result.get("status") == "success":
        groups = result.get("details", {}).get("consistency_groups", [])
        
        if not groups:
            print_warning("No lakehouse consistency groups found")
            print_info("Create a lakehouse backup to generate consistency groups")
            return
        
        print(f"\n{Colors.BOLD}Found {len(groups)} lakehouse consistency group(s):{Colors.ENDC}\n")
        
        for idx, cg in enumerate(groups, 1):
            print(format_consistency_group(cg, idx))
            print()
    else:
        print_error("Failed to fetch lakehouse consistency groups")
        print_json(result)

def handle_list_consistency_groups():
    """Handle list all consistency groups"""
    print_section("ALL CONSISTENCY GROUPS")
    
    result = list_consistency_groups()
    print_json(result)
    
    if result.get("status") == "success":
        groups = result.get("groups", [])
        print_info(f"Total consistency groups: {len(groups)}")

def handle_view_consistency_group():
    """Handle view consistency group details"""
    print_section("CONSISTENCY GROUP DETAILS")
    
    # First list available groups
    result = list_consistency_groups()
    if result.get("status") == "success":
        print_json(result)
    
    group_id = input(f"{Colors.BOLD}Enter consistency group ID: {Colors.ENDC}").strip()
    
    result = get_consistency_group(group_id)
    print_json(result)

def handle_parse_ceph_logs():
    """Handle Ceph log parsing"""
    print_section("PARSE CEPH LOGS")
    print_info("Time format: DD/MMM/YYYY:HH:MM:SS (e.g., 06/Nov/2025:04:00:00)")
    
    start_time = input(f"{Colors.BOLD}Enter start time: {Colors.ENDC}").strip()
    end_time = input(f"{Colors.BOLD}Enter end time: {Colors.ENDC}").strip()
    operation = input(f"{Colors.BOLD}Operation filter (ALL/PUT/GET/DELETE, default: ALL): {Colors.ENDC}").strip() or "ALL"
    
    result = parse_ceph_logs(start_time, end_time, operation)
    print_json(result)
    
    if result.get("status") == "success":
        print_success("Ceph log parsing completed!")
    else:
        print_error("Ceph log parsing failed - see details above")

def handle_list_ceph_files():
    """Handle list Ceph files"""
    print_section("CEPH FILES")
    
    result = list_ceph_files()
    print_json(result)

def handle_get_ceph_status():
    """Handle get Ceph status"""
    print_section("CEPH STATUS")
    
    result = get_ceph_status()
    print_json(result)

# ==============================
# MAIN HANDLER
# ==============================

def handle_choice(choice: str):
    """Route choice to appropriate handler"""
    handlers = {
        "1": handle_backup_lakehouse,
        "2": handle_backup_database,
        "3": handle_auto_restore,
        "4": handle_manual_restore,
        "5": handle_lakehouse_restore,
        "6": handle_pitr_restore,
        "7": handle_health_check,
        "8": handle_lakehouse_status,
        "9": handle_show_backups,
        "10": handle_list_lakehouse_cgs,
        "11": handle_list_consistency_groups,
        "12": handle_view_consistency_group,
        "13": handle_parse_ceph_logs,
        "14": handle_list_ceph_files,
        "15": handle_get_ceph_status,
        "0": lambda: sys.exit(0)
    }
    
    handler = handlers.get(choice)
    if handler:
        handler()
    else:
        print_error("Invalid option")

# ==============================
# MAIN LOOP
# ==============================

def main():
    """Main application loop"""
    print_banner()
    
    # Initial health check
    print_info("Checking service connectivity...")
    health = check_health()
    
    services_ok = 0
    services_total = len(health.get("services", {}))
    
    for service_name, service_data in health.get("services", {}).items():
        status = service_data.get("status")
        if status == "healthy":
            print_success(f"{service_name.upper()} connected")
            services_ok += 1
        elif status == "unreachable":
            print_error(f"{service_name.upper()} unreachable")
        else:
            print_warning(f"{service_name.upper()} unhealthy")
    
    print(f"\n{Colors.BOLD}Services Status: {services_ok}/{services_total} operational{Colors.ENDC}\n")
    
    if services_ok == 0:
        print_error("Cannot connect to any services - please check that services are running")
        print_info("Press Enter to continue anyway or Ctrl+C to exit")
        try:
            input()
        except KeyboardInterrupt:
            sys.exit(1)
    elif services_ok < services_total:
        print_warning("Some services are unavailable - limited functionality")
        print_info("Continuing with degraded mode...")
    
    # Main loop
    while True:
        try:
            show_main_menu()
            choice = input(f"{Colors.BOLD}➜ Select option: {Colors.ENDC}").strip()
            handle_choice(choice)
            
            input(f"\n{Colors.OKCYAN}Press Enter to continue...{Colors.ENDC}")
            
        except KeyboardInterrupt:
            print(f"\n\n{Colors.OKGREEN}Goodbye!{Colors.ENDC}")
            sys.exit(0)
        except Exception as e:
            print_error(f"Unexpected error: {str(e)}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()