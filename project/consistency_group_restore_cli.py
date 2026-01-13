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
