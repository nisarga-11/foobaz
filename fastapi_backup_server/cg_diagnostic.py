#!/usr/bin/env python3
"""
Diagnostic script to check Consistency Group configuration
"""
import json
import os
import requests

# Colors
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'

def check_local_config():
    """Check if local config file exists and is valid"""
    config_files = [
        "consistency_groups_config.json",
        "./consistency_groups_config.json",
        "../consistency_groups_config.json"
    ]
    
    print(f"\n{Colors.BOLD}=== Checking Local Config Files ==={Colors.END}")
    
    for config_file in config_files:
        if os.path.exists(config_file):
            print(f"{Colors.GREEN}✓ Found: {config_file}{Colors.END}")
            try:
                with open(config_file, 'r') as f:
                    data = json.load(f)
                    cgs = data.get("consistency_groups", [])
                    print(f"  Total CGs in file: {len(cgs)}")
                    for cg in cgs:
                        enabled = "✓" if cg.get("enabled", True) else "✗"
                        print(f"    {enabled} {cg['cg_id']}: {cg['name']}")
                    return config_file, data
            except Exception as e:
                print(f"{Colors.RED}✗ Error reading {config_file}: {e}{Colors.END}")
        else:
            print(f"{Colors.YELLOW}  Not found: {config_file}{Colors.END}")
    
    return None, None

def check_orchestrator_config():
    """Check what the orchestrator is returning"""
    print(f"\n{Colors.BOLD}=== Checking Orchestrator API ==={Colors.END}")
    
    try:
        response = requests.get("http://localhost:8002/consistency-groups/definitions", timeout=10)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            cgs = data.get("consistency_groups", [])
            print(f"{Colors.GREEN}✓ API Response: {len(cgs)} CGs{Colors.END}")
            
            for cg in cgs:
                enabled = "✓" if cg.get("enabled", True) else "✗"
                print(f"  {enabled} {cg['cg_id']}: {cg['name']}")
                print(f"     Buckets: {cg.get('ceph_buckets', [])}")
            
            return data
        else:
            print(f"{Colors.RED}✗ API Error: {response.text}{Colors.END}")
            return None
    except Exception as e:
        print(f"{Colors.RED}✗ Cannot reach orchestrator: {e}{Colors.END}")
        return None

def compare_configs(local_data, api_data):
    """Compare local file vs API response"""
    print(f"\n{Colors.BOLD}=== Comparison ==={Colors.END}")
    
    if not local_data or not api_data:
        print(f"{Colors.YELLOW}Cannot compare - missing data{Colors.END}")
        return
    
    local_cgs = {cg['cg_id']: cg for cg in local_data.get("consistency_groups", [])}
    api_cgs = {cg['cg_id']: cg for cg in api_data.get("consistency_groups", [])}
    
    local_ids = set(local_cgs.keys())
    api_ids = set(api_cgs.keys())
    
    missing_in_api = local_ids - api_ids
    extra_in_api = api_ids - local_ids
    
    if missing_in_api:
        print(f"{Colors.RED}Missing in API (but in local file):{Colors.END}")
        for cg_id in missing_in_api:
            print(f"  - {cg_id}: {local_cgs[cg_id]['name']}")
    
    if extra_in_api:
        print(f"{Colors.YELLOW}Extra in API (not in local file):{Colors.END}")
        for cg_id in extra_in_api:
            print(f"  - {cg_id}: {api_cgs[cg_id]['name']}")
    
    # Check for differences in common CGs
    common_ids = local_ids & api_ids
    for cg_id in common_ids:
        local_cg = local_cgs[cg_id]
        api_cg = api_cgs[cg_id]
        
        # Compare buckets
        local_buckets = set(local_cg.get('ceph_buckets', []))
        api_buckets = set(api_cg.get('ceph_buckets', []))
        
        if local_buckets != api_buckets:
            print(f"{Colors.YELLOW}Bucket mismatch for {cg_id}:{Colors.END}")
            print(f"  Local:  {local_buckets}")
            print(f"  API:    {api_buckets}")

def main():
    print(f"\n{Colors.BOLD}{'='*70}{Colors.END}")
    print(f"{Colors.BOLD}CG Configuration Diagnostic Tool{Colors.END}")
    print(f"{Colors.BOLD}{'='*70}{Colors.END}")
    
    # Check local config
    config_file, local_data = check_local_config()
    
    # Check orchestrator API
    api_data = check_orchestrator_config()
    
    # Compare
    compare_configs(local_data, api_data)
    
    # Recommendations
    print(f"\n{Colors.BOLD}=== Recommendations ==={Colors.END}")
    
    if not config_file:
        print(f"{Colors.RED}1. Create consistency_groups_config.json in the project root{Colors.END}")
    
    if api_data:
        api_cgs = api_data.get("consistency_groups", [])
        if len(api_cgs) == 1 and local_data and len(local_data.get("consistency_groups", [])) > 1:
            print(f"{Colors.YELLOW}2. Orchestrator is not reading the updated config file{Colors.END}")
            print(f"   Solutions:")
            print(f"   - Restart the orchestrator service")
            print(f"   - Check if orchestrator is reading from a different location")
            print(f"   - Verify file permissions on consistency_groups_config.json")
    else:
        print(f"{Colors.RED}3. Orchestrator is not responding - ensure it's running{Colors.END}")
    
    print()

if __name__ == "__main__":
    main()