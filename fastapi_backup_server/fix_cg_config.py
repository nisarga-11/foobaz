#!/usr/bin/env python3
"""
Script to create/update consistency_groups_config.json with your configuration
"""
import json
import os

# Your consistency groups configuration (single bucket: src-slog-bkt1)
CONFIG = {
    "consistency_groups": [
        {
            "cg_id": "cg_lakehouse_main",
            "name": "Main Lakehouse",
            "description": "Primary lakehouse with users database and all S3 objects",
            "postgres_databases": ["users_db"],
            "ceph_buckets": ["src-slog-bkt1"],
            "ceph_object_prefixes": ["*"],
            "backup_type": "full",
            "enabled": True
        },
        {
            "cg_id": "cg_analytics",
            "name": "Analytics Stack",
            "description": "Analytics databases with reports bucket",
            "postgres_databases": ["products_db", "reports_db"],
            "ceph_buckets": ["src-slog-bkt1"],
            "ceph_object_prefixes": ["reports/*", "analytics/*"],
            "backup_type": "full",
            "enabled": True
        },
        {
            "cg_id": "cg_users_only",
            "name": "Users Database",
            "description": "Only users database with user-data bucket",
            "postgres_databases": ["users_db"],
            "ceph_buckets": ["src-slog-bkt1"],
            "ceph_object_prefixes": ["users/*"],
            "backup_type": "full",
            "enabled": True
        },
        {
            "cg_id": "cg_full_system",
            "name": "Complete System Backup",
            "description": "All databases and all S3 buckets",
            "postgres_databases": ["users_db", "products_db", "reports_db"],
            "ceph_buckets": ["src-slog-bkt1"],
            "ceph_object_prefixes": ["*"],
            "backup_type": "base",
            "enabled": True
        }
    ]
}

def write_config(filename="consistency_groups_config.json"):
    """Write the configuration to file"""
    print(f"Writing configuration to: {filename}")
    
    # Check if file exists
    if os.path.exists(filename):
        print(f"⚠️  File already exists: {filename}")
        response = input("Overwrite? (y/N): ").strip().lower()
        if response != 'y':
            print("Cancelled.")
            return False
    
    try:
        with open(filename, 'w') as f:
            json.dump(CONFIG, f, indent=2)
        
        print(f"✓ Successfully wrote configuration to {filename}")
        print(f"\nConfiguration contains {len(CONFIG['consistency_groups'])} consistency groups:")
        
        for cg in CONFIG['consistency_groups']:
            enabled = "✓" if cg.get("enabled", True) else "✗"
            print(f"  {enabled} {cg['cg_id']}")
            print(f"     DBs: {', '.join(cg['postgres_databases'])}")
            print(f"     Bucket: {', '.join(cg['ceph_buckets'])}")
        
        print("\n" + "="*70)
        print("NEXT STEPS:")
        print("="*70)
        print("1. Ensure this file is in the same directory as your orchestrator")
        print("2. Restart the orchestrator service:")
        print("   - Stop the orchestrator (Ctrl+C)")
        print("   - Start it again: python orchestrator.py")
        print("3. Run the lakehouse CLI again and select option 1")
        print()
        
        return True
    
    except Exception as e:
        print(f"✗ Error writing file: {e}")
        return False

def main():
    print("="*70)
    print("Consistency Groups Configuration Creator")
    print("="*70)
    print("\nThis script will create consistency_groups_config.json")
    print("with your 4 consistency groups using bucket: src-slog-bkt1\n")
    
    # Ask for target location
    print("Where should the config file be created?")
    print("1. Current directory (./)")
    print("2. Parent directory (../)")
    print("3. Custom path")
    
    choice = input("\nSelect option [1]: ").strip() or "1"
    
    if choice == "1":
        filename = "./consistency_groups_config.json"
    elif choice == "2":
        filename = "../consistency_groups_config.json"
    elif choice == "3":
        filename = input("Enter full path: ").strip()
    else:
        print("Invalid choice")
        return
    
    if write_config(filename):
        print("✓ Configuration file created successfully!")
        
        # Offer to show the file content
        show = input("\nShow file contents? (y/N): ").strip().lower()
        if show == 'y':
            with open(filename, 'r') as f:
                print("\n" + "="*70)
                print(f.read())
                print("="*70)

if __name__ == "__main__":
    main()