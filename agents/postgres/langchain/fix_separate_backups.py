#!/usr/bin/env python3
"""
Script to fix separate backup configuration for customer and employee servers.
"""

import os
import subprocess
import sys

def run_command(cmd, env=None):
    """Run a shell command and return the result."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, env=env)
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)

def main():
    print("🔧 Fixing separate backup configuration...")
    
    # 1. Create separate backup directories
    print("📁 Creating separate backup directories...")
    customer_backup_dir = "/Users/aarthiprashanth/backups/pgbackrest_customer"
    employee_backup_dir = "/Users/aarthiprashanth/backups/pgbackrest_employee"
    
    os.makedirs(customer_backup_dir, exist_ok=True)
    os.makedirs(employee_backup_dir, exist_ok=True)
    print(f"✅ Created {customer_backup_dir}")
    print(f"✅ Created {employee_backup_dir}")
    
    # 2. Create stanzas for each server
    print("🏗️ Creating separate stanzas...")
    
    # Customer server stanza
    customer_env = {
        "PGBACKREST_PG1_PATH": "/Users/aarthiprashanth/Library/Application Support/Postgres/var-17",
        "PGBACKREST_REPO1_PATH": customer_backup_dir
    }
    
    success, stdout, stderr = run_command(
        "pgbackrest --stanza=customer_demo stanza-create",
        env=customer_env
    )
    if success:
        print("✅ Created customer_demo stanza")
    else:
        print(f"⚠️ Customer stanza creation: {stderr}")
    
    # Employee server stanza
    employee_env = {
        "PGBACKREST_PG1_PATH": "/Users/aarthiprashanth/Library/Application Support/Postgres/var-17",
        "PGBACKREST_REPO1_PATH": employee_backup_dir
    }
    
    success, stdout, stderr = run_command(
        "pgbackrest --stanza=employee_demo stanza-create",
        env=employee_env
    )
    if success:
        print("✅ Created employee_demo stanza")
    else:
        print(f"⚠️ Employee stanza creation: {stderr}")
    
    # 3. Update PostgreSQL archive command to use customer_demo (primary)
    print("🔧 Updating PostgreSQL archive command...")
    
    archive_cmd = f"/opt/homebrew/bin/pgbackrest --stanza=customer_demo --repo1-path={customer_backup_dir} archive-push %p"
    
    success, stdout, stderr = run_command(
        f'psql -h localhost -p 5432 -U postgres -d postgres -c "ALTER SYSTEM SET archive_command = \'{archive_cmd}\';"'
    )
    
    if success:
        print("✅ Updated archive command")
        
        # Reload configuration
        success, stdout, stderr = run_command(
            'psql -h localhost -p 5432 -U postgres -d postgres -c "SELECT pg_reload_conf();"'
        )
        if success:
            print("✅ Reloaded PostgreSQL configuration")
        else:
            print(f"❌ Failed to reload config: {stderr}")
    else:
        print(f"❌ Failed to update archive command: {stderr}")
    
    # 4. Test creating backups for both servers
    print("🧪 Testing backup creation...")
    
    # Test customer backup
    success, stdout, stderr = run_command(
        "pgbackrest --stanza=customer_demo --type=full backup",
        env=customer_env
    )
    if success:
        print("✅ Customer server backup test successful")
    else:
        print(f"⚠️ Customer backup test: {stderr}")
    
    # Test employee backup
    success, stdout, stderr = run_command(
        "pgbackrest --stanza=employee_demo --type=full backup",
        env=employee_env
    )
    if success:
        print("✅ Employee server backup test successful")
    else:
        print(f"⚠️ Employee backup test: {stderr}")
    
    print("\n🎉 Separate backup configuration completed!")
    print(f"📁 Customer backups: {customer_backup_dir}")
    print(f"📁 Employee backups: {employee_backup_dir}")
    print("\n⚠️ Note: You'll need to restart the backup scheduler for changes to take effect.")

if __name__ == "__main__":
    main()
