#!/usr/bin/env python3
"""
Upload PostgreSQL backup files to IBM Storage Protect
"""

import os
import sys
import asyncio
from pathlib import Path
from datetime import datetime

# Add parent directory to path to import StorageProtectClient
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from storage_protect_client import StorageProtectClient

# Fixed paths - backups are in crew_mcp_project/backups, not postgres/backups
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # crew_mcp_project directory
BACKUPS_DIR = os.path.join(BASE_DIR, "backups")


async def upload_postgres_backups_to_sp():
    """Upload PostgreSQL backup files to IBM Storage Protect."""
    try:
        # Initialize Storage Protect client for FOOBAZ
        sp_client = StorageProtectClient(
            base_url="https://9.11.52.248:9081/DpVe/api",
            node_id="FOOBAZ",
            node_admin_id="spectrumprotect",
            password="admin_12345_admin",
            http_port="1581",
            sp_backup_directory="/sp_backups/postgres_backups",
            auto_cleanup_tasks=False
        )

        print("=" * 70)
        print("  SIGNING ON TO IBM STORAGE PROTECT")
        print("=" * 70)
        await sp_client.sign_on()
        print(f"SUCCESS: Signed on to {sp_client.server_name} (Node: {sp_client.node_name})\n")

        # Define directories to search for backup files
        search_dirs = [
            os.path.join(BACKUPS_DIR, "agent1"),
            os.path.join(BACKUPS_DIR, "agent2"),
            os.path.join(BACKUPS_DIR, "backup", "pg1_17"),
            os.path.join(BACKUPS_DIR, "backup", "pg2_17"),
        ]

        # Also search for .tar.gz files in the base backups directory
        print(f"Searching for backup files in: {BACKUPS_DIR}")
        print(f"Search directories:")
        for d in search_dirs:
            exists = "✓" if os.path.exists(d) else "✗"
            print(f"  {exists} {d}")
        print()

        # Gather all backup files (.backup, .dump, .tar.gz)
        extensions = [".backup", ".dump", ".tar.gz"]
        backup_files = []
        
        # Search in subdirectories
        for directory in search_dirs:
            if os.path.exists(directory):
                for ext in extensions:
                    found = list(Path(directory).glob(f"*{ext}"))
                    backup_files.extend(found)
                    if found:
                        dir_name = directory.replace(BACKUPS_DIR + "/", "")
                        print(f"Found {len(found)} *{ext} file(s) in {dir_name}")
        
        # Also search for .tar.gz in base backups directory
        base_tar_files = list(Path(BACKUPS_DIR).glob("*.tar.gz"))
        if base_tar_files:
            backup_files.extend(base_tar_files)
            print(f"Found {len(base_tar_files)} *.tar.gz file(s) in base directory")

        if not backup_files:
            print("\nWARNING: No PostgreSQL backup files found.")
            print(f"Searched in: {BACKUPS_DIR}")
            print(f"Looking for extensions: {extensions}")
            await sp_client.close()
            return

        print(f"\n{'=' * 70}")
        print(f"Found {len(backup_files)} backup file(s) to upload:")
        print(f"{'=' * 70}")
        
        total_size = 0
        for f in backup_files:
            size = f.stat().st_size
            total_size += size
            rel_path = str(f).replace(BACKUPS_DIR + "/", "")
            print(f"  • {rel_path:50s} {size:>12,} bytes")
        
        print(f"\nTotal size: {total_size:,} bytes ({total_size / (1024*1024):.2f} MB)")

        # Create backup name with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_name = f"postgres_backups_{timestamp}"

        print(f"\n{'=' * 70}")
        print(f"  STARTING BACKUP UPLOAD: {backup_name}")
        print(f"{'=' * 70}")
        print(f"Source root: {BACKUPS_DIR}")
        print(f"Target: /sp_backups/postgres_backups/{backup_name}")
        print()

        # Convert to string paths for upload
        file_paths = [str(f) for f in backup_files]

        # Start upload process
        backup_result = await sp_client.start_backup(
            backup_path=BACKUPS_DIR,
            backup_name=backup_name,
            backup_type="postgres_backups",
            file_list=file_paths
        )

        task_id = backup_result.get("taskId")
        print(f"\nBackup task started with ID: {task_id}")
        print(f"Waiting for backup to complete (max 10 minutes)...\n")
        
        status_result = await sp_client.wait_for_task_completion(task_id, max_wait_minutes=10)

        task_state = status_result.get("taskState")
        
        print(f"\n{'=' * 70}")
        if task_state == "Success":
            print("  ✓ POSTGRES BACKUP UPLOAD SUCCESSFUL")
        else:
            print(f"  ✗ BACKUP UPLOAD {task_state.upper()}")
        print(f"{'=' * 70}")
        
        # Get detailed results
        task_data = await sp_client.get_task_data(task_id)
        backup_jobs = task_data.get("backupJobInfoList", [])
        
        if backup_jobs:
            job_info = backup_jobs[0]
            total_files = job_info.get("totalFiles", 0)
            completed = job_info.get("totalCompletedFiles", 0)
            failed = job_info.get("totalFailedFiles", 0)
            total_bytes = job_info.get("totalBytes", 0)

            print(f"\nBackup Statistics:")
            print(f"  Files processed:  {completed}/{total_files}")
            print(f"  Files failed:     {failed}")
            print(f"  Total uploaded:   {total_bytes:,} bytes ({total_bytes / (1024*1024):.2f} MB)")
            print(f"  Task ID:          {task_id}")
            
            if failed > 0:
                print(f"\n⚠ WARNING: {failed} file(s) failed to backup")
                
                # Show directory backup details if available
                dir_backups = job_info.get("baClientDirBackupStatusInfoList", [])
                if dir_backups:
                    print("\nDetailed Status:")
                    for dir_backup in dir_backups:
                        status = dir_backup.get("status", "Unknown")
                        files_backed_up = dir_backup.get("filesBackedUp", 0)
                        files_failed = dir_backup.get("filesFailed", 0)
                        
                        status_icon = "✓" if status == "Success" else "✗"
                        print(f"  {status_icon} Status: {status}")
                        print(f"    Files backed up: {files_backed_up}")
                        print(f"    Files failed: {files_failed}")
        else:
            print("\nNo detailed job information available")
            if task_state != "Success":
                error_msg = status_result.get("statusMessage", "Unknown error")
                print(f"Error: {error_msg}")

        # Cleanup if enabled
        if sp_client.auto_cleanup_tasks:
            print("\nCleaning up task...")
            await sp_client.delete_task(task_id)

        await sp_client.close()
        
        if task_state == "Success" and failed == 0:
            print("\n✓ All backup files uploaded successfully!")
            return 0
        else:
            print("\n⚠ Backup completed with warnings or errors")
            return 1

    except Exception as e:
        print(f"\n{'=' * 70}")
        print(f"  ✗ ERROR: Failed to upload to Storage Protect")
        print(f"{'=' * 70}")
        print(f"{e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    exit_code = asyncio.run(upload_postgres_backups_to_sp())
    sys.exit(exit_code if exit_code else 0)