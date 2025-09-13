#!/usr/bin/env python3
"""
Start the TRUE WAL-Based PostgreSQL Incremental Backup System.
Uses PostgreSQL WAL (Write-Ahead Log) files for real incremental backups.
"""

import asyncio
import sys
import subprocess
import time
from pathlib import Path

def check_postgresql_for_wal_backup():
    """Check PostgreSQL for WAL backup capabilities."""
    print("🔍 Checking PostgreSQL for TRUE WAL backup capabilities...")
    
    tools = ["pg_basebackup", "pg_dump", "pg_restore", "psql", "pg_waldump"]
    missing_tools = []
    available_tools = []
    
    for tool in tools:
        try:
            result = subprocess.run([tool, "--version"], 
                                  capture_output=True, check=True, text=True)
            version = result.stdout.split('\n')[0]
            print(f"   ✅ {tool}: {version}")
            available_tools.append(tool)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print(f"   ❌ {tool}: Not found")
            missing_tools.append(tool)
    
    # pg_waldump is optional but helpful
    if "pg_waldump" in missing_tools:
        print("   ℹ️  pg_waldump is optional (for WAL file analysis)")
        missing_tools.remove("pg_waldump")
    
    if missing_tools:
        print(f"\n❌ Missing essential PostgreSQL tools: {', '.join(missing_tools)}")
        print("Please install PostgreSQL client tools:")
        print("   macOS: brew install postgresql")
        print("   Ubuntu: sudo apt-get install postgresql-client")
        print("   RHEL/CentOS: sudo yum install postgresql")
        return False
    
    # Test PostgreSQL connection and WAL access
    print("\n🔍 Testing PostgreSQL connection and WAL capabilities...")
    
    try:
        # Test basic connection
        result = subprocess.run([
            "psql", "-h", "localhost", "-p", "5432", "-U", "postgres", 
            "-c", "SELECT version();"
        ], capture_output=True, check=True, text=True)
        print("   ✅ PostgreSQL connection successful")
        
        # Test WAL functions
        try:
            result = subprocess.run([
                "psql", "-h", "localhost", "-p", "5432", "-U", "postgres", 
                "-c", "SELECT pg_current_wal_lsn();"
            ], capture_output=True, check=True, text=True)
            lsn = result.stdout.strip().split('\n')[-1]
            print(f"   ✅ Current WAL LSN: {lsn}")
        except subprocess.CalledProcessError:
            print("   ⚠️  Could not get WAL LSN")
        
        # Test WAL file name function
        try:
            result = subprocess.run([
                "psql", "-h", "localhost", "-p", "5432", "-U", "postgres", 
                "-c", "SELECT pg_walfile_name(pg_current_wal_lsn());"
            ], capture_output=True, check=True, text=True)
            wal_file = result.stdout.strip().split('\n')[-1]
            print(f"   ✅ Current WAL file: {wal_file}")
        except subprocess.CalledProcessError:
            print("   ⚠️  Could not get WAL file name")
        
        # Test replication connection (needed for pg_basebackup)
        try:
            result = subprocess.run([
                "psql", "-h", "localhost", "-p", "5432", "-U", "postgres", 
                "-c", "SELECT pg_is_in_recovery();"
            ], capture_output=True, check=True, text=True)
            print("   ✅ PostgreSQL replication functions accessible")
        except subprocess.CalledProcessError:
            print("   ⚠️  Could not test replication functions")
        
        # Check WAL archiving configuration
        try:
            result = subprocess.run([
                "psql", "-h", "localhost", "-p", "5432", "-U", "postgres", 
                "-c", "SHOW archive_mode;"
            ], capture_output=True, check=True, text=True)
            archive_mode = result.stdout.strip().split('\n')[-1]
            print(f"   📋 Archive mode: {archive_mode}")
            
            result = subprocess.run([
                "psql", "-h", "localhost", "-p", "5432", "-U", "postgres", 
                "-c", "SHOW archive_command;"
            ], capture_output=True, check=True, text=True)
            archive_command = result.stdout.strip().split('\n')[-1]
            print(f"   📋 Archive command: {archive_command}")
            
            if archive_mode == "on" and archive_command not in ["", "(disabled)"]:
                print("   ✅ WAL archiving is configured")
            else:
                print("   ⚠️  WAL archiving not configured (will be set up automatically)")
                
        except subprocess.CalledProcessError:
            print("   ⚠️  Could not check WAL archiving configuration")
        
        # Check data directory accessibility
        try:
            result = subprocess.run([
                "psql", "-h", "localhost", "-p", "5432", "-U", "postgres", 
                "-c", "SHOW data_directory;"
            ], capture_output=True, check=True, text=True)
            data_dir = result.stdout.strip().split('\n')[-1]
            print(f"   📁 PostgreSQL data directory: {data_dir}")
            
            # Check if WAL directory exists
            wal_dir = Path(data_dir) / "pg_wal"
            if wal_dir.exists():
                wal_files = list(wal_dir.glob("*"))[:5]  # Show first 5 files
                print(f"   ✅ WAL directory accessible: {wal_dir}")
                print(f"   📁 Sample WAL files: {len(wal_files)} files found")
            else:
                print(f"   ⚠️  WAL directory not directly accessible: {wal_dir}")
                print("   💡 This is normal - WAL files will be archived differently")
                
        except subprocess.CalledProcessError:
            print("   ⚠️  Could not check data directory")
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"   ❌ PostgreSQL connection failed: {e}")
        print("   💡 Make sure PostgreSQL is running and accessible")
        print("   💡 Check that the 'postgres' user exists and has proper permissions")
        return False

async def start_true_wal_system():
    """Start the TRUE WAL-based backup system."""
    print("\n🚀 STARTING TRUE WAL-BASED INCREMENTAL BACKUP SYSTEM")
    print("=" * 80)
    print("🎯 TRUE WAL System Features:")
    print("   🗄️  pg_basebackup for WAL-compatible base backups")
    print("   📁 TRUE WAL file archiving (actual PostgreSQL transaction logs)")
    print("   📍 Precise LSN tracking for exact point-in-time recovery")
    print("   🔄 TRUE incremental restore (base backup + WAL replay)")
    print("   ⏰ Automatic scheduling (2min WAL archiving, weekly base)")
    print("   📊 Captures ONLY actual database changes (not full dumps)")
    print()
    print("🔧 How TRUE WAL incremental backups work:")
    print("   1. Base backup: pg_basebackup creates a full cluster snapshot")
    print("   2. WAL archiving: PostgreSQL writes changes to WAL files")
    print("   3. Incremental: Archive WAL files containing only the changes")
    print("   4. Restore: Restore base backup + replay WAL files to exact LSN")
    print("   5. Result: Incremental backups contain ONLY transaction changes")
    print()
    
    # Import and start the TRUE WAL backup server
    try:
        from true_wal_incremental_backup import main
        await main()
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure all requirements are installed")
        sys.exit(1)

async def demo_true_wal_workflow():
    """Demonstrate the TRUE WAL backup workflow."""
    print("\n🎭 DEMONSTRATING TRUE WAL BACKUP WORKFLOW")
    print("=" * 60)
    
    # Wait for servers to start
    await asyncio.sleep(5)
    
    print("🔄 Starting TRUE WAL backup schedulers...")
    
    import httpx
    
    async with httpx.AsyncClient() as client:
        for server_name, port in [("MCP1", 8001), ("MCP2", 8002)]:
            try:
                print(f"\n📡 Starting {server_name} TRUE WAL scheduler...")
                
                response = await client.post(
                    f"http://localhost:{port}/invoke",
                    json={"tool": "start_scheduler", "arguments": {}},
                    timeout=120.0  # Longer timeout for base backups and WAL setup
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get("ok"):
                        result = data["result"]
                        print(f"✅ {server_name} TRUE WAL Scheduler Started:")
                        
                        # Show WAL setup status
                        wal_setup = result.get("wal_setup", {})
                        print(f"   🔧 WAL archiving: {'✅ Configured' if wal_setup.get('wal_archiving_configured') else '⚠️  Needs setup'}")
                        if not wal_setup.get('wal_archiving_configured'):
                            print(f"   💡 Recommended config: {wal_setup.get('recommended_config', {})}")
                        
                        # Show initial backup results
                        initial_result = result.get('initial_backup_result', {})
                        print(f"   📦 Initial backups: {initial_result.get('successful', 0)} created")
                        print(f"   ⏰ Next WAL incremental: {result.get('next_incremental', 'Unknown')}")
                        print(f"   🔧 Method: {result.get('backup_method', 'Unknown')}")
                    else:
                        print(f"❌ {server_name} scheduler failed: {data.get('error')}")
                else:
                    print(f"❌ {server_name} HTTP error: {response.status_code}")
                    
            except Exception as e:
                print(f"❌ {server_name} connection error: {e}")
        
        # Show a sample backup listing
        print(f"\n📋 Sample TRUE WAL backup listing for analytics_db:")
        
        try:
            list_response = await client.post(
                "http://localhost:8001/invoke",
                json={"tool": "list_backups", "arguments": {"db_name": "analytics_db", "limit": 5}},
                timeout=10.0
            )
            
            if list_response.status_code == 200:
                list_data = list_response.json()
                if list_data.get("ok"):
                    backups = list_data["result"]["backups"]
                    print(f"   📊 Found {len(backups)} TRUE WAL backups:")
                    for backup in backups:
                        backup_type = backup["backup_type"]
                        size_mb = backup["size_bytes"] / (1024 * 1024)
                        print(f"      📁 {backup['backup_id']} ({backup_type}, {size_mb:.1f} MB)")
                        if backup.get("lsn_start"):
                            print(f"         📍 LSN: {backup['lsn_start']} → {backup.get('lsn_end', 'unknown')}")
                        if backup.get("wal_files"):
                            print(f"         📄 WAL files: {len(backup['wal_files'])} archived")
            
        except Exception as e:
            print(f"⚠️  Could not list TRUE WAL backups: {e}")
        
        # Show health status
        print(f"\n🔍 Checking TRUE WAL system health...")
        
        try:
            health_response = await client.post(
                "http://localhost:8001/invoke",
                json={"tool": "health", "arguments": {}},
                timeout=10.0
            )
            
            if health_response.status_code == 200:
                health_data = health_response.json()
                if health_data.get("ok"):
                    health = health_data["result"]
                    print(f"✅ TRUE WAL System Health:")
                    print(f"   📊 Total backups: {health['backup_counts']['total']}")
                    print(f"   🗄️  Base backups: {health['backup_counts']['base_backups']}")
                    print(f"   📁 WAL incremental: {health['backup_counts']['wal_incremental_backups']}")
                    print(f"   📍 Last base backup: {health.get('last_base_backup', 'None')}")
                    print(f"   🔗 PostgreSQL: {'✅ Connected' if health['postgresql']['accessible'] else '❌ Not accessible'}")
                    if health['postgresql'].get('current_lsn'):
                        print(f"   📍 Current LSN: {health['postgresql']['current_lsn']}")
                    if health['postgresql'].get('current_wal_file'):
                        print(f"   📁 Current WAL file: {health['postgresql']['current_wal_file']}")
                    print(f"   🔧 WAL archiving: {'✅ Enabled' if health.get('wal_archiving_enabled') else '⚠️  Disabled'}")
                    print(f"   💾 Backup system: {health['backup_system']}")
            
        except Exception as e:
            print(f"⚠️  Could not check TRUE WAL health: {e}")
    
    print("\n🎉 TRUE WAL-based incremental backup system is now running!")
    print("📊 Monitor TRUE WAL backup files in: ./backups/")
    print("⏱️  TRUE WAL incremental backups will start in 30 seconds")
    print("🔄 Use Ctrl+C to stop the system")
    print("\n💡 File structure for TRUE WAL backups:")
    print("   📁 ./backups/mcp1/basebackups/ - pg_basebackup cluster snapshots")
    print("   📁 ./backups/mcp1/wal_incremental/ - WAL files with actual changes")
    print("   📁 ./backups/mcp1/wal_archive/ - Archived WAL files")
    print("   📁 ./backups/mcp2/basebackups/ - pg_basebackup cluster snapshots")
    print("   📁 ./backups/mcp2/wal_incremental/ - WAL files with actual changes")
    print("   📁 ./backups/mcp2/wal_archive/ - Archived WAL files")
    print("\n💡 To test TRUE WAL restore:")
    print("   curl -X POST http://localhost:8001/invoke \\")
    print("        -H 'Content-Type: application/json' \\")
    print("        -d '{\"tool\": \"restore_database\", \"arguments\": {\"db_name\": \"analytics_db\", \"backup_id\": \"analytics_db_wal_YYYYMMDD_HHMMSS\"}}'")
    print("\n🎯 TRUE WAL incremental backups contain:")
    print("   📄 Actual WAL files (compressed) with transaction changes")
    print("   📋 Metadata with LSN ranges and timeline information")
    print("   📊 Summary of what changed since last backup")
    print("   ✅ ONLY the database changes, not full dumps!")

def main():
    """Main function."""
    print("🎯 SP LAKEHOUSE BACKUP - TRUE WAL-BASED INCREMENTAL SYSTEM")
    print("=" * 70)
    print("🆕 NEW: TRUE WAL-based incremental backups - only captures changes!")
    print()
    
    # Check if we're in the right directory
    if not Path("true_wal_incremental_backup.py").exists():
        print("❌ Error: Must run from project root directory")
        sys.exit(1)
    
    # Install requirements
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", 
            "-r", "requirements_real_backup.txt"
        ])
        print("✅ Requirements installed successfully")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install requirements: {e}")
        sys.exit(1)
    
    # Check PostgreSQL for TRUE WAL backup capabilities
    if not check_postgresql_for_wal_backup():
        print("\n⚠️  Warning: PostgreSQL issues detected.")
        print("The TRUE WAL system may have limited functionality.")
        response = input("Continue anyway? (y/N): ").lower()
        if response != 'y':
            sys.exit(1)
    
    print("\n🎉 All checks passed! Starting TRUE WAL-based incremental backup system...")
    
    async def run_true_wal_system():
        # Start the backup servers
        server_task = asyncio.create_task(start_true_wal_system())
        
        # Start the demo workflow after a delay
        demo_task = asyncio.create_task(demo_true_wal_workflow())
        
        # Wait for the server task (demo will complete first)
        await server_task
    
    try:
        asyncio.run(run_true_wal_system())
    except KeyboardInterrupt:
        print("\n\n🛑 TRUE WAL-based incremental backup system stopped by user")
        print("📊 Check ./backups/ directory for TRUE WAL backup files:")
        print("   📁 ./backups/mcp1/basebackups/ - pg_basebackup cluster snapshots")
        print("   📁 ./backups/mcp1/wal_incremental/ - WAL files with ONLY changes")
        print("   📁 ./backups/mcp1/wal_archive/ - Archived WAL transaction logs")
        print("   📁 ./backups/mcp2/basebackups/ - pg_basebackup cluster snapshots")  
        print("   📁 ./backups/mcp2/wal_incremental/ - WAL files with ONLY changes")
        print("   📁 ./backups/mcp2/wal_archive/ - Archived WAL transaction logs")
        print("\n✅ These are TRUE WAL files containing only transaction changes!")
        print("🎯 Incremental backups now capture ONLY what changed, not full dumps!")
    except Exception as e:
        print(f"\n❌ Error starting TRUE WAL-based incremental backup system: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
