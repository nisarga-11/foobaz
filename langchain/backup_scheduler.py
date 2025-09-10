#!/usr/bin/env python3
"""
Backup Scheduler for Postgres Backup and Restore System with pgBackRest
Runs incremental backups every 2 minutes and full backups weekly
"""

import time
import threading
import signal
import sys
from datetime import datetime, timedelta
from typing import Dict, Any
import requests
import json

# Initialize logging configuration
from config.logging_config import (
    get_incremental_backup_logger,
    get_backup_logger,
    get_error_logger,
    log_incremental_backup_start,
    log_incremental_backup_success,
    log_incremental_backup_error
)
from config.settings import settings

class BackupScheduler:
    """Scheduler for automated backup operations."""
    
    def __init__(self):
        self.running = False
        self.threads = []
        self.mcp_server_url = settings.MCP_SERVER_URL
        
        # Initialize loggers
        self.incremental_logger = get_incremental_backup_logger()
        self.backup_logger = get_backup_logger()
        self.error_logger = get_error_logger()
        
        # Backup configuration
        self.incremental_interval = settings.INCREMENTAL_BACKUP_INTERVAL_MINUTES * 60  # Convert to seconds
        self.databases = ["customer_db", "employee_db"]
        
        # Track last full backup
        self.last_full_backup = None
        self.full_backup_interval = 7 * 24 * 60 * 60  # 7 days in seconds
        
        print(f"🔄 Backup Scheduler initialized")
        print(f"   - Incremental backup interval: {settings.INCREMENTAL_BACKUP_INTERVAL_MINUTES} minutes")
        print(f"   - Full backup interval: Weekly")
        print(f"   - MCP Server URL: {self.mcp_server_url}")
    
    def start(self):
        """Start the backup scheduler."""
        if self.running:
            print("⚠️  Backup scheduler is already running")
            return
        
        self.running = True
        print("🚀 Starting backup scheduler...")
        
        # Start incremental backup thread
        incremental_thread = threading.Thread(target=self._incremental_backup_loop, daemon=True)
        incremental_thread.start()
        self.threads.append(incremental_thread)
        
        # Start full backup thread
        full_backup_thread = threading.Thread(target=self._full_backup_loop, daemon=True)
        full_backup_thread.start()
        self.threads.append(full_backup_thread)
        
        print("✅ Backup scheduler started successfully")
        self.backup_logger.info("Backup scheduler started")
    
    def stop(self):
        """Stop the backup scheduler."""
        if not self.running:
            print("⚠️  Backup scheduler is not running")
            return
        
        print("⏹️  Stopping backup scheduler...")
        self.running = False
        
        # Wait for threads to finish
        for thread in self.threads:
            thread.join(timeout=5)
        
        print("✅ Backup scheduler stopped")
        self.backup_logger.info("Backup scheduler stopped")
    
    def _incremental_backup_loop(self):
        """Main loop for incremental backups."""
        print(f"🔄 Starting incremental backup loop (every {settings.INCREMENTAL_BACKUP_INTERVAL_MINUTES} minutes)")
        
        while self.running:
            try:
                # Run incremental backups for all databases
                for database in self.databases:
                    if not self.running:
                        break
                    
                    self._run_incremental_backup(database)
                
                # Wait for next interval
                if self.running:
                    time.sleep(self.incremental_interval)
            
            except Exception as e:
                error_msg = f"Error in incremental backup loop: {e}"
                print(f"❌ {error_msg}")
                self.error_logger.error(error_msg)
                time.sleep(60)  # Wait 1 minute before retrying
    
    def _full_backup_loop(self):
        """Main loop for full backups."""
        print("🔄 Starting full backup loop (weekly)")
        
        while self.running:
            try:
                # Check if it's time for a full backup
                now = datetime.now()
                if (self.last_full_backup is None or 
                    (now - self.last_full_backup).total_seconds() >= self.full_backup_interval):
                    
                    self._run_full_backup()
                    self.last_full_backup = now
                
                # Check every hour
                time.sleep(3600)
            
            except Exception as e:
                error_msg = f"Error in full backup loop: {e}"
                print(f"❌ {error_msg}")
                self.error_logger.error(error_msg)
                time.sleep(3600)  # Wait 1 hour before retrying
    
    def _run_incremental_backup(self, database: str):
        """Run incremental backup for a specific database."""
        try:
            print(f"🔄 Starting incremental backup for {database}")
            log_incremental_backup_start(database, "incremental")
            
            # Get server-specific configuration
            if database == "customer_db":
                server_name = "customerServer"
                server_config = settings.get_pgbackrest_config("customerServer")
            elif database == "employee_db":
                server_name = "employeeServer"
                server_config = settings.get_pgbackrest_config("employeeServer")
            else:
                # Fallback to customer server
                server_name = "customerServer"
                server_config = settings.get_pgbackrest_config("customerServer")
            
            # Call MCP server for incremental backup with server-specific config
            result = self._call_mcp_tool("pgbackrest_incremental_backup", {
                "stanza": server_config["stanza"],
                "backup_path": server_config["backup_path"],
                "server_name": server_name
            })
            
            if result and result.get("status") == "success":
                backup_info = result.get("backup_info", {})
                backup_id = backup_info.get("backup_id", "Unknown")
                size = backup_info.get("size", "Unknown")
                
                success_msg = f"Successfully completed incremental backup for {database} - ID: {backup_id}, Size: {size}"
                print(f"✅ {success_msg}")
                log_incremental_backup_success(database, backup_id, size)
                
            else:
                error_msg = result.get("error", "Unknown error") if result else "No response from MCP server"
                error_msg = f"Incremental backup failed for {database}: {error_msg}"
                print(f"❌ {error_msg}")
                log_incremental_backup_error(database, error_msg)
        
        except Exception as e:
            error_msg = f"Incremental backup failed for {database}: {e}"
            print(f"❌ {error_msg}")
            log_incremental_backup_error(database, error_msg)
    
    def _run_full_backup(self):
        """Run full backup for all databases."""
        try:
            print("🔄 Starting weekly full backup for all databases")
            self.backup_logger.info("Starting weekly full backup for all databases")
            
            # Run full backup for each server
            for database in self.databases:
                if database == "customer_db":
                    server_name = "customerServer"
                    server_config = settings.get_pgbackrest_config("customerServer")
                elif database == "employee_db":
                    server_name = "employeeServer"
                    server_config = settings.get_pgbackrest_config("employeeServer")
                else:
                    continue
                
                print(f"🔄 Running full backup for {database} on {server_name}")
                
                # Call MCP server for full backup with server-specific config
                result = self._call_mcp_tool("pgbackrest_full_backup", {
                    "stanza": server_config["stanza"],
                    "backup_path": server_config["backup_path"],
                    "server_name": server_name
                })
            
                if result and result.get("status") == "success":
                    backup_info = result.get("backup_info", {})
                    backup_id = backup_info.get("backup_id", "Unknown")
                    size = backup_info.get("size", "Unknown")
                    
                    success_msg = f"Successfully completed weekly full backup for {database} on {server_name} - ID: {backup_id}, Size: {size}"
                    print(f"✅ {success_msg}")
                    self.backup_logger.info(success_msg)
                    
                else:
                    error_msg = result.get("error", "Unknown error") if result else "No response from MCP server"
                    error_msg = f"Weekly full backup failed for {database} on {server_name}: {error_msg}"
                    print(f"❌ {error_msg}")
                    self.error_logger.error(error_msg)
        
        except Exception as e:
            error_msg = f"Weekly full backup failed: {e}"
            print(f"❌ {error_msg}")
            self.error_logger.error(error_msg)
    
    def _call_mcp_tool(self, tool_name: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Call an MCP tool via HTTP."""
        try:
            url = f"{self.mcp_server_url}/mcp"
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": tool_name,
                    "arguments": parameters
                }
            }
            
            response = requests.post(url, json=payload, timeout=300)  # 5 minutes timeout for pgBackRest operations
            response.raise_for_status()
            
            result = response.json()
            if "result" in result:
                return result["result"]
            elif "error" in result:
                return {"status": "error", "error": result["error"]}
            else:
                return {"status": "error", "error": "Unexpected response format"}
        
        except requests.exceptions.RequestException as e:
            return {"status": "error", "error": f"HTTP request failed: {e}"}
        except Exception as e:
            return {"status": "error", "error": f"Error calling MCP tool: {e}"}
    
    def get_status(self) -> Dict[str, Any]:
        """Get current scheduler status."""
        return {
            "running": self.running,
            "incremental_interval_minutes": settings.INCREMENTAL_BACKUP_INTERVAL_MINUTES,
            "last_full_backup": self.last_full_backup.isoformat() if self.last_full_backup else None,
            "databases": self.databases,
            "mcp_server_url": self.mcp_server_url
        }

def signal_handler(signum, frame):
    """Handle shutdown signals."""
    print("\n🛑 Received shutdown signal...")
    if 'scheduler' in globals():
        scheduler.stop()
    sys.exit(0)

def main():
    """Main function to run the backup scheduler."""
    print("🔄 Postgres Backup & Restore System - Backup Scheduler")
    print("=" * 60)
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and start scheduler
    global scheduler
    scheduler = BackupScheduler()
    
    try:
        scheduler.start()
        
        # Keep the main thread alive
        while scheduler.running:
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("\n🛑 Keyboard interrupt received...")
    except Exception as e:
        print(f"❌ Error in main: {e}")
    finally:
        scheduler.stop()

if __name__ == "__main__":
    main()
