#!/usr/bin/env python3
"""
PostgreSQL Backup MCP Server

A Model Context Protocol server that provides PostgreSQL backup and restore tools.
Implements the MCP protocol using JSON-RPC over stdio transport.
"""

import asyncio
import json
import sys
import logging
import argparse
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from pathlib import Path
import subprocess
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger(__name__)


# ============================================================================
# PostgreSQL Connection Configuration
# ============================================================================

class PostgresConfig:
    """PostgreSQL connection configuration."""
    
    def __init__(self, server_name: str):
        self.server_name = server_name
        
        # Load from environment with server-specific prefix
        prefix = server_name.upper()
        self.host = os.getenv(f"{prefix}_HOST", "localhost")
        self.port = int(os.getenv(f"{prefix}_PORT", "5432"))
        self.database = os.getenv(f"{prefix}_DATABASE", "postgres")
        self.user = os.getenv(f"{prefix}_USER", "postgres")
        self.password = os.getenv(f"{prefix}_PASSWORD", "")
        
        # Backup storage
        self.backup_dir = Path(os.getenv(f"{prefix}_BACKUP_DIR", f"./backups/{server_name}"))
        self.backup_dir.mkdir(parents=True, exist_ok=True)
    
    def get_connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def get_pg_dump_env(self) -> Dict[str, str]:
        """Get environment variables for pg_dump."""
        env = os.environ.copy()
        if self.password:
            env['PGPASSWORD'] = self.password
        # Also set these to avoid password prompts
        env['PGHOST'] = self.host
        env['PGPORT'] = str(self.port)
        env['PGUSER'] = self.user
        env['PGDATABASE'] = self.database
        return env


# ============================================================================
# Backup Manager
# ============================================================================

class BackupManager:
    """Manages PostgreSQL backups and restores."""
    
    def __init__(self, config: PostgresConfig):
        self.config = config
        self.metadata_file = self.config.backup_dir / "metadata.json"
        self.backups = self._load_metadata()
    
    def _load_metadata(self) -> Dict[str, Any]:
        """Load backup metadata."""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load metadata: {e}")
        return {"backups": []}
    
    def _save_metadata(self):
        """Save backup metadata."""
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(self.backups, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Failed to save metadata: {e}")
    
    def _generate_backup_id(self, backup_type: str) -> str:
        """Generate unique backup ID."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{backup_type}_{timestamp}"
    
    def list_backups(self, db_name: str, limit: int = 50) -> List[Dict[str, Any]]:
        """List backups for a database."""
        db_backups = [
            b for b in self.backups.get("backups", [])
            if b.get("db_name") == db_name
        ]
        
        # Sort by timestamp (newest first)
        db_backups.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        
        return db_backups[:limit]
    
    def trigger_full_backup(self, db_name: str) -> Dict[str, Any]:
        """Trigger a full backup."""
        try:
            backup_id = self._generate_backup_id("full")
            backup_file = self.config.backup_dir / f"{backup_id}.sql"
            
            logger.info(f"Starting full backup for {db_name}...")
            
            # Run pg_dump
            cmd = [
                "pg_dump",
                "-h", self.config.host,
                "-p", str(self.config.port),
                "-U", self.config.user,
                "-d", db_name,
                "-F", "p",  # Plain text format
                "-f", str(backup_file)
            ]
            
            result = subprocess.run(
                cmd,
                env=self.config.get_pg_dump_env(),
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                raise Exception(f"pg_dump failed: {result.stderr}")
            
            # Get file size
            file_size = backup_file.stat().st_size
            
            # Create metadata
            backup_metadata = {
                "id": backup_id,
                "db_name": db_name,
                "type": "full",
                "timestamp": datetime.now().isoformat(),
                "file": str(backup_file),
                "size_bytes": file_size,
                "status": "completed"
            }
            
            # Save metadata
            self.backups.setdefault("backups", []).append(backup_metadata)
            self._save_metadata()
            
            logger.info(f"Full backup completed: {backup_id}")
            
            return {
                "success": True,
                "backup_id": backup_id,
                "size_bytes": file_size,
                "timestamp": backup_metadata["timestamp"]
            }
            
        except Exception as e:
            logger.error(f"Full backup failed: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def trigger_incremental_backup(self, db_name: str) -> Dict[str, Any]:
        """Trigger an incremental backup (simplified version)."""
        try:
            # For simplicity, we'll do a full backup labeled as incremental
            # In production, you'd use WAL archiving or pg_basebackup
            backup_id = self._generate_backup_id("incremental")
            backup_file = self.config.backup_dir / f"{backup_id}.sql"
            
            logger.info(f"Starting incremental backup for {db_name}...")
            
            # Get last backup timestamp for reference
            last_backups = self.list_backups(db_name, limit=1)
            last_backup_time = None
            if last_backups:
                last_backup_time = last_backups[0].get("timestamp")
            
            # Run pg_dump (in production, use WAL archiving instead)
            cmd = [
                "pg_dump",
                "-h", self.config.host,
                "-p", str(self.config.port),
                "-U", self.config.user,
                "-d", db_name,
                "-F", "p",
                "-f", str(backup_file)
            ]
            
            result = subprocess.run(
                cmd,
                env=self.config.get_pg_dump_env(),
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                raise Exception(f"pg_dump failed: {result.stderr}")
            
            file_size = backup_file.stat().st_size
            
            backup_metadata = {
                "id": backup_id,
                "db_name": db_name,
                "type": "incremental",
                "timestamp": datetime.now().isoformat(),
                "file": str(backup_file),
                "size_bytes": file_size,
                "base_backup": last_backup_time,
                "status": "completed"
            }
            
            self.backups.setdefault("backups", []).append(backup_metadata)
            self._save_metadata()
            
            logger.info(f"Incremental backup completed: {backup_id}")
            
            return {
                "success": True,
                "backup_id": backup_id,
                "size_bytes": file_size,
                "timestamp": backup_metadata["timestamp"],
                "base_backup": last_backup_time
            }
            
        except Exception as e:
            logger.error(f"Incremental backup failed: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def restore_database(
        self,
        db_name: str,
        backup_id: Optional[str] = None,
        target_timestamp: Optional[str] = None
    ) -> Dict[str, Any]:
        """Restore database from backup."""
        try:
            # Find the backup to restore
            backup = None
            
            if backup_id:
                # Find by backup ID
                for b in self.backups.get("backups", []):
                    if b.get("id") == backup_id and b.get("db_name") == db_name:
                        backup = b
                        break
            elif target_timestamp:
                # Find closest backup before target timestamp
                target_dt = datetime.fromisoformat(target_timestamp.replace('Z', '+00:00'))
                candidates = [
                    b for b in self.backups.get("backups", [])
                    if b.get("db_name") == db_name
                ]
                
                # Sort by how close they are to target
                candidates.sort(
                    key=lambda x: abs(
                        (datetime.fromisoformat(x.get("timestamp", "")) - target_dt).total_seconds()
                    )
                )
                
                if candidates:
                    backup = candidates[0]
            else:
                # Use most recent backup
                backups = self.list_backups(db_name, limit=1)
                if backups:
                    backup = backups[0]
            
            if not backup:
                raise Exception("No suitable backup found")
            
            backup_file = Path(backup["file"])
            if not backup_file.exists():
                raise Exception(f"Backup file not found: {backup_file}")
            
            logger.info(f"Restoring {db_name} from backup {backup['id']}...")
            
            # Drop and recreate database (WARNING: destructive!)
            # In production, you'd want more safeguards
            drop_cmd = [
                "psql",
                "-h", self.config.host,
                "-p", str(self.config.port),
                "-U", self.config.user,
                "-d", "postgres",
                "-c", f"DROP DATABASE IF EXISTS {db_name};"
            ]
            
            subprocess.run(
                drop_cmd,
                env=self.config.get_pg_dump_env(),
                capture_output=True
            )
            
            create_cmd = [
                "psql",
                "-h", self.config.host,
                "-p", str(self.config.port),
                "-U", self.config.user,
                "-d", "postgres",
                "-c", f"CREATE DATABASE {db_name};"
            ]
            
            result = subprocess.run(
                create_cmd,
                env=self.config.get_pg_dump_env(),
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                raise Exception(f"Failed to create database: {result.stderr}")
            
            # Restore from backup
            restore_cmd = [
                "psql",
                "-h", self.config.host,
                "-p", str(self.config.port),
                "-U", self.config.user,
                "-d", db_name,
                "-f", str(backup_file)
            ]
            
            result = subprocess.run(
                restore_cmd,
                env=self.config.get_pg_dump_env(),
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                logger.warning(f"Restore had warnings: {result.stderr}")
            
            logger.info(f"Restore completed from backup {backup['id']}")
            
            return {
                "success": True,
                "backup_id": backup["id"],
                "backup_timestamp": backup["timestamp"],
                "restored_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def enable_schedules(
        self,
        incremental_every: str = "PT2M",
        full_cron: str = "0 3 * * 0"
    ) -> Dict[str, Any]:
        """Enable backup schedules (mock implementation)."""
        # In production, this would set up cron jobs or systemd timers
        logger.info(f"Schedules would be configured:")
        logger.info(f"  Incremental: every {incremental_every}")
        logger.info(f"  Full: {full_cron}")
        
        return {
            "success": True,
            "incremental_schedule": incremental_every,
            "full_schedule": full_cron,
            "note": "Schedule configuration not implemented in this demo"
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check."""
        try:
            # Test PostgreSQL connection
            cmd = [
                "psql",
                "-h", self.config.host,
                "-p", str(self.config.port),
                "-U", self.config.user,
                "-d", self.config.database,
                "-c", "SELECT 1;"
            ]
            
            result = subprocess.run(
                cmd,
                env=self.config.get_pg_dump_env(),
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                return {
                    "status": "healthy",
                    "server": self.config.server_name,
                    "postgres_version": "connected",
                    "backup_dir": str(self.config.backup_dir),
                    "total_backups": len(self.backups.get("backups", []))
                }
            else:
                return {
                    "status": "unhealthy",
                    "error": result.stderr
                }
                
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e)
            }


# ============================================================================
# MCP Server Implementation
# ============================================================================

class MCPServer:
    """MCP Server for PostgreSQL backups."""
    
    def __init__(self, server_name: str):
        self.server_name = server_name
        self.config = PostgresConfig(server_name)
        self.backup_manager = BackupManager(self.config)
        
        # Tool definitions
        self.tools = [
            {
                "name": "list_backups",
                "description": "List available backups for a database",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "db_name": {
                            "type": "string",
                            "description": "Database name"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of backups to return",
                            "default": 50
                        }
                    },
                    "required": ["db_name"]
                }
            },
            {
                "name": "trigger_full_backup",
                "description": "Trigger a full backup for a database",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "db_name": {
                            "type": "string",
                            "description": "Database name to backup"
                        }
                    },
                    "required": ["db_name"]
                }
            },
            {
                "name": "trigger_incremental_backup",
                "description": "Trigger an incremental backup for a database",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "db_name": {
                            "type": "string",
                            "description": "Database name to backup"
                        }
                    },
                    "required": ["db_name"]
                }
            },
            {
                "name": "restore_database",
                "description": "Restore a database from backup",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "db_name": {
                            "type": "string",
                            "description": "Database name to restore"
                        },
                        "backup_id": {
                            "type": "string",
                            "description": "Specific backup ID to restore (optional)"
                        },
                        "target_timestamp": {
                            "type": "string",
                            "description": "Target timestamp for restore (ISO8601, optional)"
                        }
                    },
                    "required": ["db_name"]
                }
            },
            {
                "name": "enable_schedules",
                "description": "Enable automatic backup schedules",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "incremental_every": {
                            "type": "string",
                            "description": "Incremental backup interval (ISO8601 duration)",
                            "default": "PT2M"
                        },
                        "full_cron": {
                            "type": "string",
                            "description": "Full backup cron expression",
                            "default": "0 3 * * 0"
                        }
                    }
                }
            },
            {
                "name": "health",
                "description": "Perform health check on the backup system",
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            }
        ]
    
    async def handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle JSON-RPC request."""
        method = request.get("method")
        params = request.get("params", {})
        request_id = request.get("id")
        
        try:
            if method == "initialize":
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "serverInfo": {
                            "name": f"postgres-backup-{self.server_name}",
                            "version": "1.0.0"
                        },
                        "capabilities": {
                            "tools": {}
                        }
                    }
                }
            
            elif method == "tools/list":
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "tools": self.tools
                    }
                }
            
            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                
                result = await self.call_tool(tool_name, arguments)
                
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(result, default=str)
                            }
                        ]
                    }
                }
            
            else:
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32601,
                        "message": f"Method not found: {method}"
                    }
                }
                
        except Exception as e:
            logger.error(f"Error handling request: {e}")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32603,
                    "message": str(e)
                }
            }
    
    async def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a tool."""
        if tool_name == "list_backups":
            db_name = arguments["db_name"]
            limit = arguments.get("limit", 50)
            return self.backup_manager.list_backups(db_name, limit)
        
        elif tool_name == "trigger_full_backup":
            db_name = arguments["db_name"]
            return self.backup_manager.trigger_full_backup(db_name)
        
        elif tool_name == "trigger_incremental_backup":
            db_name = arguments["db_name"]
            return self.backup_manager.trigger_incremental_backup(db_name)
        
        elif tool_name == "restore_database":
            db_name = arguments["db_name"]
            backup_id = arguments.get("backup_id")
            target_timestamp = arguments.get("target_timestamp")
            return self.backup_manager.restore_database(db_name, backup_id, target_timestamp)
        
        elif tool_name == "enable_schedules":
            incremental = arguments.get("incremental_every", "PT2M")
            full_cron = arguments.get("full_cron", "0 3 * * 0")
            return self.backup_manager.enable_schedules(incremental, full_cron)
        
        elif tool_name == "health":
            return self.backup_manager.health_check()
        
        else:
            raise Exception(f"Unknown tool: {tool_name}")
    
    async def run(self):
        """Run the MCP server (stdio mode)."""
        logger.info(f"Starting MCP server: {self.server_name}")
        
        # Read from stdin, write to stdout
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await asyncio.get_event_loop().connect_read_pipe(lambda: protocol, sys.stdin)
        
        while True:
            try:
                # Read JSON-RPC request
                line = await reader.readline()
                if not line:
                    break
                
                request = json.loads(line.decode())
                logger.debug(f"Received request: {request.get('method')}")
                
                # Handle request
                response = await self.handle_request(request)
                
                # Write response
                sys.stdout.write(json.dumps(response) + "\n")
                sys.stdout.flush()
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                break
        
        logger.info(f"MCP server stopped: {self.server_name}")


# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="PostgreSQL Backup MCP Server")
    parser.add_argument(
        "--server-name",
        required=True,
        help="Server name (e.g., PG1, PG2)"
    )
    
    args = parser.parse_args()
    
    # Create and run server
    server = MCPServer(args.server_name)
    
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        logger.info("Server interrupted")
    except Exception as e:
        logger.error(f"Server error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()