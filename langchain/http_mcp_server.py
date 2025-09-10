#!/usr/bin/env python3
"""
HTTP-based MCP server implementation with actual PostgreSQL operations.
"""

import json
import logging
import os
import subprocess
import glob
import psycopg2
from typing import Dict, Any, List
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn
from pydantic import BaseModel
from config.settings import settings
from tools.db_tools import DatabaseTools
from mcp_server.tools.pgbackrest_tools import PGBackRestServerTools
from config.logging_config import get_backup_logger, get_pgbackrest_logger, get_error_logger

# Initialize logging configuration
from config.logging_config import logging_config

# Get specific loggers
logger = logging.getLogger(__name__)
backup_logger = get_backup_logger()
pgbackrest_logger = get_pgbackrest_logger()
error_logger = get_error_logger()

# Pydantic models for request/response
class InitializeRequest(BaseModel):
    protocolVersion: str
    capabilities: Dict[str, Any]
    clientInfo: Dict[str, Any]

class ToolCallRequest(BaseModel):
    name: str
    arguments: Dict[str, Any]

class MCPRequest(BaseModel):
    jsonrpc: str = "2.0"
    id: int
    method: str
    params: Dict[str, Any]

class HTTPMCPServer:
    """HTTP-based MCP server with actual PostgreSQL operations."""
    
    def __init__(self, host: str = "localhost", port: int = 8082):
        self.host = host
        self.port = port
        self.app = FastAPI(title="HTTP MCP Server", version="1.0.0")
        self.tools = {
            "postgres_backup": {
                "name": "postgres_backup",
                "description": "Create a backup of a PostgreSQL database using pg_dump",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "database_name": {
                            "type": "string",
                            "description": "Name of the database to backup (customer_db or employee_db)"
                        }
                    },
                    "required": ["database_name"]
                }
            },
            "postgres_restore": {
                "name": "postgres_restore",
                "description": "Restore a PostgreSQL database from a backup using pg_restore",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "backup_file": {
                            "type": "string",
                            "description": "Path to the backup file to restore from"
                        },
                        "database_name": {
                            "type": "string",
                            "description": "Name of the database to restore (customer_db or employee_db)"
                        }
                    },
                    "required": ["backup_file", "database_name"]
                }
            },
            "list_backups": {
                "name": "list_backups",
                "description": "List available backup files for a database",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "database_name": {
                            "type": "string",
                            "description": "Name of the database to list backups for"
                        }
                    },
                    "required": ["database_name"]
                }
            },
            "get_database_info": {
                "name": "get_database_info",
                "description": "Get database information including integrity status",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "database_name": {
                            "type": "string",
                            "description": "Name of the database to get info for"
                        }
                    },
                    "required": ["database_name"]
                }
            },
            "postgres_start": {
                "name": "postgres_start",
                "description": "Start PostgreSQL service",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "database_name": {
                            "type": "string",
                            "description": "Name of the database to start PostgreSQL for"
                        }
                    },
                    "required": ["database_name"]
                }
            },
            "postgres_stop": {
                "name": "postgres_stop",
                "description": "Stop PostgreSQL service",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "database_name": {
                            "type": "string",
                            "description": "Name of the database to stop PostgreSQL for"
                        }
                    },
                    "required": ["database_name"]
                }
            },
            "pgbackrest_full_backup": {
                "name": "pgbackrest_full_backup",
                "description": "Create a full backup using pgBackRest",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "stanza": {
                            "type": "string",
                            "description": "pgBackRest stanza name",
                            "default": "demo"
                        },
                        "backup_path": {
                            "type": "string",
                            "description": "Backup repository path",
                            "default": "/var/lib/pgbackrest"
                        }
                    }
                }
            },
            "pgbackrest_incremental_backup": {
                "name": "pgbackrest_incremental_backup",
                "description": "Create an incremental backup using pgBackRest",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "stanza": {
                            "type": "string",
                            "description": "pgBackRest stanza name",
                            "default": "demo"
                        },
                        "backup_path": {
                            "type": "string",
                            "description": "Backup repository path",
                            "default": "/var/lib/pgbackrest"
                        }
                    }
                }
            },
            "pgbackrest_restore": {
                "name": "pgbackrest_restore",
                "description": "Restore from a pgBackRest backup",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "stanza": {
                            "type": "string",
                            "description": "pgBackRest stanza name",
                            "default": "demo"
                        },
                        "pgdata": {
                            "type": "string",
                            "description": "PostgreSQL data directory",
                            "default": "/var/lib/postgresql/15/main"
                        },
                        "backup_path": {
                            "type": "string",
                            "description": "Backup repository path",
                            "default": "/var/lib/pgbackrest"
                        },
                        "backup_type": {
                            "type": "string",
                            "description": "Backup type or ID to restore",
                            "default": "latest"
                        }
                    }
                }
            },
            "pgbackrest_list_backups": {
                "name": "pgbackrest_list_backups",
                "description": "List available pgBackRest backups with timestamps",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "stanza": {
                            "type": "string",
                            "description": "pgBackRest stanza name",
                            "default": "demo"
                        },
                        "backup_path": {
                            "type": "string",
                            "description": "Backup repository path",
                            "default": "/var/lib/pgbackrest"
                        }
                    }
                }
            },
            "pgbackrest_recommended_backups": {
                "name": "pgbackrest_recommended_backups",
                "description": "Get the 3 most recent incremental backups with consistent timestamps and provide recommendation",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "stanza": {
                            "type": "string",
                            "description": "pgBackRest stanza name",
                            "default": "demo"
                        },
                        "backup_path": {
                            "type": "string",
                            "description": "Backup repository path",
                            "default": "/var/lib/pgbackrest"
                        }
                    }
                }
            },
            "pgbackrest_info": {
                "name": "pgbackrest_info",
                "description": "Get pgBackRest information and backup status",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "stanza": {
                            "type": "string",
                            "description": "pgBackRest stanza name",
                            "default": "demo"
                        },
                        "backup_path": {
                            "type": "string",
                            "description": "Backup repository path",
                            "default": "/var/lib/pgbackrest"
                        }
                    }
                }
            },
            "pgbackrest_pitr_restore": {
                "name": "pgbackrest_pitr_restore",
                "description": "Perform Point-in-Time Recovery (PITR) restore to a specific target time",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "stanza": {
                            "type": "string",
                            "description": "pgBackRest stanza name",
                            "default": "demo"
                        },
                        "pgdata": {
                            "type": "string",
                            "description": "PostgreSQL data directory",
                            "default": "/var/lib/postgresql/15/main"
                        },
                        "backup_path": {
                            "type": "string",
                            "description": "Backup repository path",
                            "default": "/var/lib/pgbackrest"
                        },
                        "target_time": {
                            "type": "string",
                            "description": "Target time for PITR restore (e.g., '2025-09-07 15:00:00+05:30')"
                        }
                    },
                    "required": ["target_time"]
                }
            },
            "pgbackrest_pitr_restore_with_workflow": {
                "name": "pgbackrest_pitr_restore_with_workflow",
                "description": "Perform complete PITR restore workflow with PostgreSQL stop/start and port management",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "stanza": {
                            "type": "string",
                            "description": "pgBackRest stanza name",
                            "default": "customer_demo"
                        },
                        "pgdata": {
                            "type": "string",
                            "description": "PostgreSQL data directory path",
                            "default": "/Users/aarthiprashanth/postgres/pg-customer"
                        },
                        "backup_path": {
                            "type": "string",
                            "description": "Backup repository path",
                            "default": "/Users/aarthiprashanth/pgbackrest-repo"
                        },
                        "backup_id": {
                            "type": "string",
                            "description": "Specific backup ID to restore (e.g., 20250908-125120F_20250908-130131I)"
                        },
                        "target_time": {
                            "type": "string",
                            "description": "Target time for PITR restore (format: YYYY-MM-DD HH:MM:SS+TZ)"
                        },
                        "server_name": {
                            "type": "string",
                            "description": "Server name (customerServer or employeeServer)",
                            "default": "customerServer"
                        }
                    },
                    "required": []
                }
            },
            "pgbackrest_coordinated_recommendations": {
                "name": "pgbackrest_coordinated_recommendations",
                "description": "Find coordinated backup recommendations with matching timestamps between customer and employee servers",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "customer_stanza": {
                            "type": "string",
                            "description": "Customer pgBackRest stanza name",
                            "default": "customer_demo"
                        },
                        "employee_stanza": {
                            "type": "string",
                            "description": "Employee pgBackRest stanza name",
                            "default": "employee_demo"
                        }
                    },
                    "required": []
                }
            }
        }
        self._setup_routes()
    
    def _setup_routes(self):
        """Setup FastAPI routes."""
        
        @self.app.post("/mcp")
        async def handle_mcp_request(request: MCPRequest):
            """Handle MCP requests."""
            logger.info(f"Received MCP request: {request.method}")
            
            try:
                if request.method == "initialize":
                    return {
                        "jsonrpc": "2.0",
                        "id": request.id,
                        "result": {
                            "protocolVersion": "2024-11-05",
                            "capabilities": {
                                "tools": {}
                            },
                            "serverInfo": {
                                "name": "http-mcp-server",
                                "version": "1.0.0"
                            }
                        }
                    }
                elif request.method == "tools/list":
                    return {
                        "jsonrpc": "2.0",
                        "id": request.id,
                        "result": {
                            "tools": list(self.tools.values())
                        }
                    }
                elif request.method == "tools/call":
                    tool_name = request.params.get("name")
                    arguments = request.params.get("arguments", {})
                    
                    if tool_name not in self.tools:
                        return {
                            "jsonrpc": "2.0",
                            "id": request.id,
                            "error": {
                                "code": -32601,
                                "message": f"Tool '{tool_name}' not found"
                            }
                        }
                    
                    result = await self.execute_tool(tool_name, arguments)
                    return {
                        "jsonrpc": "2.0",
                        "id": request.id,
                        "result": result
                    }
                else:
                    return {
                        "jsonrpc": "2.0",
                        "id": request.id,
                        "error": {
                            "code": -32601,
                            "message": f"Method '{request.method}' not found"
                        }
                    }
            except Exception as e:
                logger.error(f"Error processing request: {e}")
                return {
                    "jsonrpc": "2.0",
                    "id": request.id,
                    "error": {
                        "code": -32603,
                        "message": f"Internal error: {str(e)}"
                    }
                }
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint."""
            return {"status": "healthy", "server": "http-mcp-server"}
        
        @self.app.get("/tools")
        async def list_tools():
            """List available tools."""
            return {"tools": list(self.tools.keys())}
    
    async def execute_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a specific tool."""
        logger.info(f"Executing tool: {tool_name} with arguments: {arguments}")
        
        if tool_name == "postgres_backup":
            return await self._backup_database(arguments.get("database_name"))
        elif tool_name == "postgres_restore":
            return await self._restore_database(arguments.get("backup_file"), arguments.get("database_name"))
        elif tool_name == "list_backups":
            return await self._list_backups(arguments.get("database_name"))
        elif tool_name == "get_database_info":
            return await self._get_database_info(arguments.get("database_name"))
        elif tool_name == "postgres_start":
            return await self._start_postgres(arguments.get("database_name"))
        elif tool_name == "postgres_stop":
            return await self._stop_postgres(arguments.get("database_name"))
        elif tool_name == "pgbackrest_full_backup":
            return await self._pgbackrest_full_backup(
                arguments.get("stanza", "demo"), 
                arguments.get("backup_path", "/var/lib/pgbackrest"),
                arguments.get("server_name", "customerServer")
            )
        elif tool_name == "pgbackrest_incremental_backup":
            return await self._pgbackrest_incremental_backup(
                arguments.get("stanza", "demo"), 
                arguments.get("backup_path", "/var/lib/pgbackrest"),
                arguments.get("server_name", "customerServer")
            )
        elif tool_name == "pgbackrest_restore":
            return await self._pgbackrest_restore(
                arguments.get("stanza", "demo"),
                arguments.get("pgdata", "/var/lib/postgresql/15/main"),
                arguments.get("backup_path", "/var/lib/pgbackrest"),
                arguments.get("backup_type", "latest")
            )
        elif tool_name == "pgbackrest_list_backups":
            return await self._pgbackrest_list_backups(arguments.get("stanza", "demo"), arguments.get("backup_path", "/var/lib/pgbackrest"))
        elif tool_name == "pgbackrest_recommended_backups":
            return await self._pgbackrest_recommended_backups(
                arguments.get("stanza", "demo"), 
                arguments.get("backup_path", "/var/lib/pgbackrest"),
                arguments.get("server_name", "customerServer")
            )
        elif tool_name == "pgbackrest_info":
            return await self._pgbackrest_info(arguments.get("stanza", "demo"), arguments.get("backup_path", "/var/lib/pgbackrest"))
        elif tool_name == "pgbackrest_pitr_restore":
            return await self._pgbackrest_pitr_restore(
                arguments.get("stanza", "demo"),
                arguments.get("pgdata", "/var/lib/postgresql/15/main"),
                arguments.get("backup_path", "/var/lib/pgbackrest"),
                arguments.get("target_time")
            )
        elif tool_name == "pgbackrest_pitr_restore_with_workflow":
            return await self._pgbackrest_pitr_restore_with_workflow(
                arguments.get("stanza", "customer_demo"),
                arguments.get("pgdata", "/Users/aarthiprashanth/postgres/pg-customer"),
                arguments.get("backup_path", "/Users/aarthiprashanth/pgbackrest-repo"),
                arguments.get("backup_id"),
                arguments.get("target_time"),
                arguments.get("server_name", "customerServer")
            )
        elif tool_name == "pgbackrest_coordinated_recommendations":
            return await self._pgbackrest_coordinated_recommendations(
                arguments.get("customer_stanza", "customer_demo"),
                arguments.get("employee_stanza", "employee_demo")
            )
        else:
            return {"error": f"Unknown tool: {tool_name}"}
    
    async def _backup_database(self, database_name: str) -> Dict[str, Any]:
        """Create a database backup using pg_dump."""
        try:
            logger.info(f"Starting backup for database: {database_name}")
            
            # Get database configuration
            if database_name == "customer_db":
                db_config = settings.DB1_CONFIG
            elif database_name == "employee_db":
                db_config = settings.DB2_CONFIG
            else:
                return {
                    "status": "error",
                    "error": f"Unknown database: {database_name}"
                }
            
            # Create backup directory if it doesn't exist
            os.makedirs(settings.BACKUP_DIR, exist_ok=True)
            
            # Generate timestamp for backup filename
            timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
            backup_filename = f"{database_name}_{timestamp}.tar"
            backup_path = os.path.join(settings.BACKUP_DIR, backup_filename)
            
            # Build pg_dump command
            cmd = [
                "pg_dump",
                f"--host={db_config['host']}",
                f"--port={db_config['port']}",
                f"--username={db_config['user']}",
                f"--dbname={db_config['database']}",
                "--format=custom",
                "--verbose",
                f"--file={backup_path}"
            ]
            
            # Set password environment variable if provided
            env = os.environ.copy()
            if db_config.get('password'):
                env['PGPASSWORD'] = db_config['password']
            
            logger.info(f"Executing pg_dump command: {' '.join(cmd)}")
            
            # Execute pg_dump
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                # Get file size
                file_size = os.path.getsize(backup_path)
                size_mb = round(file_size / (1024 * 1024), 2)
                
                logger.info(f"Backup completed successfully: {backup_path} ({size_mb} MB)")
                
                return {
                    "status": "success",
                    "backup_file": backup_path,
                    "filename": backup_filename,
                    "size_bytes": file_size,
                    "size_mb": size_mb,
                    "timestamp": timestamp,
                    "database": database_name
                }
            else:
                error_msg = result.stderr if result.stderr else "Unknown error"
                logger.error(f"pg_dump failed: {error_msg}")
                return {
                    "status": "error",
                    "error": f"pg_dump failed: {error_msg}",
                    "stdout": result.stdout,
                    "stderr": error_msg
                }
                
        except subprocess.TimeoutExpired:
            logger.error("Backup operation timed out")
            return {
                "status": "error",
                "error": "Backup operation timed out after 5 minutes"
            }
        except Exception as e:
            logger.error(f"Backup error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def _restore_database(self, backup_file: str, database_name: str) -> Dict[str, Any]:
        """Restore a database from backup using pg_restore."""
        try:
            logger.info(f"Starting restore for database: {database_name} from {backup_file}")
            
            # Get database configuration
            if database_name == "customer_db":
                db_config = settings.DB1_CONFIG
            elif database_name == "employee_db":
                db_config = settings.DB2_CONFIG
            else:
                return {
                    "status": "error",
                    "error": f"Unknown database: {database_name}"
                }
            
            # Verify backup file exists
            if not os.path.exists(backup_file):
                return {
                    "status": "error",
                    "error": f"Backup file not found: {backup_file}"
                }
            
            # Build pg_restore command
            cmd = [
                "pg_restore",
                f"--host={db_config['host']}",
                f"--port={db_config['port']}",
                f"--username={db_config['user']}",
                f"--dbname={db_config['database']}",
                "--clean",
                "--if-exists",
                "--verbose",
                backup_file
            ]
            
            # Set password environment variable if provided
            env = os.environ.copy()
            if db_config.get('password'):
                env['PGPASSWORD'] = db_config['password']
            
            logger.info(f"Executing pg_restore command: {' '.join(cmd)}")
            
            # Execute pg_restore
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=600  # 10 minute timeout
            )
            
            if result.returncode == 0 or "errors ignored on restore" in result.stderr:
                logger.info(f"Restore completed successfully for {database_name}")
                return {
                    "status": "success",
                    "output": result.stdout,
                    "warnings": result.stderr,
                    "database": database_name,
                    "backup_file": backup_file
                }
            else:
                error_msg = result.stderr if result.stderr else "Unknown error"
                logger.error(f"pg_restore failed: {error_msg}")
                return {
                    "status": "error",
                    "error": f"pg_restore failed: {error_msg}",
                    "stdout": result.stdout,
                    "stderr": error_msg
                }
                
        except subprocess.TimeoutExpired:
            logger.error("Restore operation timed out")
            return {
                "status": "error",
                "error": "Restore operation timed out after 10 minutes"
            }
        except Exception as e:
            logger.error(f"Restore error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def _list_backups(self, database_name: str) -> Dict[str, Any]:
        """List backup files for a database."""
        try:
            logger.info(f"Listing backups for database: {database_name}")
            
            # Create backup directory if it doesn't exist
            os.makedirs(settings.BACKUP_DIR, exist_ok=True)
            
            # Find backup files for this database
            pattern = os.path.join(settings.BACKUP_DIR, f"{database_name}_*.tar")
            backup_files = glob.glob(pattern)
            
            # Sort files by modification time (newest first)
            backup_files.sort(key=os.path.getmtime, reverse=True)
            
            # Get file information
            backups = []
            for file_path in backup_files:
                filename = os.path.basename(file_path)
                file_size = os.path.getsize(file_path)
                size_mb = round(file_size / (1024 * 1024), 2)
                mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                
                backups.append({
                    "filename": filename,
                    "file_path": file_path,
                    "size_bytes": file_size,
                    "size_mb": size_mb,
                    "modified": mod_time.isoformat(),
                    "database": database_name
                })
            
            logger.info(f"Found {len(backups)} backup files for {database_name}")
            
            return {
                "status": "success",
                "backups": backups,
                "count": len(backups),
                "database": database_name
            }
            
        except Exception as e:
            logger.error(f"List backups error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def _get_database_info(self, database_name: str) -> Dict[str, Any]:
        """Get database information and integrity status."""
        try:
            logger.info(f"Getting database info for: {database_name}")
            
            # Get database configuration
            if database_name == "customer_db":
                db_config = settings.DB1_CONFIG
            elif database_name == "employee_db":
                db_config = settings.DB2_CONFIG
            else:
                return {
                    "status": "error",
                    "error": f"Unknown database: {database_name}"
                }
            
            # Test connection and get info
            connection_info = DatabaseTools.test_connection(db_config)
            
            if connection_info["status"] == "connected":
                return {
                    "status": "success",
                    "database_name": database_name,
                    "connection": connection_info
                }
            else:
                return {
                    "status": "error",
                    "error": connection_info.get("error", "Connection failed")
                }
                
        except Exception as e:
            logger.error(f"Get database info error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def _start_postgres(self, database_name: str) -> Dict[str, Any]:
        """Start PostgreSQL service with verification."""
        try:
            logger.info(f"Starting PostgreSQL for database: {database_name}")
            
            # Get the appropriate PostgreSQL instance based on database
            if database_name == "customer_db":
                data_dir = "/Users/aarthiprashanth/postgres/pg-customer"
                port = "5433"
            elif database_name == "employee_db":
                data_dir = "/Users/aarthiprashanth/postgres/pg-employee"
                port = "5434"
            else:
                return {
                    "status": "error",
                    "error": f"Unknown database: {database_name}"
                }
            
            # Start PostgreSQL instance
            cmd = ["/Applications/Postgres.app/Contents/Versions/17/bin/pg_ctl", "start", "-D", data_dir, "-o", f"-p {port}"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                # Wait a moment for PostgreSQL to fully start
                import time
                time.sleep(2)
                
                # Quick verification - check if PID file exists (faster than ps aux)
                import os
                pid_file = f"{data_dir}/postmaster.pid"
                if os.path.exists(pid_file):
                    # PID file exists, PostgreSQL likely started successfully
                    return {
                        "status": "success",
                        "message": f"PostgreSQL started successfully for {database_name}",
                        "stdout": result.stdout,
                        "stderr": result.stderr
                    }
                else:
                    # Fallback to process check if no PID file
                    try:
                        check_cmd = ["ps", "aux"]
                        check_result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=5)
                        
                        postgres_running = False
                        if check_result.returncode == 0:
                            for line in check_result.stdout.split('\n'):
                                if f"-D {data_dir}" in line and "postgres" in line:
                                    postgres_running = True
                                    logger.info(f"PostgreSQL process confirmed running: {line.strip()}")
                                    break
                        
                        if postgres_running:
                            return {
                                "status": "success",
                                "message": f"PostgreSQL started successfully for {database_name}",
                                "stdout": result.stdout,
                                "stderr": result.stderr
                            }
                        else:
                            return {
                                "status": "error",
                                "error": f"PostgreSQL start command succeeded but process not found for {database_name}",
                                "stdout": result.stdout,
                                "stderr": result.stderr
                            }
                    except subprocess.TimeoutExpired:
                        # If verification times out, assume success since pg_ctl succeeded
                        return {
                            "status": "success",
                            "message": f"PostgreSQL started successfully for {database_name} (verification timed out but pg_ctl succeeded)",
                            "stdout": result.stdout,
                            "stderr": result.stderr
                        }
            else:
                return {
                    "status": "error",
                    "error": f"Failed to start PostgreSQL: {result.stderr}",
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
                
        except subprocess.TimeoutExpired:
            return {
                "status": "error",
                "error": "PostgreSQL start command timed out"
            }
        except Exception as e:
            logger.error(f"Start PostgreSQL error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def _stop_postgres(self, database_name: str) -> Dict[str, Any]:
        """Stop PostgreSQL service with robust process verification."""
        try:
            logger.info(f"Stopping PostgreSQL for database: {database_name}")
            
            # Get the appropriate PostgreSQL instance based on database
            if database_name == "customer_db":
                data_dir = "/Users/aarthiprashanth/postgres/pg-customer"
                port = "5433"
            elif database_name == "employee_db":
                data_dir = "/Users/aarthiprashanth/postgres/pg-employee"
                port = "5434"
            else:
                return {
                    "status": "error",
                    "error": f"Unknown database: {database_name}"
                }
            
            # Step 1: Try graceful stop with pg_ctl
            logger.info(f"Attempting graceful stop for {database_name}")
            cmd = ["/Applications/Postgres.app/Contents/Versions/17/bin/pg_ctl", "stop", "-D", data_dir, "-m", "fast"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            # Step 2: Wait a moment for the process to stop
            import time
            time.sleep(2)
            
            # Step 3: Check if PostgreSQL process is still running
            check_cmd = ["ps", "aux"]
            check_result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=10)
            
            # Look for PostgreSQL processes for this specific data directory
            postgres_running = False
            if check_result.returncode == 0:
                for line in check_result.stdout.split('\n'):
                    if f"-D {data_dir}" in line and "postgres" in line:
                        postgres_running = True
                        logger.warning(f"PostgreSQL process still running: {line.strip()}")
                        break
            
            # Step 4: If still running, try force kill
            if postgres_running:
                logger.info(f"PostgreSQL still running, attempting force kill for {database_name}")
                kill_cmd = ["pkill", "-f", f"-D {data_dir}"]
                kill_result = subprocess.run(kill_cmd, capture_output=True, text=True, timeout=10)
                time.sleep(2)
                
                # Check again
                check_result2 = subprocess.run(check_cmd, capture_output=True, text=True, timeout=10)
                postgres_running = False
                if check_result2.returncode == 0:
                    for line in check_result2.stdout.split('\n'):
                        if f"-D {data_dir}" in line and "postgres" in line:
                            postgres_running = True
                            break
            
            # Step 5: Remove PID file if PostgreSQL is stopped
            import os
            pid_file = f"{data_dir}/postmaster.pid"
            if not postgres_running and os.path.exists(pid_file):
                try:
                    os.remove(pid_file)
                    logger.info(f"Removed postmaster.pid file: {pid_file}")
                except Exception as e:
                    logger.warning(f"Could not remove postmaster.pid file: {e}")
            
            # Step 6: Final verification
            if postgres_running:
                return {
                    "status": "error",
                    "error": f"PostgreSQL process still running for {database_name} after stop attempts",
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
            else:
                return {
                    "status": "success",
                    "message": f"PostgreSQL stopped successfully for {database_name}",
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
                
        except subprocess.TimeoutExpired:
            return {
                "status": "error",
                "error": "PostgreSQL stop command timed out"
            }
        except Exception as e:
            logger.error(f"Stop PostgreSQL error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def _pgbackrest_full_backup(self, stanza: str, backup_path: str, server_name: str = "customerServer") -> Dict[str, Any]:
        """Create a full backup using pgBackRest."""
        try:
            pgbackrest_logger.info(f"Starting pgBackRest full backup for stanza: {stanza}")
            backup_logger.info(f"Starting pgBackRest full backup for stanza: {stanza}")
            result = PGBackRestServerTools.full_backup(stanza, backup_path, server_name)
            
            if result["status"] == "success":
                pgbackrest_logger.info(f"Successfully completed pgBackRest full backup for stanza: {stanza}")
                backup_logger.info(f"Successfully completed pgBackRest full backup for stanza: {stanza}")
            else:
                pgbackrest_logger.error(f"pgBackRest full backup failed for stanza: {stanza}")
                backup_logger.error(f"pgBackRest full backup failed for stanza: {stanza}")
            
            return result
        except Exception as e:
            error_msg = f"pgBackRest full backup error: {e}"
            logger.error(error_msg)
            pgbackrest_logger.error(error_msg)
            backup_logger.error(error_msg)
            error_logger.error(error_msg)
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def _pgbackrest_incremental_backup(self, stanza: str, backup_path: str, server_name: str = "customerServer") -> Dict[str, Any]:
        """Create an incremental backup using pgBackRest."""
        try:
            from config.logging_config import log_incremental_backup_start, log_incremental_backup_success, log_incremental_backup_error
            
            # Log start of incremental backup
            log_incremental_backup_start("pgbackrest", "incremental")
            pgbackrest_logger.info(f"Starting pgBackRest incremental backup for stanza: {stanza}")
            backup_logger.info(f"Starting pgBackRest incremental backup for stanza: {stanza}")
            
            result = PGBackRestServerTools.incremental_backup(stanza, backup_path, server_name)
            
            if result["status"] == "success":
                backup_info = result.get("backup_info", {})
                backup_id = backup_info.get("backup_id", "Unknown")
                size = backup_info.get("size", "Unknown")
                
                # Log successful incremental backup
                log_incremental_backup_success("pgbackrest", backup_id, size)
                pgbackrest_logger.info(f"Successfully completed pgBackRest incremental backup for stanza: {stanza} - ID: {backup_id}, Size: {size}")
                backup_logger.info(f"Successfully completed pgBackRest incremental backup for stanza: {stanza} - ID: {backup_id}, Size: {size}")
            else:
                error_msg = result.get("error", "Unknown error")
                log_incremental_backup_error("pgbackrest", error_msg)
                pgbackrest_logger.error(f"pgBackRest incremental backup failed for stanza: {stanza} - {error_msg}")
                backup_logger.error(f"pgBackRest incremental backup failed for stanza: {stanza} - {error_msg}")
            
            return result
        except Exception as e:
            error_msg = f"pgBackRest incremental backup error: {e}"
            log_incremental_backup_error("pgbackrest", error_msg)
            logger.error(error_msg)
            pgbackrest_logger.error(error_msg)
            backup_logger.error(error_msg)
            error_logger.error(error_msg)
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def _pgbackrest_restore(self, stanza: str, pgdata: str, backup_path: str, backup_type: str) -> Dict[str, Any]:
        """Restore from a pgBackRest backup."""
        try:
            logger.info(f"Starting pgBackRest restore for stanza: {stanza}, backup: {backup_type}")
            result = PGBackRestServerTools.restore(stanza, pgdata, backup_path, backup_type)
            return result
        except Exception as e:
            logger.error(f"pgBackRest restore error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def _pgbackrest_list_backups(self, stanza: str, backup_path: str) -> Dict[str, Any]:
        """List available pgBackRest backups."""
        try:
            logger.info(f"Listing pgBackRest backups for stanza: {stanza}")
            result = PGBackRestServerTools.list_backups(stanza, backup_path)
            return result
        except Exception as e:
            logger.error(f"pgBackRest list backups error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def _pgbackrest_info(self, stanza: str, backup_path: str) -> Dict[str, Any]:
        """Get pgBackRest information and status."""
        try:
            logger.info(f"Getting pgBackRest info for stanza: {stanza}")
            result = PGBackRestServerTools.get_info(stanza, backup_path)
            return result
        except Exception as e:
            logger.error(f"pgBackRest info error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def _pgbackrest_recommended_backups(self, stanza: str, backup_path: str, server_name: str = "customerServer") -> Dict[str, Any]:
        """Compare timestamps of incremental backups from both customer_demo and employee_demo, and recommend the closest matching timestamps using Ollama."""
        try:
            logger.info(f"Getting coordinated backup recommendations for both stanzas")
            result = PGBackRestServerTools.get_recommended_backups(stanza, backup_path, server_name)
            return result
        except Exception as e:
            logger.error(f"pgBackRest coordinated backup recommendations error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def _pgbackrest_pitr_restore(self, stanza: str, pgdata: str, backup_path: str, target_time: str) -> Dict[str, Any]:
        """Perform Point-in-Time Recovery (PITR) restore to a specific target time."""
        try:
            logger.info(f"Starting pgBackRest PITR restore for stanza: {stanza}, target time: {target_time}")
            result = PGBackRestServerTools.pitr_restore(stanza, pgdata, backup_path, target_time)
            return result
        except Exception as e:
            logger.error(f"pgBackRest PITR restore error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def _pgbackrest_pitr_restore_with_workflow(self, stanza: str, pgdata: str, backup_path: str, backup_id: str = None, target_time: str = None, server_name: str = "customerServer") -> Dict[str, Any]:
        """Perform complete PITR restore workflow with PostgreSQL stop/start and port management."""
        try:
            logger.info(f"Starting PITR restore workflow for {server_name}: stanza={stanza}")
            result = PGBackRestServerTools.pitr_restore_with_workflow(stanza, pgdata, backup_path, backup_id, target_time, server_name)
            
            if result["status"] == "success":
                return {
                    "status": "success",
                    "message": result.get("message", "PITR restore workflow completed successfully"),
                    "workflow_steps": result.get("workflow_steps", []),
                    "server_name": result.get("server_name", server_name),
                    "postgres_port": result.get("postgres_port", "unknown"),
                    "restore_target": result.get("restore_target", backup_id or target_time or "latest"),
                    "stanza": stanza,
                    "pgdata": pgdata
                }
            else:
                return {
                    "status": "error",
                    "error": result.get("error", "Unknown error during PITR restore workflow"),
                    "workflow_steps": result.get("workflow_steps", []),
                    "server_name": server_name,
                    "stanza": stanza
                }
            
        except Exception as e:
            logger.error(f"PITR restore workflow error: {e}")
            return {
                "status": "error",
                "error": str(e),
                "server_name": server_name,
                "stanza": stanza
            }
    
    async def _pgbackrest_coordinated_recommendations(self, customer_stanza: str = "customer_demo", employee_stanza: str = "employee_demo") -> Dict[str, Any]:
        """Find coordinated backup recommendations with matching timestamps between customer and employee servers."""
        try:
            logger.info("Getting coordinated backup recommendations for both servers")
            result = PGBackRestServerTools.find_coordinated_backup_recommendations(customer_stanza, employee_stanza)
            
            if result["status"] == "success":
                return {
                    "status": "success",
                    "coordinated_recommendations": result.get("coordinated_recommendations", []),
                    "customer_backups_count": result.get("customer_backups_count", 0),
                    "employee_backups_count": result.get("employee_backups_count", 0),
                    "total_matches": result.get("total_matches", 0),
                    "customer_stanza": customer_stanza,
                    "employee_stanza": employee_stanza,
                    "message": "Successfully found coordinated backup recommendations"
                }
            else:
                return {
                    "status": "error",
                    "error": result.get("error", "Unknown error finding coordinated recommendations"),
                    "customer_stanza": customer_stanza,
                    "employee_stanza": employee_stanza
                }
            
        except Exception as e:
            logger.error(f"Coordinated recommendations error: {e}")
            return {
                "status": "error",
                "error": str(e),
                "customer_stanza": customer_stanza,
                "employee_stanza": employee_stanza
            }
    
    def start(self):
        """Start the HTTP MCP server."""
        logger.info(f"Starting HTTP MCP server on http://{self.host}:{self.port}")
        logger.info("Available tools:")
        for tool_name in self.tools.keys():
            logger.info(f"  - {tool_name}")
        
        uvicorn.run(
            self.app,
            host=self.host,
            port=self.port,
            log_level="info"
        )

if __name__ == "__main__":
    server = HTTPMCPServer()
    server.start()
