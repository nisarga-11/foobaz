import os
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from config.settings import settings
from http_mcp_client import SyncHTTPMCPClient

logger = logging.getLogger(__name__)

class MCPBackupTool:
    """MCP tool for creating PostgreSQL backups using pg_dump."""
    
    def __init__(self, mcp_server_url: str = None):
        self.name = "postgres_backup"
        self.description = "Create a backup of a PostgreSQL database using pg_dump"
        self.mcp_server_url = mcp_server_url or settings.MCP_SERVER_URL.replace("ws://", "http://").replace(":8080", ":8082")
        self.mcp_client = SyncHTTPMCPClient(self.mcp_server_url)
    
    def run(self, database_name: str) -> Dict[str, Any]:
        """Execute the backup operation via MCP."""
        try:
            # Call the MCP tool
            result = self.mcp_client.call_tool("postgres_backup", {
                "database_name": database_name
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Error calling MCP backup tool: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

class MCPRestoreTool:
    """MCP tool for restoring PostgreSQL databases using pg_restore."""
    
    def __init__(self, mcp_server_url: str = None):
        self.name = "postgres_restore"
        self.description = "Restore a PostgreSQL database from a backup using pg_restore"
        self.mcp_server_url = mcp_server_url or settings.MCP_SERVER_URL.replace("ws://", "http://").replace(":8080", ":8082")
        self.mcp_client = SyncHTTPMCPClient(self.mcp_server_url)
    
    def run(self, backup_file: str, database_name: str) -> Dict[str, Any]:
        """Execute the restore operation via MCP."""
        try:
            # Call the MCP tool
            result = self.mcp_client.call_tool("postgres_restore", {
                "backup_file": backup_file,
                "database_name": database_name
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Error calling MCP restore tool: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

class MCPListBackupsTool:
    """MCP tool for listing available backup files."""
    
    def __init__(self, mcp_server_url: str = None):
        self.name = "list_backups"
        self.description = "List available backup files for a database"
        self.mcp_server_url = mcp_server_url or settings.MCP_SERVER_URL.replace("ws://", "http://").replace(":8080", ":8082")
        self.mcp_client = SyncHTTPMCPClient(self.mcp_server_url)
    
    def run(self, database_name: str) -> Dict[str, Any]:
        """List backup files via MCP."""
        try:
            # Call the MCP tool
            result = self.mcp_client.call_tool("list_backups", {
                "database_name": database_name
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Error calling MCP list backups tool: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

class MCPDatabaseInfoTool:
    """MCP tool for getting database information and integrity status."""
    
    def __init__(self, mcp_server_url: str = None):
        self.name = "get_database_info"
        self.description = "Get database information including integrity status"
        self.mcp_server_url = mcp_server_url or settings.MCP_SERVER_URL.replace("ws://", "http://").replace(":8080", ":8082")
        self.mcp_client = SyncHTTPMCPClient(self.mcp_server_url)
    
    def run(self, database_name: str) -> Dict[str, Any]:
        """Get database information via MCP."""
        try:
            # Call the MCP tool
            result = self.mcp_client.call_tool("get_database_info", {
                "database_name": database_name
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Error calling MCP database info tool: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

class MCPPostgresStartTool:
    """MCP tool for starting PostgreSQL service."""
    
    def __init__(self, mcp_server_url: str = None):
        self.name = "postgres_start"
        self.description = "Start PostgreSQL service"
        self.mcp_server_url = mcp_server_url or settings.MCP_SERVER_URL.replace("ws://", "http://").replace(":8080", ":8082")
        self.mcp_client = SyncHTTPMCPClient(self.mcp_server_url)
    
    def run(self, database_name: str) -> Dict[str, Any]:
        """Start PostgreSQL service via MCP."""
        try:
            result = self.mcp_client.call_tool("postgres_start", {
                "database_name": database_name
            })
            return result
        except Exception as e:
            logger.error(f"Error calling MCP postgres start tool: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

class MCPPostgresStopTool:
    """MCP tool for stopping PostgreSQL service."""
    
    def __init__(self, mcp_server_url: str = None):
        self.name = "postgres_stop"
        self.description = "Stop PostgreSQL service"
        self.mcp_server_url = mcp_server_url or settings.MCP_SERVER_URL.replace("ws://", "http://").replace(":8080", ":8082")
        self.mcp_client = SyncHTTPMCPClient(self.mcp_server_url)
    
    def run(self, database_name: str) -> Dict[str, Any]:
        """Stop PostgreSQL service via MCP."""
        try:
            result = self.mcp_client.call_tool("postgres_stop", {
                "database_name": database_name
            })
            return result
        except Exception as e:
            logger.error(f"Error calling MCP postgres stop tool: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

# Create tool instances with MCP server URL from settings
backup_tool = MCPBackupTool()
restore_tool = MCPRestoreTool()
list_backups_tool = MCPListBackupsTool()
database_info_tool = MCPDatabaseInfoTool()
postgres_start_tool = MCPPostgresStartTool()
postgres_stop_tool = MCPPostgresStopTool()

# Import pgBackRest tools
from tools.pgbackrest_tools import PGBACKREST_TOOLS

# Export tools for use by agents
MCP_TOOLS = [
    backup_tool,
    restore_tool,
    list_backups_tool,
    database_info_tool,
    postgres_start_tool,
    postgres_stop_tool
] + PGBACKREST_TOOLS
