import subprocess
import sys
import os
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from config.settings import settings
from http_mcp_client import SyncHTTPMCPClient

logger = logging.getLogger(__name__)

class PGBackRestTool:
    """Base class for pgBackRest operations."""
    
    def __init__(self, mcp_server_url: str = None):
        self.mcp_server_url = mcp_server_url or settings.MCP_SERVER_URL.replace("ws://", "http://").replace(":8080", ":8082")
        self.mcp_client = SyncHTTPMCPClient(self.mcp_server_url)
    
    def run_command(self, cmd: List[str]) -> Dict[str, Any]:
        """Run a shell command and return structured output."""
        try:
            logger.info(f"Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            
            return {
                "status": "success",
                "stdout": result.stdout,
                "stderr": result.stderr,
                "command": ' '.join(cmd)
            }
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {e.stderr}")
            return {
                "status": "error",
                "stdout": e.stdout,
                "stderr": e.stderr,
                "command": ' '.join(cmd),
                "error": str(e)
            }
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return {
                "status": "error",
                "stdout": "",
                "stderr": "",
                "command": ' '.join(cmd),
                "error": str(e)
            }

class PGBackRestFullBackupTool(PGBackRestTool):
    """MCP tool for creating full pgBackRest backups."""
    
    def __init__(self, mcp_server_url: str = None):
        super().__init__(mcp_server_url)
        self.name = "pgbackrest_full_backup"
        self.description = "Create a full backup using pgBackRest"
    
    def run(self, stanza: str = "demo", backup_path: str = "/var/lib/pgbackrest", server_name: str = "customerServer") -> Dict[str, Any]:
        """Execute full backup operation via MCP."""
        try:
            # Call the MCP tool
            result = self.mcp_client.call_tool("pgbackrest_full_backup", {
                "stanza": stanza,
                "backup_path": backup_path,
                "server_name": server_name
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Error calling MCP full backup tool: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

class PGBackRestIncrementalBackupTool(PGBackRestTool):
    """MCP tool for creating incremental pgBackRest backups."""
    
    def __init__(self, mcp_server_url: str = None):
        super().__init__(mcp_server_url)
        self.name = "pgbackrest_incremental_backup"
        self.description = "Create an incremental backup using pgBackRest"
    
    def run(self, stanza: str = "demo", backup_path: str = "/var/lib/pgbackrest", server_name: str = "customerServer") -> Dict[str, Any]:
        """Execute incremental backup operation via MCP."""
        try:
            # Call the MCP tool
            result = self.mcp_client.call_tool("pgbackrest_incremental_backup", {
                "stanza": stanza,
                "backup_path": backup_path,
                "server_name": server_name
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Error calling MCP incremental backup tool: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

class PGBackRestRestoreTool(PGBackRestTool):
    """MCP tool for restoring from pgBackRest backups."""
    
    def __init__(self, mcp_server_url: str = None):
        super().__init__(mcp_server_url)
        self.name = "pgbackrest_restore"
        self.description = "Restore from a pgBackRest backup"
    
    def run(self, stanza: str = "demo", pgdata: str = "/var/lib/postgresql/15/main", 
            backup_path: str = "/var/lib/pgbackrest", backup_type: str = "latest", server_name: str = "customerServer") -> Dict[str, Any]:
        """Execute restore operation via MCP."""
        try:
            # Call the MCP tool
            result = self.mcp_client.call_tool("pgbackrest_restore", {
                "stanza": stanza,
                "pgdata": pgdata,
                "backup_path": backup_path,
                "backup_type": backup_type,
                "server_name": server_name
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Error calling MCP restore tool: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

class PGBackRestListBackupsTool(PGBackRestTool):
    """MCP tool for listing available pgBackRest backups."""
    
    def __init__(self, mcp_server_url: str = None):
        super().__init__(mcp_server_url)
        self.name = "pgbackrest_list_backups"
        self.description = "List available pgBackRest backups with timestamps"
    
    def run(self, stanza: str = "demo", backup_path: str = "/var/lib/pgbackrest", server_name: str = "customerServer") -> Dict[str, Any]:
        """List available backups via MCP."""
        try:
            # Call the MCP tool
            result = self.mcp_client.call_tool("pgbackrest_list_backups", {
                "stanza": stanza,
                "backup_path": backup_path,
                "server_name": server_name
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Error calling MCP list backups tool: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

class PGBackRestInfoTool(PGBackRestTool):
    """MCP tool for getting pgBackRest information and status."""
    
    def __init__(self, mcp_server_url: str = None):
        super().__init__(mcp_server_url)
        self.name = "pgbackrest_info"
        self.description = "Get pgBackRest information and backup status"
    
    def run(self, stanza: str = "demo", backup_path: str = "/var/lib/pgbackrest", server_name: str = "customerServer") -> Dict[str, Any]:
        """Get pgBackRest information via MCP."""
        try:
            # Call the MCP tool
            result = self.mcp_client.call_tool("pgbackrest_info", {
                "stanza": stanza,
                "backup_path": backup_path,
                "server_name": server_name
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Error calling MCP info tool: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

class PGBackRestRecommendedBackupsTool(PGBackRestTool):
    """MCP tool for getting recommended pgBackRest backups."""
    
    def __init__(self, mcp_server_url: str = None):
        super().__init__(mcp_server_url)
        self.name = "pgbackrest_recommended_backups"
        self.description = "Get the 3 most recent incremental backups with consistent timestamps and provide recommendation"
    
    def run(self, stanza: str = "demo", backup_path: str = "/var/lib/pgbackrest", server_name: str = "customerServer") -> Dict[str, Any]:
        """Get recommended pgBackRest backups via MCP."""
        try:
            # Call the MCP tool
            result = self.mcp_client.call_tool("pgbackrest_recommended_backups", {
                "stanza": stanza,
                "backup_path": backup_path,
                "server_name": server_name
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Error calling MCP pgBackRest recommended backups tool: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

class PGBackRestPITRRestoreTool(PGBackRestTool):
    """MCP tool for performing Point-in-Time Recovery (PITR) restore."""
    
    def __init__(self, mcp_server_url: str = None):
        super().__init__(mcp_server_url)
        self.name = "pgbackrest_pitr_restore"
        self.description = "Perform Point-in-Time Recovery (PITR) restore to a specific target time"
    
    def run(self, stanza: str = "demo", pgdata: str = "/var/lib/postgresql/15/main", 
            backup_path: str = "/var/lib/pgbackrest", target_time: str = None, server_name: str = "customerServer") -> Dict[str, Any]:
        """Execute PITR restore operation via MCP."""
        try:
            # Call the MCP tool
            result = self.mcp_client.call_tool("pgbackrest_pitr_restore", {
                "stanza": stanza,
                "pgdata": pgdata,
                "backup_path": backup_path,
                "target_time": target_time,
                "server_name": server_name
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Error calling MCP PITR restore tool: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

class PGBackRestPITRRestoreWithWorkflowTool(PGBackRestTool):
    """MCP tool for performing complete PITR restore workflow with PostgreSQL stop/start and port management."""
    
    def __init__(self, mcp_server_url: str = None):
        super().__init__(mcp_server_url)
        self.name = "pgbackrest_pitr_restore_with_workflow"
        self.description = "Perform complete PITR restore workflow with PostgreSQL stop/start and port management"
    
    def run(self, stanza: str = "customer_demo", pgdata: str = "/Users/aarthiprashanth/postgres/pg-customer", 
            backup_path: str = "/Users/aarthiprashanth/pgbackrest-repo", backup_id: str = None, target_time: str = None, server_name: str = "customerServer") -> Dict[str, Any]:
        """Execute complete PITR restore workflow via MCP."""
        try:
            # Call the MCP tool
            result = self.mcp_client.call_tool("pgbackrest_pitr_restore_with_workflow", {
                "stanza": stanza,
                "pgdata": pgdata,
                "backup_path": backup_path,
                "backup_id": backup_id,
                "target_time": target_time,
                "server_name": server_name
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Error calling MCP PITR restore with workflow tool: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

class PGBackRestCoordinatedRecommendationsTool(PGBackRestTool):
    """MCP tool for finding coordinated backup recommendations with matching timestamps."""
    
    def __init__(self, mcp_server_url: str = None):
        super().__init__(mcp_server_url)
        self.name = "pgbackrest_coordinated_recommendations"
        self.description = "Find coordinated backup recommendations with matching timestamps between customer and employee servers"
    
    def run(self, customer_stanza: str = "customer_demo", employee_stanza: str = "employee_demo") -> Dict[str, Any]:
        """Get coordinated backup recommendations via MCP."""
        try:
            # Call the MCP tool
            result = self.mcp_client.call_tool("pgbackrest_coordinated_recommendations", {
                "customer_stanza": customer_stanza,
                "employee_stanza": employee_stanza
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Error calling MCP coordinated recommendations tool: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

# Create tool instances
pgbackrest_full_backup_tool = PGBackRestFullBackupTool()
pgbackrest_incremental_backup_tool = PGBackRestIncrementalBackupTool()
pgbackrest_restore_tool = PGBackRestRestoreTool()
pgbackrest_list_backups_tool = PGBackRestListBackupsTool()
pgbackrest_info_tool = PGBackRestInfoTool()
pgbackrest_recommended_backups_tool = PGBackRestRecommendedBackupsTool()
pgbackrest_pitr_restore_tool = PGBackRestPITRRestoreTool()
pgbackrest_pitr_restore_with_workflow_tool = PGBackRestPITRRestoreWithWorkflowTool()
pgbackrest_coordinated_recommendations_tool = PGBackRestCoordinatedRecommendationsTool()

# Export tools for use by agents
PGBACKREST_TOOLS = [
    pgbackrest_full_backup_tool,
    pgbackrest_incremental_backup_tool,
    pgbackrest_restore_tool,
    pgbackrest_list_backups_tool,
    pgbackrest_info_tool,
    pgbackrest_recommended_backups_tool,
    pgbackrest_pitr_restore_tool,
    pgbackrest_pitr_restore_with_workflow_tool,
    pgbackrest_coordinated_recommendations_tool
]
