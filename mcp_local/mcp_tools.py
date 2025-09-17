"""LangChain Tools that wrap MCP REST API calls."""

import json
import logging
from typing import Any, Dict, List, Optional, Type

from langchain.tools import BaseTool
from pydantic import BaseModel, Field

from .mcp_client import MCPClient, MCPError, SyncMCPClient

logger = logging.getLogger(__name__)


class ListBackupsInput(BaseModel):
    """Input for listing backups."""

    db_name: str = Field(description="Database name to list backups for")
    limit: int = Field(default=50, description="Maximum number of backups to return")


class TriggerBackupInput(BaseModel):
    """Input for triggering backups."""

    db_name: str = Field(description="Database name to backup")


class RestoreDatabaseInput(BaseModel):
    """Input for restoring database."""

    db_name: str = Field(description="Database name to restore")
    backup_id: Optional[str] = Field(default=None, description="Specific backup ID to restore")
    target_timestamp: Optional[str] = Field(
        default=None, description="Target timestamp in ISO8601 format (alternative to backup_id)"
    )


class EnableSchedulesInput(BaseModel):
    """Input for enabling backup schedules."""

    incremental_every: str = Field(
        default="PT2M", description="Incremental backup interval in ISO8601 duration format"
    )
    full_cron: str = Field(
        default="0 3 * * 0", description="Full backup cron expression (weekly Sunday 03:00)"
    )


class HealthInput(BaseModel):
    """Input for health check (no parameters needed)."""

    pass


class ListAvailableMCPToolsInput(BaseModel):
    """Input for listing available MCP tools (no parameters needed)."""

    pass


class BaseMCPTool(BaseTool):
    """Base class for MCP tools."""
    
    # Class attributes to avoid Pydantic field issues
    mcp_client: SyncMCPClient = None
    server_name: str = None

    def __init__(self, mcp_client: SyncMCPClient, server_name: str, **kwargs):
        super().__init__(**kwargs)
        # Use object.__setattr__ to bypass Pydantic validation
        object.__setattr__(self, 'mcp_client', mcp_client)
        object.__setattr__(self, 'server_name', server_name)

    def _handle_mcp_error(self, e: Exception, tool_name: str) -> str:
        """Handle MCP errors and return formatted error message."""
        if isinstance(e, MCPError):
            error_msg = f"MCP tool '{tool_name}' failed on {self.server_name}: {e}"
            if e.status_code:
                error_msg += f" (HTTP {e.status_code})"
            logger.error(error_msg)
            return error_msg
        else:
            error_msg = f"Unexpected error in MCP tool '{tool_name}' on {self.server_name}: {e}"
            logger.error(error_msg)
            return error_msg
            
    def _run_with_error_handling(self, func, tool_name: str) -> str:
        """Execute a function with proper error handling that prevents AI hallucination."""
        try:
            result = func()
            return self._format_result(result, tool_name)
        except Exception as e:
            error_msg = self._handle_mcp_error(e, tool_name)
            # Raise the error to prevent the AI from hallucinating success
            # The error will be caught by the agent and properly reported
            raise RuntimeError(error_msg)

    def _format_result(self, result: Dict[str, Any], tool_name: str) -> str:
        """Format MCP result for display."""
        formatted = f"MCP tool '{tool_name}' result from {self.server_name}:\n"
        formatted += json.dumps(result, indent=2, default=str)
        return formatted


class ListBackupsTool(BaseMCPTool):
    """Tool for listing available backups."""

    name: str
    description: str = "List available backups for a specific database"
    args_schema: Type[BaseModel] = ListBackupsInput

    def __init__(self, mcp_client: SyncMCPClient, server_name: str, tool_suffix: str = ""):
        name = f"{server_name.lower()}_list_backups"
        if tool_suffix:
            name += f"_{tool_suffix}"
        super().__init__(
            mcp_client=mcp_client,
            server_name=server_name,
            name=name,
            description=f"List available backups for a database on {server_name}",
        )

    def _run(self, db_name: str, limit: int = 50) -> str:
        """Execute the list backups tool."""
        def execute_list():
            return self.mcp_client.invoke("list_backups", {"db_name": db_name, "limit": limit})
        
        return self._run_with_error_handling(execute_list, "list_backups")


class TriggerFullBackupTool(BaseMCPTool):
    """Tool for triggering full backup."""

    name: str
    description: str = "Trigger a full backup for a specific database"
    args_schema: Type[BaseModel] = TriggerBackupInput

    def __init__(self, mcp_client: SyncMCPClient, server_name: str, tool_suffix: str = ""):
        name = f"{server_name.lower()}_trigger_full_backup"
        if tool_suffix:
            name += f"_{tool_suffix}"
        super().__init__(
            mcp_client=mcp_client,
            server_name=server_name,
            name=name,
            description=f"Trigger a full backup for a database on {server_name}",
        )

    def _run(self, db_name: str) -> str:
        """Execute the trigger full backup tool."""
        try:
            result = self.mcp_client.invoke("trigger_full_backup", {"db_name": db_name})
            return self._format_result(result, "trigger_full_backup")
        except Exception as e:
            return self._handle_mcp_error(e, "trigger_full_backup")


class TriggerIncrementalBackupTool(BaseMCPTool):
    """Tool for triggering incremental backup."""

    name: str
    description: str = "Trigger an incremental backup for a specific database"
    args_schema: Type[BaseModel] = TriggerBackupInput

    def __init__(self, mcp_client: SyncMCPClient, server_name: str, tool_suffix: str = ""):
        name = f"{server_name.lower()}_trigger_incremental_backup"
        if tool_suffix:
            name += f"_{tool_suffix}"
        super().__init__(
            mcp_client=mcp_client,
            server_name=server_name,
            name=name,
            description=f"Trigger an incremental backup for a database on {server_name}",
        )

    def _run(self, db_name: str) -> str:
        """Execute the trigger incremental backup tool."""
        try:
            result = self.mcp_client.invoke("trigger_incremental_backup", {"db_name": db_name})
            return self._format_result(result, "trigger_incremental_backup")
        except Exception as e:
            return self._handle_mcp_error(e, "trigger_incremental_backup")


class RestoreDatabaseTool(BaseMCPTool):
    """Tool for restoring database."""

    name: str
    description: str = "Restore a database from backup"
    args_schema: Type[BaseModel] = RestoreDatabaseInput

    def __init__(self, mcp_client: SyncMCPClient, server_name: str, tool_suffix: str = ""):
        name = f"{server_name.lower()}_restore_database"
        if tool_suffix:
            name += f"_{tool_suffix}"
        super().__init__(
            mcp_client=mcp_client,
            server_name=server_name,
            name=name,
            description=f"Restore a database from backup on {server_name}",
        )

    def _run(
        self,
        db_name: str,
        backup_id: Optional[str] = None,
        target_timestamp: Optional[str] = None,
    ) -> str:
        """Execute the restore database tool."""
        def execute_restore():
            arguments = {"db_name": db_name}
            if backup_id:
                arguments["backup_id"] = backup_id
            if target_timestamp:
                arguments["target_timestamp"] = target_timestamp
            return self.mcp_client.invoke("restore_database", arguments)
        
        return self._run_with_error_handling(execute_restore, "restore_database")


class EnableSchedulesTool(BaseMCPTool):
    """Tool for enabling backup schedules."""

    name: str
    description: str = "Enable backup schedules (incremental and full)"
    args_schema: Type[BaseModel] = EnableSchedulesInput

    def __init__(self, mcp_client: SyncMCPClient, server_name: str, tool_suffix: str = ""):
        name = f"{server_name.lower()}_enable_schedules"
        if tool_suffix:
            name += f"_{tool_suffix}"
        super().__init__(
            mcp_client=mcp_client,
            server_name=server_name,
            name=name,
            description=f"Enable backup schedules on {server_name}",
        )

    def _run(self, incremental_every: str = "PT2M", full_cron: str = "0 3 * * 0") -> str:
        """Execute the enable schedules tool."""
        try:
            result = self.mcp_client.invoke(
                "enable_schedules",
                {"incremental_every": incremental_every, "full_cron": full_cron},
            )
            return self._format_result(result, "enable_schedules")
        except Exception as e:
            return self._handle_mcp_error(e, "enable_schedules")


class ListAvailableMCPToolsTool(BaseMCPTool):
    """Tool for listing available MCP tools."""

    name: str
    description: str = "List all available MCP tools on the server"
    args_schema: Type[BaseModel] = ListAvailableMCPToolsInput

    def __init__(self, mcp_client: SyncMCPClient, server_name: str, tool_suffix: str = ""):
        name = f"{server_name.lower()}_list_tools"
        if tool_suffix:
            name += f"_{tool_suffix}"
        super().__init__(
            mcp_client=mcp_client,
            server_name=server_name,
            name=name,
            description=f"List all available MCP tools on {server_name}",
        )

    def _run(self) -> str:
        """Execute the list tools tool."""
        try:
            tools = self.mcp_client.list_tools()
            result = {"tools": [{"name": tool.name, "description": tool.description} for tool in tools]}
            return self._format_result(result, "list_tools")
        except Exception as e:
            return self._handle_mcp_error(e, "list_tools")


class HealthTool(BaseMCPTool):
    """Tool for health check."""

    name: str
    description: str = "Perform health check on the MCP server"
    args_schema: Type[BaseModel] = HealthInput

    def __init__(self, mcp_client: SyncMCPClient, server_name: str, tool_suffix: str = ""):
        name = f"{server_name.lower()}_health"
        if tool_suffix:
            name += f"_{tool_suffix}"
        super().__init__(
            mcp_client=mcp_client,
            server_name=server_name,
            name=name,
            description=f"Perform health check on {server_name}",
        )

    def _run(self) -> str:
        """Execute the health check tool."""
        try:
            result = self.mcp_client.health_check()
            return self._format_result(result, "health")
        except Exception as e:
            return self._handle_mcp_error(e, "health")


def create_mcp_toolset(
    mcp_client: SyncMCPClient, server_name: str, tool_suffix: str = ""
) -> List[BaseTool]:
    """
    Factory function to create a complete toolset for an MCP server.

    Args:
        mcp_client: MCP client instance
        server_name: Name of the server (e.g., "PG1", "PG2")
        tool_suffix: Optional suffix for tool names

    Returns:
        List of configured MCP tools
    """
    from .restore_recommendation_tool import create_restore_recommendation_tools
    
    tools = [
        ListBackupsTool(mcp_client, server_name, tool_suffix),
        TriggerFullBackupTool(mcp_client, server_name, tool_suffix),
        TriggerIncrementalBackupTool(mcp_client, server_name, tool_suffix),
        RestoreDatabaseTool(mcp_client, server_name, tool_suffix),
        EnableSchedulesTool(mcp_client, server_name, tool_suffix),
        ListAvailableMCPToolsTool(mcp_client, server_name, tool_suffix),
        HealthTool(mcp_client, server_name, tool_suffix),
    ]
    
    # Add restore recommendation tools
    tools.extend(create_restore_recommendation_tools(mcp_client, server_name, tool_suffix))
    
    return tools


def create_pg1_toolset(mcp1_client: SyncMCPClient) -> List[BaseTool]:
    """Create toolset for PG1 server."""
    return create_mcp_toolset(mcp1_client, "PG1")


def create_pg2_toolset(mcp2_client: SyncMCPClient) -> List[BaseTool]:
    """Create toolset for PG2 server."""
    return create_mcp_toolset(mcp2_client, "PG2")
