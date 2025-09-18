"""Backup/Restore Agent for PG1 server."""

import os
from typing import Any, Dict, List

from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain.prompts import ChatPromptTemplate
from langchain.tools import BaseTool

from llm.ollama_helper import create_backup_agent_llm
from mcp_local.mcp_client import SyncMCPClient
from mcp_local.mcp_http_client import SyncMCPHTTPClient
from mcp_local.mcp_tools import create_pg1_toolset

# Server-specific system prompt
SYSTEM_PROMPT = """You are the Backup/Restore Agent for PG1. You use true MCP (Model Context Protocol) tools.
Rules:
- Always require `db_name` for any backup or restore.
- list_backups(db_name) yields available restore points; do not invent results.
- Full backup now: trigger_full_backup(db_name).
- Incremental backup now: trigger_incremental_backup(db_name).
- Enable schedules: enable_schedules(incremental_every="PT2M", full_cron="0 3 * * 0").
- Restore: Prefer backup_id if provided; otherwise accept target_timestamp (ISO8601).
- Never perform local DB/file operations.
- Return the MCP JSON responses as structured output.
- You communicate with PostgreSQL via true MCP protocol using JSON-RPC."""


class BackupAgentPG1:
    """Backup/Restore Agent for PG1 server."""

    def __init__(
        self,
        mcp1_base_url: str = None,
        mcp1_api_key: str = None,
        **llm_kwargs
    ):
        """
        Initialize PG1 backup agent.

        Args:
            mcp1_base_url: URL for MCP1 HTTP server (e.g., "http://localhost:8001")
            mcp1_api_key: API key for MCP1 server (optional)
            **llm_kwargs: Additional arguments for LLM
        """
        self.server_name = "PG1"
        
        # Check if we should use HTTP client (server running separately) or stdio client
        if mcp1_base_url and mcp1_base_url.startswith("http"):
            # Use HTTP client to connect to running server
            self.mcp_client = SyncMCPHTTPClient(base_url=mcp1_base_url)
            self.using_http = True
        else:
            # Use stdio client (spawn server process)
            self.mcp_client = SyncMCPClient(server_name="PG1")
            self.using_http = False
        
        # Create LLM
        self.llm = create_backup_agent_llm(**llm_kwargs)
        
        # Create tools
        self.tools = create_pg1_toolset(self.mcp_client)
        
        # Create prompt template
        self.prompt = ChatPromptTemplate.from_messages([
            ("system", SYSTEM_PROMPT),
            ("human", "{input}"),
            ("placeholder", "{agent_scratchpad}"),
        ])
        
        # Create agent
        self.agent = create_tool_calling_agent(
            llm=self.llm,
            tools=self.tools,
            prompt=self.prompt
        )
        
        # Create executor
        self.executor = AgentExecutor(
            agent=self.agent,
            tools=self.tools,
            verbose=True,
            max_iterations=10,
            return_intermediate_steps=True
        )

    def execute(self, instruction: str) -> Dict[str, Any]:
        """
        Execute a backup/restore instruction.

        Args:
            instruction: Natural language instruction

        Returns:
            Execution result with output and intermediate steps
        """
        try:
            result = self.executor.invoke({"input": instruction})
            return {
                "success": True,
                "output": result.get("output", ""),
                "intermediate_steps": result.get("intermediate_steps", []),
                "server": self.server_name
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "server": self.server_name
            }

    def list_backups(self, db_name: str, limit: int = 50) -> Dict[str, Any]:
        """
        List available backups for a database.

        Args:
            db_name: Database name
            limit: Maximum number of backups to return

        Returns:
            List backups result
        """
        instruction = f"List available backups for database '{db_name}' with limit {limit}"
        return self.execute(instruction)

    def trigger_full_backup(self, db_name: str) -> Dict[str, Any]:
        """
        Trigger a full backup for a database.

        Args:
            db_name: Database name

        Returns:
            Backup trigger result
        """
        instruction = f"Trigger a full backup for database '{db_name}'"
        return self.execute(instruction)

    def trigger_incremental_backup(self, db_name: str) -> Dict[str, Any]:
        """
        Trigger an incremental backup for a database.

        Args:
            db_name: Database name

        Returns:
            Backup trigger result
        """
        instruction = f"Trigger an incremental backup for database '{db_name}'"
        return self.execute(instruction)

    def restore_database(
        self,
        db_name: str,
        backup_id: str = None,
        target_timestamp: str = None
    ) -> Dict[str, Any]:
        """
        Restore a database from backup.

        Args:
            db_name: Database name
            backup_id: Specific backup ID (optional)
            target_timestamp: Target timestamp (optional)

        Returns:
            Restore result
        """
        if backup_id:
            instruction = f"Restore database '{db_name}' from backup ID '{backup_id}'"
        elif target_timestamp:
            instruction = f"Restore database '{db_name}' to timestamp '{target_timestamp}'"
        else:
            instruction = f"Restore database '{db_name}' from the latest backup"
        
        return self.execute(instruction)

    def enable_schedules(
        self,
        incremental_every: str = "PT2M",
        full_cron: str = "0 3 * * 0"
    ) -> Dict[str, Any]:
        """
        Enable backup schedules.

        Args:
            incremental_every: Incremental backup interval
            full_cron: Full backup cron expression

        Returns:
            Schedule enable result
        """
        instruction = (
            f"Enable backup schedules with incremental every '{incremental_every}' "
            f"and full backup cron '{full_cron}'"
        )
        return self.execute(instruction)

    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check.

        Returns:
            Health check result
        """
        instruction = "Perform a health check on the MCP server"
        return self.execute(instruction)

    def get_available_tools(self) -> List[str]:
        """
        Get list of available tool names.

        Returns:
            List of tool names
        """
        return [tool.name for tool in self.tools]


def create_pg1_agent_from_env() -> BackupAgentPG1:
    """
    Create PG1 backup agent using environment variables.

    Returns:
        Configured BackupAgentPG1 instance

    Raises:
        ValueError: If required environment variables are missing
    """
    from dotenv import load_dotenv
    load_dotenv()

    mcp1_base_url = os.getenv("MCP1_BASE_URL")
    if not mcp1_base_url:
        raise ValueError("MCP1_BASE_URL environment variable is required")

    mcp1_api_key = os.getenv("MCP1_API_KEY")

    return BackupAgentPG1(
        mcp1_base_url=mcp1_base_url,
        mcp1_api_key=mcp1_api_key
    )


if __name__ == "__main__":
    # Test the backup agent
    from dotenv import load_dotenv
    
    load_dotenv()
    
    try:
        agent = create_pg1_agent_from_env()
        print(f"PG1 Backup Agent created successfully")
        print(f"Available tools: {', '.join(agent.get_available_tools())}")
        
        # Test health check
        result = agent.health_check()
        if result["success"]:
            print("Health check passed")
        else:
            print(f"Health check failed: {result.get('error')}")
            
    except Exception as e:
        print(f"Failed to create PG1 agent: {e}")
