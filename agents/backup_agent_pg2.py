
"""Backup/Restore Agent for PG2 server."""

import os
from typing import Any, Dict, List

from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain.prompts import ChatPromptTemplate
from langchain.tools import BaseTool

from llm.ollama_helper import create_backup_agent_llm
from mcp.mcp_client import SyncMCPClient
from mcp.mcp_tools import create_pg2_toolset

# Server-specific system prompt
SYSTEM_PROMPT = """You are the Backup/Restore Agent for PG2. You only act via MCP tools over HTTP.
Rules:
- Always require `db_name` for any backup or restore.
- list_backups(db_name) yields available restore points; do not invent results.
- Full backup now: trigger_full_backup(db_name).
- Incremental backup now: trigger_incremental_backup(db_name).
- Enable schedules: enable_schedules(incremental_every="PT2M", full_cron="0 3 * * 0").
- Restore: Prefer backup_id if provided; otherwise accept target_timestamp (ISO8601).
- Never perform local DB/file operations.
- Return the MCP JSON responses as structured output."""


class BackupAgentPG2:
    """Backup/Restore Agent for PG2 server."""

    def __init__(
        self,
        mcp2_base_url: str,
        mcp2_api_key: str = None,
        **llm_kwargs
    ):
        """
        Initialize PG2 backup agent.

        Args:
            mcp2_base_url: Base URL for MCP2 server
            mcp2_api_key: API key for MCP2 server
            **llm_kwargs: Additional arguments for LLM
        """
        self.server_name = "PG2"
        self.mcp_client = SyncMCPClient(
            base_url=mcp2_base_url,
            api_key=mcp2_api_key
        )
        
        # Create LLM
        self.llm = create_backup_agent_llm(**llm_kwargs)
        
        # Create tools
        self.tools = create_pg2_toolset(self.mcp_client)
        
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


def create_pg2_agent_from_env() -> BackupAgentPG2:
    """
    Create PG2 backup agent using environment variables.

    Returns:
        Configured BackupAgentPG2 instance

    Raises:
        ValueError: If required environment variables are missing
    """
    from dotenv import load_dotenv
    load_dotenv()

    mcp2_base_url = os.getenv("MCP2_BASE_URL")
    if not mcp2_base_url:
        raise ValueError("MCP2_BASE_URL environment variable is required")

    mcp2_api_key = os.getenv("MCP2_API_KEY")

    return BackupAgentPG2(
        mcp2_base_url=mcp2_base_url,
        mcp2_api_key=mcp2_api_key
    )


if __name__ == "__main__":
    # Test the backup agent
    from dotenv import load_dotenv
    
    load_dotenv()
    
    try:
        agent = create_pg2_agent_from_env()
        print(f"✅ PG2 Backup Agent created successfully")
        print(f"Available tools: {', '.join(agent.get_available_tools())}")
        
        # Test health check
        result = agent.health_check()
        if result["success"]:
            print("✅ Health check passed")
        else:
            print(f"❌ Health check failed: {result.get('error')}")
            
    except Exception as e:
        print(f"❌ Failed to create PG2 agent: {e}")
