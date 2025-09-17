#!/usr/bin/env python3
"""
MCP Tool for Intelligent Restore Recommendations

Provides LangChain tools for AI-powered restore recommendations.
"""

import json
import logging
from typing import Any, Dict, List, Optional, Type

from langchain.tools import BaseTool
from pydantic import BaseModel, Field

from .mcp_client import SyncMCPClient
from .mcp_tools import BaseMCPTool

logger = logging.getLogger(__name__)

class RestoreRecommendationInput(BaseModel):
    """Input for intelligent restore recommendations."""
    target_timestamp: Optional[str] = Field(
        None, 
        description="Optional target timestamp for recommendations (ISO format)"
    )
    num_recommendations: int = Field(
        3, 
        description="Number of recommendation sets to generate (default: 3)"
    )

class RecommendRestoreTool(BaseMCPTool):
    """Tool for generating intelligent restore recommendations."""

    name: str
    description: str = "Generate intelligent restore recommendations for coordinated multi-database restoration"
    args_schema: Type[BaseModel] = RestoreRecommendationInput

    def __init__(self, mcp_client: SyncMCPClient, server_name: str, tool_suffix: str = ""):
        name = f"{server_name.lower()}_recommend_restore"
        if tool_suffix:
            name += f"_{tool_suffix}"
        super().__init__(
            mcp_client=mcp_client,
            server_name=server_name,
            name=name,
            description=f"Generate AI-powered restore recommendations for all databases across servers",
        )

    def _run(
        self,
        target_timestamp: Optional[str] = None,
        num_recommendations: int = 3,
    ) -> str:
        """Execute the restore recommendation tool."""
        def execute_recommend():
            arguments = {
                "num_recommendations": num_recommendations
            }
            if target_timestamp:
                arguments["target_timestamp"] = target_timestamp
            return self.mcp_client.invoke("recommend_restore", arguments)
        
        return self._run_with_error_handling(execute_recommend, "recommend_restore")

class ExecuteRecommendedRestoreInput(BaseModel):
    """Input for executing a recommended restore plan."""
    recommendation_id: str = Field(
        description="ID of the recommendation to execute (e.g., 'rec_001')"
    )
    databases: Optional[List[str]] = Field(
        None,
        description="Optional list of specific databases to restore (if not provided, restores all in recommendation)"
    )

class ExecuteRecommendedRestoreTool(BaseMCPTool):
    """Tool for executing a specific restore recommendation."""

    name: str
    description: str = "Execute a specific restore recommendation across multiple databases"
    args_schema: Type[BaseModel] = ExecuteRecommendedRestoreInput

    def __init__(self, mcp_client: SyncMCPClient, server_name: str, tool_suffix: str = ""):
        name = f"{server_name.lower()}_execute_recommended_restore"
        if tool_suffix:
            name += f"_{tool_suffix}"
        super().__init__(
            mcp_client=mcp_client,
            server_name=server_name,
            name=name,
            description=f"Execute a coordinated restore across all databases using AI recommendations",
        )

    def _run(
        self,
        recommendation_id: str,
        databases: Optional[List[str]] = None,
    ) -> str:
        """Execute the recommended restore plan."""
        def execute_recommended_restore():
            arguments = {
                "recommendation_id": recommendation_id
            }
            if databases:
                arguments["databases"] = databases
            return self.mcp_client.invoke("execute_recommended_restore", arguments)
        
        return self._run_with_error_handling(execute_recommended_restore, "execute_recommended_restore")

def create_restore_recommendation_tools(
    mcp_client: SyncMCPClient, server_name: str, tool_suffix: str = ""
) -> List[BaseTool]:
    """Create restore recommendation tools for a server."""
    return [
        RecommendRestoreTool(mcp_client, server_name, tool_suffix),
        ExecuteRecommendedRestoreTool(mcp_client, server_name, tool_suffix),
    ]
