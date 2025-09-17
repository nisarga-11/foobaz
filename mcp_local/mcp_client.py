"""MCP client using the official Model Context Protocol via stdio transport."""

import asyncio
import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from pydantic import BaseModel, Field
from rich.console import Console

logger = logging.getLogger(__name__)
console = Console()


class MCPError(Exception):
    """Exception raised for MCP API errors."""

    def __init__(self, message: str, status_code: Optional[int] = None, response_data: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data


class MCPTool(BaseModel):
    """MCP tool descriptor."""

    name: str
    description: str
    tool_schema: Dict[str, Any] = Field(default_factory=dict, alias="inputSchema")


class MCPClient:
    """Client for interacting with MCP servers using official protocol."""

    def __init__(
        self,
        server_name: str,
        server_script_path: Optional[str] = None,
        server_args: Optional[List[str]] = None,
        timeout: float = 60.0,
    ):
        """
        Initialize MCP client.

        Args:
            server_name: Name of the MCP server (e.g., "PG1", "PG2")
            server_script_path: Path to the MCP server script
            server_args: Arguments to pass to the server script
            timeout: Request timeout in seconds
        """
        self.server_name = server_name
        self.timeout = timeout
        
        # Default server script path and args
        if server_script_path is None:
            project_root = Path(__file__).parent.parent
            server_script_path = str(project_root / "mcp_local" / "postgres_backup_server.py")
        
        if server_args is None:
            server_args = ["--server-name", server_name]
        
        self.server_params = StdioServerParameters(
            command=sys.executable,
            args=[server_script_path] + server_args,
            env=None
        )
        
        self._session: Optional[ClientSession] = None
        self._client_context = None

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()

    async def connect(self):
        """Connect to the MCP server."""
        try:
            logger.info(f"🔌 Connecting to MCP server {self.server_name}...")
            
            # Create stdio client
            self._client_context = stdio_client(self.server_params)
            read_stream, write_stream = await self._client_context.__aenter__()
            
            # Create session
            self._session = ClientSession(read_stream, write_stream)
            
            # Initialize the session
            init_result = await self._session.initialize()
            logger.info(f"✅ Connected to MCP server {self.server_name}")
            logger.info(f"Server info: {init_result}")
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to MCP server {self.server_name}: {e}")
            raise MCPError(f"Failed to connect to MCP server: {e}")

    async def disconnect(self):
        """Disconnect from the MCP server."""
        try:
            if self._session:
                await self._session.close()
                self._session = None
            
            if self._client_context:
                await self._client_context.__aexit__(None, None, None)
                self._client_context = None
                
            logger.info(f"🔌 Disconnected from MCP server {self.server_name}")
            
        except Exception as e:
            logger.warning(f"⚠️ Error during disconnect from {self.server_name}: {e}")

    async def list_tools(self) -> List[MCPTool]:
        """
        List available MCP tools.

        Returns:
            List of available tools

        Raises:
            MCPError: If request fails
        """
        if not self._session:
            raise MCPError("Not connected to MCP server")
        
        try:
            logger.debug(f"📋 Listing tools for {self.server_name}...")
            
            # List tools using MCP protocol
            tools_result = await self._session.list_tools()
            
            tools = []
            for tool in tools_result.tools:
                try:
                    mcp_tool = MCPTool(
                        name=tool.name,
                        description=tool.description,
                        inputSchema=tool.inputSchema or {}
                    )
                    tools.append(mcp_tool)
                except Exception as e:
                    logger.warning(f"Failed to parse tool {tool.name}: {e}")
                    continue

            logger.debug(f"📋 Found {len(tools)} tools for {self.server_name}")
            return tools

        except Exception as e:
            logger.error(f"❌ Failed to list tools for {self.server_name}: {e}")
            raise MCPError(f"Failed to list tools: {e}")

    async def invoke(self, tool: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """
        Invoke an MCP tool.

        Args:
            tool: Tool name to invoke
            arguments: Tool arguments

        Returns:
            Tool execution result

        Raises:
            MCPError: If tool invocation fails
        """
        if not self._session:
            raise MCPError("Not connected to MCP server")
        
        try:
            logger.debug(f"🔧 Invoking tool '{tool}' on {self.server_name} with args: {arguments}")
            
            # Call tool using MCP protocol
            result = await self._session.call_tool(tool, arguments)
            
            # Extract content from result
            if result.content:
                # Combine all content
                content_text = ""
                for content in result.content:
                    if hasattr(content, 'text'):
                        content_text += content.text
                    else:
                        content_text += str(content)
                
                # Try to parse as JSON
                try:
                    return json.loads(content_text)
                except json.JSONDecodeError:
                    # Return as plain text if not JSON
                    return {"result": content_text}
            
            # Check for errors
            if result.isError:
                error_msg = f"Tool '{tool}' failed"
                if result.content:
                    error_msg += f": {result.content[0].text if result.content else 'Unknown error'}"
                raise MCPError(error_msg)
            
            return {"result": "Tool executed successfully"}

        except Exception as e:
            if isinstance(e, MCPError):
                raise
            logger.error(f"❌ Failed to invoke tool '{tool}' on {self.server_name}: {e}")
            raise MCPError(f"Failed to invoke tool '{tool}': {e}")

    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on MCP server.

        Returns:
            Health check result

        Raises:
            MCPError: If health check fails
        """
        try:
            return await self.invoke("health", {})
        except Exception as e:
            if isinstance(e, MCPError):
                raise
            raise MCPError(f"Health check failed: {e}")

    def __repr__(self) -> str:
        return f"MCPClient(server_name='{self.server_name}', timeout={self.timeout})"


class SyncMCPClient:
    """Synchronous wrapper for MCPClient using true MCP protocol."""

    def __init__(self, server_name: str, **kwargs):
        """
        Initialize synchronous MCP client.
        
        Args:
            server_name: Name of the MCP server (e.g., "PG1", "PG2")
            **kwargs: Additional arguments for MCPClient
        """
        self.server_name = server_name
        self._async_client = MCPClient(server_name, **kwargs)
        self._loop = None
        self._closed = False
        self._connected = False

    def _ensure_loop(self):
        """Ensure we have an active event loop."""
        if self._closed:
            raise RuntimeError("SyncMCPClient has been closed")
        
        try:
            # Check if there's already an event loop running
            loop = asyncio.get_running_loop()
            # If we get here, there's already a loop running
            raise RuntimeError("Cannot use SyncMCPClient from within an existing event loop. Use async version instead.")
        except RuntimeError:
            # No loop running, which is what we want for sync client
            pass
        
        # Create a new event loop if we don't have one
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        
        return self._loop

    def _run_async(self, coro):
        """Run an async coroutine synchronously."""
        loop = self._ensure_loop()
        try:
            return loop.run_until_complete(coro)
        except Exception as e:
            # If the loop got closed externally, recreate it
            if loop.is_closed():
                self._loop = None
                loop = self._ensure_loop()
                return loop.run_until_complete(coro)
            else:
                raise

    def connect(self):
        """Connect to the MCP server synchronously."""
        if not self._connected:
            try:
                self._run_async(self._async_client.connect())
                self._connected = True
                logger.info(f"✅ SyncMCPClient connected to {self.server_name}")
            except Exception as e:
                logger.error(f"❌ SyncMCPClient failed to connect to {self.server_name}: {e}")
                raise

    def disconnect(self):
        """Disconnect from the MCP server synchronously."""
        if self._connected:
            try:
                self._run_async(self._async_client.disconnect())
                self._connected = False
                logger.info(f"🔌 SyncMCPClient disconnected from {self.server_name}")
            except Exception as e:
                logger.warning(f"⚠️ SyncMCPClient disconnect error for {self.server_name}: {e}")

    def list_tools(self) -> List[MCPTool]:
        """Synchronous version of list_tools."""
        if not self._connected:
            self.connect()
        
        try:
            return self._run_async(self._async_client.list_tools())
        except Exception as e:
            logger.error(f"❌ SyncMCPClient list_tools failed for {self.server_name}: {e}")
            if isinstance(e, MCPError):
                raise
            raise MCPError(f"Failed to list tools: {e}")

    def invoke(self, tool: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Synchronous version of invoke."""
        if not self._connected:
            self.connect()
        
        try:
            return self._run_async(self._async_client.invoke(tool, arguments))
        except Exception as e:
            logger.error(f"❌ SyncMCPClient invoke failed for tool '{tool}' on {self.server_name}: {e}")
            if isinstance(e, MCPError):
                raise
            raise MCPError(f"Failed to invoke tool '{tool}': {e}")

    def health_check(self) -> Dict[str, Any]:
        """Synchronous version of health_check."""
        if not self._connected:
            self.connect()
        
        try:
            return self._run_async(self._async_client.health_check())
        except Exception as e:
            logger.error(f"❌ SyncMCPClient health check failed for {self.server_name}: {e}")
            if isinstance(e, MCPError):
                raise
            raise MCPError(f"Health check failed: {e}")

    def close(self):
        """Close the client and clean up resources."""
        if not self._closed:
            self.disconnect()
            self._closed = True
            if self._loop and not self._loop.is_closed():
                # Close the loop gracefully
                pending = asyncio.all_tasks(self._loop)
                if pending:
                    for task in pending:
                        task.cancel()
                try:
                    self._loop.close()
                except:
                    # Ignore errors during cleanup
                    pass
            self._loop = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        """Cleanup when object is garbage collected."""
        if not self._closed:
            try:
                self.close()
            except:
                # Ignore errors during cleanup
                pass