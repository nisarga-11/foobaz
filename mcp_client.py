"""MCP REST API client with error handling, retries, and timeouts."""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional

import httpx
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


class MCPResponse(BaseModel):
    """MCP API response model."""

    ok: bool
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class MCPTool(BaseModel):
    """MCP tool descriptor."""

    name: str
    description: str
    tool_schema: Dict[str, Any] = Field(default_factory=dict, alias="schema")


class MCPClient:
    """Client for interacting with MCP REST API."""

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        timeout: float = 60.0,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        """
        Initialize MCP client.

        Args:
            base_url: Base URL of the MCP server
            api_key: Optional API key for authentication
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries for failed requests
            retry_delay: Delay between retries in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        # Create httpx client with timeout and retry configuration
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        )

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.client.aclose()

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers including API key if provided."""
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    async def _make_request(
        self, method: str, endpoint: str, data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make HTTP request with retries and error handling.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            data: Request data for POST requests

        Returns:
            Response data as dictionary

        Raises:
            MCPError: If request fails after all retries
        """
        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers()

        for attempt in range(self.max_retries + 1):
            try:
                if method.upper() == "GET":
                    response = await self.client.get(url, headers=headers)
                elif method.upper() == "POST":
                    response = await self.client.post(url, headers=headers, json=data)
                else:
                    raise MCPError(f"Unsupported HTTP method: {method}")

                # Check for HTTP errors
                if response.status_code >= 500 and attempt < self.max_retries:
                    logger.warning(
                        f"Server error {response.status_code} on attempt {attempt + 1}, retrying..."
                    )
                    await asyncio.sleep(self.retry_delay * (2**attempt))  # Exponential backoff
                    continue

                response.raise_for_status()

                # Parse JSON response
                try:
                    return response.json()
                except json.JSONDecodeError as e:
                    raise MCPError(f"Invalid JSON response: {e}", response.status_code)

            except httpx.HTTPStatusError as e:
                if e.response.status_code >= 500 and attempt < self.max_retries:
                    logger.warning(
                        f"HTTP {e.response.status_code} on attempt {attempt + 1}, retrying..."
                    )
                    await asyncio.sleep(self.retry_delay * (2**attempt))
                    continue
                else:
                    error_detail = ""
                    try:
                        error_data = e.response.json()
                        error_detail = error_data.get("error", str(e))
                    except:
                        error_detail = str(e)

                    raise MCPError(
                        f"HTTP {e.response.status_code}: {error_detail}",
                        e.response.status_code,
                        getattr(e.response, "json", lambda: {})(),
                    )

            except httpx.RequestError as e:
                if attempt < self.max_retries:
                    logger.warning(f"Request error on attempt {attempt + 1}: {e}, retrying...")
                    await asyncio.sleep(self.retry_delay * (2**attempt))
                    continue
                else:
                    raise MCPError(f"Request failed after {self.max_retries + 1} attempts: {e}")

        raise MCPError(f"Request failed after {self.max_retries + 1} attempts")

    async def list_tools(self) -> List[MCPTool]:
        """
        List available MCP tools.

        Returns:
            List of available tools

        Raises:
            MCPError: If request fails
        """
        try:
            response_data = await self._make_request("GET", "/tools")

            if not isinstance(response_data, list):
                raise MCPError("Expected list response for /tools endpoint")

            tools = []
            for tool_data in response_data:
                try:
                    tools.append(MCPTool(**tool_data))
                except Exception as e:
                    logger.warning(f"Failed to parse tool data {tool_data}: {e}")
                    continue

            return tools

        except Exception as e:
            if isinstance(e, MCPError):
                raise
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
        try:
            payload = {"tool": tool, "arguments": arguments}
            response_data = await self._make_request("POST", "/invoke", payload)

            # Parse MCP response format
            try:
                mcp_response = MCPResponse(**response_data)
            except Exception as e:
                raise MCPError(f"Invalid MCP response format: {e}")

            if not mcp_response.ok:
                raise MCPError(f"Tool '{tool}' failed: {mcp_response.error}")

            return mcp_response.result or {}

        except Exception as e:
            if isinstance(e, MCPError):
                raise
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
        return f"MCPClient(base_url='{self.base_url}', timeout={self.timeout})"


# Synchronous wrapper for backward compatibility
class SyncMCPClient:
    """Synchronous wrapper for MCPClient."""

    def __init__(self, *args, **kwargs):
        self._async_client = MCPClient(*args, **kwargs)

    def list_tools(self) -> List[MCPTool]:
        """Synchronous version of list_tools."""
        return asyncio.run(self._async_client.list_tools())

    def invoke(self, tool: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Synchronous version of invoke."""
        try:
            # Check if there's already an event loop running
            try:
                loop = asyncio.get_running_loop()
                # If we get here, there's already a loop running
                raise RuntimeError("Cannot call invoke() from within an existing event loop. Use async version instead.")
            except RuntimeError:
                # No loop running, safe to use asyncio.run()
                return asyncio.run(self._async_client.invoke(tool, arguments))
        except Exception as e:
            # Log the actual error and re-raise it properly
            logger.error(f"MCP client invoke failed for tool '{tool}': {e}")
            if isinstance(e, MCPError):
                raise
            raise MCPError(f"Failed to invoke tool '{tool}': {e}")

    def health_check(self) -> Dict[str, Any]:
        """Synchronous version of health_check."""
        try:
            # Check if there's already an event loop running
            try:
                loop = asyncio.get_running_loop()
                raise RuntimeError("Cannot call health_check() from within an existing event loop. Use async version instead.")
            except RuntimeError:
                # No loop running, safe to use asyncio.run()
                return asyncio.run(self._async_client.health_check())
        except Exception as e:
            logger.error(f"MCP client health check failed: {e}")
            if isinstance(e, MCPError):
                raise
            raise MCPError(f"Health check failed: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        asyncio.run(self._async_client.__aexit__(exc_type, exc_val, exc_tb))
