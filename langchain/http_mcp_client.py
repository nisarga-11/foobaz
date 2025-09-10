#!/usr/bin/env python3
"""
HTTP-based MCP client implementation.
"""

import json
import logging
import requests
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class HTTPMCPClient:
    """HTTP-based MCP client."""
    
    def __init__(self, server_url: str = "http://localhost:8082"):
        self.server_url = server_url
        self.request_id = 0
        
    def _send_request(self, method: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Send a request to the HTTP MCP server."""
        self.request_id += 1
        request = {
            "jsonrpc": "2.0",
            "id": self.request_id,
            "method": method,
            "params": params or {}
        }
        
        try:
            response = requests.post(
                f"{self.server_url}/mcp",
                json=request,
                headers={"Content-Type": "application/json"},
                timeout=300  # 5 minute timeout for restore operations
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP request failed: {e}")
            return {"error": str(e)}
    
    def health_check(self) -> Dict[str, Any]:
        """Check the health of the HTTP MCP server."""
        try:
            response = requests.get(f"{self.server_url}/health", timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"status": "unhealthy", "error": str(e)}
    
    def list_available_tools(self) -> List[str]:
        """List available tools from the HTTP MCP server."""
        try:
            response = requests.get(f"{self.server_url}/tools", timeout=10)
            response.raise_for_status()
            data = response.json()
            return data.get("tools", [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to list tools: {e}")
            return []
    
    def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Call a specific tool on the HTTP MCP server."""
        try:
            response = self._send_request("tools/call", {
                "name": tool_name,
                "arguments": arguments
            })
            
            if "result" in response:
                return response["result"]
            elif "error" in response:
                return {"error": response["error"]}
            return {"error": "Unknown response format"}
        except Exception as e:
            logger.error(f"Failed to call tool {tool_name}: {e}")
            return {"error": str(e)}
    
    def initialize(self) -> Dict[str, Any]:
        """Initialize the MCP connection."""
        return self._send_request("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "tools": {}
            },
            "clientInfo": {
                "name": "http-mcp-client",
                "version": "1.0.0"
            }
        })

# Synchronous wrapper for easier integration
class SyncHTTPMCPClient:
    """Synchronous wrapper for HTTP MCP client."""
    
    def __init__(self, server_url: str = "http://localhost:8082"):
        self.server_url = server_url
        self._client = HTTPMCPClient(server_url)
    
    def list_available_tools(self) -> List[str]:
        """List available tools synchronously."""
        return self._client.list_available_tools()
    
    def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Call a tool synchronously."""
        return self._client.call_tool(tool_name, arguments)
    
    def health_check(self) -> Dict[str, Any]:
        """Check health synchronously."""
        return self._client.health_check()
    
    def initialize(self) -> Dict[str, Any]:
        """Initialize synchronously."""
        return self._client.initialize()
