#!/usr/bin/env python3
"""
HTTP-based MCP Server that can run independently and be accessed by clients.
This provides MCP tools over HTTP while maintaining the MCP protocol structure.
"""

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from mcp_local.postgres_backup_server import PostgresBackupMCPServer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("mcp-http-server")

class MCPRequest(BaseModel):
    """MCP tool invocation request."""
    tool: str
    arguments: Dict[str, Any]

class MCPResponse(BaseModel):
    """MCP tool invocation response."""
    ok: bool
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

class MCPHTTPServer:
    """HTTP wrapper for MCP server that can run independently."""
    
    def __init__(self, server_name: str, port: int):
        self.server_name = server_name
        self.port = port
        
        # Create the underlying MCP server
        self.mcp_server = PostgresBackupMCPServer(server_name)
        self.mcp_server.setup_handlers()
        
        # Create FastAPI app
        self.app = FastAPI(
            title=f"MCP HTTP Server - {server_name}",
            description=f"HTTP wrapper for MCP tools on {server_name}",
            version="1.0.0"
        )
        
        # Add CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        self._setup_routes()
        
        logger.info(f"MCP HTTP Server initialized for {server_name} on port {port}")
        logger.info(f"Available routes: {[route.path for route in self.app.routes]}")
    
    def _setup_routes(self):
        """Set up HTTP routes that expose MCP functionality."""
        
        # Store reference to self for closures
        mcp_server = self.mcp_server
        server_name = self.server_name
        
        @self.app.get("/")
        async def root():
            """Root endpoint with server info."""
            return {
                "server": server_name,
                "type": "MCP HTTP Server",
                "timestamp": datetime.now().isoformat(),
                "endpoints": {
                    "tools": "/tools",
                    "invoke": "/invoke", 
                    "health": "/health"
                }
            }
        
        @self.app.get("/tools")
        async def list_tools():
            """List available MCP tools."""
            try:
                tool_handlers = [
                    {"name": "list_backups", "description": f"List available backups for databases on {server_name}"},
                    {"name": "trigger_full_backup", "description": f"Trigger a full backup for a database on {server_name}"},
                    {"name": "trigger_incremental_backup", "description": f"Trigger an incremental WAL backup for a database on {server_name}"},
                    {"name": "restore_database", "description": f"Restore a database from backup on {server_name}"},
                    {"name": "enable_schedules", "description": f"Enable backup schedules on {server_name}"},
                    {"name": "health", "description": f"Perform health check on {server_name}"}
                ]
                
                return tool_handlers
                
            except Exception as e:
                logger.error(f"Failed to list tools: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/invoke")
        async def invoke_tool(request: MCPRequest):
            """Invoke an MCP tool."""
            try:
                logger.info(f"Invoking tool '{request.tool}' with args: {request.arguments}")
                
                # Call the MCP server's tool handler directly
                result = await mcp_server._call_tool_direct(request.tool, request.arguments)
                
                if result["ok"]:
                    return MCPResponse(ok=True, result=result["result"])
                else:
                    return MCPResponse(ok=False, error=result["error"])
                
            except Exception as e:
                logger.error(f"Tool invocation failed: {e}")
                return MCPResponse(ok=False, error=str(e))
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint."""
            try:
                # Call the health tool directly
                result = await mcp_server._call_tool_direct("health", {})
                
                if result["ok"]:
                    return result["result"]
                else:
                    return {
                        "status": "unhealthy",
                        "server": server_name,
                        "error": result["error"]
                    }
                
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                return {
                    "status": "unhealthy",
                    "server": server_name,
                    "error": str(e)
                }

async def start_server(server_name: str, port: int):
    """Start the MCP HTTP server."""
    logger.info(f"Starting MCP HTTP Server for {server_name} on port {port}")
    
    # Create server instance
    mcp_http_server = MCPHTTPServer(server_name, port)
    
    # Configure uvicorn
    config = uvicorn.Config(
        app=mcp_http_server.app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
    
    # Start server
    server = uvicorn.Server(config)
    await server.serve()

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="MCP HTTP Server")
    parser.add_argument("--server-name", required=True, choices=["PG1", "PG2"],
                       help="Server name (PG1 or PG2)")
    parser.add_argument("--port", type=int, 
                       help="Port to run server on (default: 8001 for PG1, 8002 for PG2)")
    
    args = parser.parse_args()
    
    # Default ports
    if args.port is None:
        args.port = 8001 if args.server_name == "PG1" else 8002
    
    try:
        asyncio.run(start_server(args.server_name, args.port))
    except KeyboardInterrupt:
        logger.info(f"MCP HTTP Server {args.server_name} stopped")
    except Exception as e:
        logger.error(f"MCP HTTP Server {args.server_name} failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
