#!/usr/bin/env python3
"""
Standalone MCP Server for PG1
Run this in a separate terminal to provide MCP services for PG1 databases.
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from mcp_local.postgres_backup_server import main

async def start_pg1_server():
    """Start the PG1 MCP server."""
    print("🚀 Starting MCP Server for PG1")
    print("=" * 50)
    print("📋 Databases: customer_db, inventory_db, analytics_db")
    print("🔧 Protocol: JSON-RPC over stdio")
    print("⚡ Ready to receive MCP tool calls...")
    print()
    
    # Set server name argument
    sys.argv = ["postgres_backup_server.py", "--server-name", "PG1"]
    
    # Start the server
    await main()

if __name__ == "__main__":
    try:
        asyncio.run(start_pg1_server())
    except KeyboardInterrupt:
        print("\n🛑 PG1 MCP Server stopped")
    except Exception as e:
        print(f"❌ PG1 MCP Server failed: {e}")
        sys.exit(1)

