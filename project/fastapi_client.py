"""
Python client library for the FastAPI MCP Backup Server.
Provides simple interface for backup operations.
"""

import requests
from typing import List, Dict, Any, Optional
from datetime import datetime


class MCPBackupClient:
    """Client for interacting with FastAPI MCP Backup Server."""
    
    def __init__(self, base_url: str = "http://localhost:8001"):
        """
        Initialize the client.
        
        Args:
            base_url: Base URL of the FastAPI server
        """
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
    
    def _post(self, endpoint: str, data: dict) -> dict:
        """Make POST request."""
        url = f"{self.base_url}{endpoint}"
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()
    
    def _get(self, endpoint: str, params: dict = None) -> dict:
        """Make GET request."""
        url = f"{self.base_url}{endpoint}"
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    # ========================================================================
    # Server Management
    # ========================================================================
    
    def health_check(self) -> Dict[str, Any]:
        """Check health of all MCP servers."""
        return self._get("/health")
    
    def list_servers(self) -> Dict[str, Any]:
        """List all available MCP servers."""
        return self._get("/servers")
    
    def list_server_tools(self, server_name: str) -> Dict[str, Any]:
        """List tools available on a specific server."""
        return self._get(f"/servers/{server_name}/tools")
    
# Add this method to your MCPBackupClient class in fastapi_client.py

    def list_server_databases(self, server_name: str) -> dict:
        """
        List all databases for a specific server.
        
        Args:
            server_name: Name of the PostgreSQL server
            
        Returns:
            dict with 'databases' key containing list of database names
        """
        response = requests.get(
            f"{self.base_url}/servers/{server_name}/databases",
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    
    def automated_restore(
        self,
        server_name: str,
        db_name: str,
        backup_id: str
    ) -> Dict[str, Any]:
        """
        Execute automated restore on a specific server.
        
        Args:
            server_name: Name of the server
            db_name: Database name
            backup_id: Backup ID to restore from
            
        Returns:
            Dictionary containing restore result
        """
        return self._post(
            f"/servers/{server_name}/restore/automated",
            {
                "db_name": db_name,
                "backup_id": backup_id
            }
        )
    
    # ========================================================================
    # Single Server Operations
    # ========================================================================
    
    def list_backups(
        self,
        server_name: str,
        db_name: str,
        limit: int = 50
    ) -> Dict[str, Any]:
        """
        List backups for a database on a specific server.
        
        Args:
            server_name: Name of the server (e.g., "PG1")
            db_name: Database name
            limit: Maximum number of backups to return
        
        Returns:
            Dictionary containing backup list
        """
        return self._post(
            f"/servers/{server_name}/backups/list",
            {"db_name": db_name, "limit": limit}
        )
    
    def trigger_backup(
        self,
        server_name: str,
        db_name: str,
        backup_type: str = "full"
    ) -> Dict[str, Any]:
        """
        Trigger a backup on a specific server.
        
        Args:
            server_name: Name of the server
            db_name: Database name
            backup_type: Type of backup ("full" or "incremental")
        
        Returns:
            Dictionary containing backup result
        """
        return self._post(
            f"/servers/{server_name}/backups/trigger",
            {"db_name": db_name, "backup_type": backup_type}
        )
    
    def restore_database(
        self,
        server_name: str,
        db_name: str,
        backup_id: Optional[str] = None,
        target_timestamp: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Restore a database on a specific server.
        
        Args:
            server_name: Name of the server
            db_name: Database name
            backup_id: Specific backup ID to restore
            target_timestamp: Target timestamp for restore (ISO8601)
        
        Returns:
            Dictionary containing restore result
        """
        data = {"db_name": db_name}
        if backup_id:
            data["backup_id"] = backup_id
        if target_timestamp:
            data["target_timestamp"] = target_timestamp
        
        return self._post(f"/servers/{server_name}/restore", data)
    
    def enable_schedules(
        self,
        server_name: str,
        incremental_every: str = "PT2M",
        full_cron: str = "0 3 * * 0"
    ) -> Dict[str, Any]:
        """
        Enable backup schedules on a specific server.
        
        Args:
            server_name: Name of the server
            incremental_every: Incremental backup interval (ISO8601 duration)
            full_cron: Full backup cron expression
        
        Returns:
            Dictionary containing result
        """
        return self._post(
            f"/servers/{server_name}/schedules/enable",
            {
                "incremental_every": incremental_every,
                "full_cron": full_cron
            }
        )
    
    # ========================================================================
    # Multi-Server Operations
    # ========================================================================
    
    def multi_server_backup(
        self,
        servers: List[str],
        db_name: str,
        backup_type: str = "full"
    ) -> Dict[str, Any]:
        """
        Trigger coordinated backup across multiple servers.
        
        Args:
            servers: List of server names (e.g., ["PG1", "PG2"])
            db_name: Database name
            backup_type: Type of backup
        
        Returns:
            Dictionary containing results from all servers
        """
        return self._post(
            "/multi-server/backup",
            {
                "servers": servers,
                "db_name": db_name,
                "backup_type": backup_type
            }
        )
    
    def multi_server_restore(
        self,
        servers: List[str],
        db_name: str,
        target_timestamp: str
    ) -> Dict[str, Any]:
        """
        Trigger coordinated restore across multiple servers.
        
        Args:
            servers: List of server names
            db_name: Database name
            target_timestamp: Target timestamp for restore (ISO8601)
        
        Returns:
            Dictionary containing results from all servers
        """
        return self._post(
            "/multi-server/restore",
            {
                "servers": servers,
                "db_name": db_name,
                "target_timestamp": target_timestamp
            }
        )
    
    def recommend_restore_point(
        self,
        db_name: str,
        target_timestamp: str,
        servers: List[str] = None
    ) -> Dict[str, Any]:
        """
        Get restore point recommendations for coordinated restore.
        
        Args:
            db_name: Database name
            target_timestamp: Target timestamp
            servers: List of server names (defaults to ["PG1", "PG2"])
        
        Returns:
            Dictionary containing recommendations
        """
        if servers is None:
            servers = ["PG1", "PG2"]
        
        return self._get(
            "/backups/recommend",
            {
                "db_name": db_name,
                "target_timestamp": target_timestamp,
                "servers": ",".join(servers)
            }
        )


# ============================================================================
# Example Usage
# ============================================================================

def example_usage():
    """Example usage of the MCP Backup Client."""
    
    # Create client
    client = MCPBackupClient("http://localhost:8000")
    
    print("=" * 70)
    print("FastAPI MCP Backup Client - Example Usage")
    print("=" * 70)
    
    # 1. Health Check
    print("\n1. Health Check")
    print("-" * 70)
    health = client.health_check()
    for server, status in health.items():
        print(f"{server}: {status['status']}")
    
    # 2. List Servers
    print("\n2. List Servers")
    print("-" * 70)
    servers = client.list_servers()
    for server in servers['servers']:
        print(f"  - {server['name']} (connected: {server['connected']})")
    
    # 3. List Databases for Each Server
    print("\n3. List Databases")
    print("-" * 70)
    try:
        for server in servers['servers']:
            if server['connected']:
                server_name = server['name']
                databases = client.list_server_databases(server_name)
                print(f"{server_name}: {', '.join(databases.get('databases', []))}")
    except Exception as e:
        print(f"Error: {e}")
    
    # 4. List Backups
    print("\n4. List Backups for PG1")
    print("-" * 70)
    try:
        backups = client.list_backups("PG1", "myapp_db", limit=5)
        print(f"Found {len(backups.get('backups', []))} backups")
        for backup in backups.get('backups', [])[:3]:
            print(f"  - {backup.get('id')}: {backup.get('timestamp')}")
    except Exception as e:
        print(f"Error: {e}")
    
    # 5. Trigger Full Backup
    print("\n5. Trigger Full Backup on PG1")
    print("-" * 70)
    try:
        result = client.trigger_backup("PG1", "myapp_db", "full")
        print(f"Backup triggered: {result}")
    except Exception as e:
        print(f"Error: {e}")
    
    # 6. Multi-Server Backup
    print("\n6. Coordinated Multi-Server Backup")
    print("-" * 70)
    try:
        result = client.multi_server_backup(
            ["PG1", "PG2"],
            "myapp_db",
            "full"
        )
        print(f"Operation: {result['operation']}")
        for server, status in result['results'].items():
            print(f"  {server}: {status['status']}")
    except Exception as e:
        print(f"Error: {e}")
    
    # 7. Get Restore Recommendations
    print("\n7. Get Restore Recommendations")
    print("-" * 70)
    try:
        target_time = datetime.now().isoformat()
        recommendations = client.recommend_restore_point(
            "myapp_db",
            target_time,
            ["PG1", "PG2"]
        )
        print(f"Target: {recommendations['target_timestamp']}")
        for server, rec in recommendations['recommendations'].items():
            backup = rec['recommended_backup']
            time_diff = rec['time_difference_seconds']
            print(f"  {server}: {backup['id']} (±{time_diff}s)")
    except Exception as e:
        print(f"Error: {e}")
    
    # 8. Multi-Server Restore
    print("\n8. Coordinated Multi-Server Restore")
    print("-" * 70)
    try:
        target_time = datetime.now().isoformat()
        result = client.multi_server_restore(
            ["PG1", "PG2"],
            "myapp_db",
            target_time
        )
        print(f"Operation: {result['operation']}")
        for server, status in result['results'].items():
            print(f"  {server}: {status['status']}")
    except Exception as e:
        print(f"Error: {e}")
    
    # 9. Enable Schedules
    print("\n9. Enable Backup Schedules on PG1")
    print("-" * 70)
    try:
        result = client.enable_schedules(
            "PG1",
            incremental_every="PT2M",  # Every 2 minutes
            full_cron="0 3 * * 0"      # Sundays at 3 AM
        )
        print(f"Schedules enabled: {result}")
    except Exception as e:
        print(f"Error: {e}")
    
    # 10. Automated Restore
    print("\n10. Automated Restore Example")
    print("-" * 70)
    try:
        result = client.automated_restore(
            "PG1",
            "myapp_db",
            "backup_20241208_102030"
        )
        print(f"Restore result: {result}")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n" + "=" * 70)
    print("Example completed!")
    print("=" * 70)


if __name__ == "__main__":
    example_usage()