#!/usr/bin/env python3
"""
Coordinated Restore Planner for Real PostgreSQL Backup System.
Finds backups across all databases with matching or closest timestamps.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import httpx

logger = logging.getLogger(__name__)

class CoordinatedBackup:
    """Represents a coordinated backup across multiple databases."""
    
    def __init__(self, backup_id: str, database: str, server: str, 
                 backup_type: str, timestamp: str, time_diff: float = 0.0):
        self.backup_id = backup_id
        self.database = database
        self.server = server
        self.backup_type = backup_type
        self.timestamp = timestamp
        self.time_diff = time_diff  # Difference from target time in seconds

class CoordinatedRestorePlanner:
    """Plans coordinated restores across multiple PostgreSQL servers."""
    
    def __init__(self, mcp1_url: str = "http://localhost:8001", 
                 mcp2_url: str = "http://localhost:8002"):
        self.mcp1_url = mcp1_url
        self.mcp2_url = mcp2_url
        self.servers = {
            "MCP1": {"url": mcp1_url, "databases": ["customer_db", "inventory_db", "analytics_db"]},
            "MCP2": {"url": mcp2_url, "databases": ["hr_db", "finance_db", "reporting_db"]}
        }
    
    async def find_coordinated_backups(self, target_timestamp: Optional[str] = None,
                                     max_time_diff: int = 3600) -> Dict[str, Any]:
        """
        Find coordinated backups across all databases.
        
        Args:
            target_timestamp: Target timestamp in ISO format, or None for latest
            max_time_diff: Maximum time difference in seconds for coordination
            
        Returns:
            Dictionary with coordinated backup plan
        """
        if target_timestamp:
            target_time = datetime.fromisoformat(target_timestamp.replace('Z', ''))
        else:
            target_time = datetime.now()
        
        logger.info(f"Finding coordinated backups for target time: {target_time}")
        
        # Collect all backups from both servers
        all_backups = await self._collect_all_backups()
        
        if not all_backups:
            return {
                "error": "No backups found on any server",
                "target_timestamp": target_time.isoformat() + "Z"
            }
        
        # Find the best coordinated set
        coordinated_set = self._find_best_coordinated_set(all_backups, target_time, max_time_diff)
        
        if not coordinated_set:
            return {
                "error": f"No coordinated backups found within {max_time_diff} seconds of target time",
                "target_timestamp": target_time.isoformat() + "Z",
                "available_backups": len(all_backups)
            }
        
        # Create restore plan
        restore_plan = self._create_restore_plan(coordinated_set, target_time)
        
        return restore_plan
    
    async def _collect_all_backups(self) -> List[CoordinatedBackup]:
        """Collect all backups from both MCP servers."""
        all_backups = []
        
        async with httpx.AsyncClient() as client:
            for server_name, server_info in self.servers.items():
                try:
                    for db_name in server_info["databases"]:
                        # Get backups for this database
                        response = await client.post(
                            f"{server_info['url']}/invoke",
                            json={"tool": "list_backups", "arguments": {"db_name": db_name, "limit": 100}},
                            timeout=10.0
                        )
                        
                        if response.status_code == 200:
                            data = response.json()
                            if data.get("ok"):
                                backups = data["result"]["backups"]
                                
                                for backup in backups:
                                    coordinated_backup = CoordinatedBackup(
                                        backup_id=backup["backup_id"],
                                        database=db_name,
                                        server=server_name,
                                        backup_type=backup["backup_type"],
                                        timestamp=backup["completed_at_iso"]
                                    )
                                    all_backups.append(coordinated_backup)
                            else:
                                logger.warning(f"Failed to get backups for {db_name}: {data.get('error')}")
                        else:
                            logger.warning(f"HTTP error getting backups for {db_name}: {response.status_code}")
                            
                except Exception as e:
                    logger.error(f"Error collecting backups from {server_name}: {e}")
        
        logger.info(f"Collected {len(all_backups)} total backups")
        return all_backups
    
    def _find_best_coordinated_set(self, all_backups: List[CoordinatedBackup], 
                                  target_time: datetime, max_time_diff: int) -> List[CoordinatedBackup]:
        """Find the best set of coordinated backups."""
        
        # Group backups by database
        backups_by_db = {}
        for backup in all_backups:
            if backup.database not in backups_by_db:
                backups_by_db[backup.database] = []
            backups_by_db[backup.database].append(backup)
        
        # For each database, find the backup closest to target time
        coordinated_set = []
        total_time_diff = 0
        
        for db_name, db_backups in backups_by_db.items():
            best_backup = None
            best_diff = float('inf')
            
            for backup in db_backups:
                backup_time = datetime.fromisoformat(backup.timestamp.replace('Z', ''))
                time_diff = abs((backup_time - target_time).total_seconds())
                
                if time_diff < best_diff and time_diff <= max_time_diff:
                    best_diff = time_diff
                    best_backup = backup
                    best_backup.time_diff = time_diff
            
            if best_backup:
                coordinated_set.append(best_backup)
                total_time_diff += best_diff
                logger.info(f"Selected {best_backup.backup_id} for {db_name} (diff: {best_diff:.1f}s)")
            else:
                logger.warning(f"No suitable backup found for {db_name} within time limit")
        
        # Sort by time difference (closest first)
        coordinated_set.sort(key=lambda x: x.time_diff)
        
        return coordinated_set
    
    def _create_restore_plan(self, coordinated_set: List[CoordinatedBackup], 
                           target_time: datetime) -> Dict[str, Any]:
        """Create a comprehensive restore plan."""
        
        # Group by server for execution
        by_server = {}
        for backup in coordinated_set:
            if backup.server not in by_server:
                by_server[backup.server] = []
            by_server[backup.server].append(backup)
        
        # Calculate coordination quality
        if coordinated_set:
            max_diff = max(backup.time_diff for backup in coordinated_set)
            avg_diff = sum(backup.time_diff for backup in coordinated_set) / len(coordinated_set)
            
            if max_diff <= 300:  # 5 minutes
                quality = "excellent"
            elif max_diff <= 1800:  # 30 minutes
                quality = "good"
            elif max_diff <= 3600:  # 1 hour
                quality = "fair"
            else:
                quality = "poor"
        else:
            quality = "none"
            max_diff = 0
            avg_diff = 0
        
        # Create detailed plan
        plan = {
            "target_timestamp": target_time.isoformat() + "Z",
            "coordination_quality": quality,
            "max_time_difference_seconds": max_diff,
            "average_time_difference_seconds": avg_diff,
            "total_databases": len(coordinated_set),
            "coordinated_backups": [],
            "execution_plan": {},
            "summary": self._create_summary(coordinated_set, target_time, quality),
            "warnings": self._create_warnings(coordinated_set)
        }
        
        # Add backup details
        for backup in coordinated_set:
            plan["coordinated_backups"].append({
                "database": backup.database,
                "server": backup.server,
                "backup_id": backup.backup_id,
                "backup_type": backup.backup_type,
                "timestamp": backup.timestamp,
                "time_difference_seconds": backup.time_diff,
                "time_difference_human": self._format_time_diff(backup.time_diff)
            })
        
        # Add execution plan
        for server, backups in by_server.items():
            plan["execution_plan"][server] = {
                "server_url": self.servers[server]["url"],
                "restore_operations": [
                    {
                        "database": backup.database,
                        "backup_id": backup.backup_id,
                        "method": "pg_restore" if backup.backup_type == "full" else "WAL replay"
                    }
                    for backup in backups
                ]
            }
        
        return plan
    
    def _create_summary(self, coordinated_set: List[CoordinatedBackup], 
                       target_time: datetime, quality: str) -> List[str]:
        """Create human-readable summary."""
        summary = []
        
        if not coordinated_set:
            summary.append("❌ No coordinated backups found")
            return summary
        
        summary.append(f"🎯 Coordinated restore plan for {len(coordinated_set)} databases")
        summary.append(f"📅 Target time: {target_time.strftime('%Y-%m-%d %H:%M:%S')}")
        summary.append(f"⭐ Coordination quality: {quality.upper()}")
        
        # Group by server
        by_server = {}
        for backup in coordinated_set:
            if backup.server not in by_server:
                by_server[backup.server] = []
            by_server[backup.server].append(backup)
        
        for server, backups in by_server.items():
            summary.append(f"\n🖥️  {server}:")
            for backup in backups:
                time_str = self._format_time_diff(backup.time_diff)
                summary.append(f"   📁 {backup.database}: {backup.backup_id} ({time_str})")
        
        return summary
    
    def _create_warnings(self, coordinated_set: List[CoordinatedBackup]) -> List[str]:
        """Create warnings for the restore plan."""
        warnings = []
        
        if not coordinated_set:
            warnings.append("No backups available for restore")
            return warnings
        
        # Check for large time differences
        max_diff = max(backup.time_diff for backup in coordinated_set) if coordinated_set else 0
        if max_diff > 3600:  # 1 hour
            warnings.append(f"Large time differences detected (up to {max_diff/3600:.1f} hours)")
        
        # Check for mixed backup types
        backup_types = set(backup.backup_type for backup in coordinated_set)
        if len(backup_types) > 1:
            warnings.append("Mixed backup types (full and incremental) - restore order matters")
        
        # Check for missing databases
        all_databases = set()
        for server_info in self.servers.values():
            all_databases.update(server_info["databases"])
        
        restored_databases = set(backup.database for backup in coordinated_set)
        missing_databases = all_databases - restored_databases
        
        if missing_databases:
            warnings.append(f"Some databases have no suitable backups: {', '.join(missing_databases)}")
        
        return warnings
    
    def _format_time_diff(self, seconds: float) -> str:
        """Format time difference in human-readable format."""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds/60:.1f}m"
        else:
            return f"{seconds/3600:.1f}h"
    
    async def execute_coordinated_restore(self, restore_plan: Dict[str, Any], 
                                        confirm: bool = False) -> Dict[str, Any]:
        """Execute the coordinated restore plan."""
        
        if not confirm:
            return {
                "status": "confirmation_required",
                "message": "Coordinated restore requires confirmation",
                "plan": restore_plan
            }
        
        logger.info("Executing coordinated restore...")
        
        results = {}
        errors = []
        
        async with httpx.AsyncClient() as client:
            for server, execution_info in restore_plan["execution_plan"].items():
                server_results = []
                
                for operation in execution_info["restore_operations"]:
                    try:
                        response = await client.post(
                            f"{execution_info['server_url']}/invoke",
                            json={
                                "tool": "restore_database",
                                "arguments": {
                                    "db_name": operation["database"],
                                    "backup_id": operation["backup_id"]
                                }
                            },
                            timeout=60.0
                        )
                        
                        if response.status_code == 200:
                            data = response.json()
                            if data.get("ok"):
                                server_results.append({
                                    "database": operation["database"],
                                    "status": "success",
                                    "result": data["result"]
                                })
                                logger.info(f"✅ Restored {operation['database']} on {server}")
                            else:
                                error_msg = data.get("error", "Unknown error")
                                server_results.append({
                                    "database": operation["database"],
                                    "status": "error",
                                    "error": error_msg
                                })
                                errors.append(f"{server}/{operation['database']}: {error_msg}")
                        else:
                            error_msg = f"HTTP {response.status_code}"
                            server_results.append({
                                "database": operation["database"],
                                "status": "error",
                                "error": error_msg
                            })
                            errors.append(f"{server}/{operation['database']}: {error_msg}")
                            
                    except Exception as e:
                        error_msg = str(e)
                        server_results.append({
                            "database": operation["database"],
                            "status": "error",
                            "error": error_msg
                        })
                        errors.append(f"{server}/{operation['database']}: {error_msg}")
                
                results[server] = server_results
        
        return {
            "status": "completed" if not errors else "partial_success",
            "results": results,
            "errors": errors,
            "total_operations": sum(len(info["restore_operations"]) 
                                  for info in restore_plan["execution_plan"].values()),
            "successful_operations": sum(
                len([r for r in server_results if r["status"] == "success"])
                for server_results in results.values()
            ),
            "timestamp": datetime.now().isoformat() + "Z"
        }

async def demo_coordinated_restore():
    """Demonstrate coordinated restore planning."""
    planner = CoordinatedRestorePlanner()
    
    print("🎯 COORDINATED RESTORE DEMONSTRATION")
    print("=" * 50)
    
    # Find latest coordinated backups
    print("1. Finding latest coordinated backups...")
    plan = await planner.find_coordinated_backups()
    
    print("\n📋 Restore Plan:")
    if "error" not in plan:
        for line in plan["summary"]:
            print(f"   {line}")
        
        if plan["warnings"]:
            print("\n⚠️  Warnings:")
            for warning in plan["warnings"]:
                print(f"   - {warning}")
        
        print(f"\n📊 Plan Details:")
        print(f"   - Coordination Quality: {plan['coordination_quality']}")
        print(f"   - Max Time Difference: {plan['max_time_difference_seconds']:.1f}s")
        print(f"   - Databases: {plan['total_databases']}")
        
        # Show execution plan
        print(f"\n🔧 Execution Plan:")
        for server, exec_info in plan["execution_plan"].items():
            print(f"   {server}:")
            for op in exec_info["restore_operations"]:
                print(f"     - {op['database']}: {op['backup_id']} ({op['method']})")
    else:
        print(f"   ❌ {plan['error']}")

if __name__ == "__main__":
    asyncio.run(demo_coordinated_restore())
