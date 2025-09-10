import subprocess
import sys
import os
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import re

logger = logging.getLogger(__name__)

class PGBackRestServerTools:
    """Server-side pgBackRest tools that execute actual commands with shared repository."""
    
    @staticmethod
    def run_command(cmd: List[str], env: Dict[str, str] = None, server_name: str = "customerServer") -> Dict[str, Any]:
        """Run a shell command and return structured output."""
        try:
            logger.info(f"Running command for {server_name}: {' '.join(cmd)}")
            
            # Set up environment for shared repository with config file
            if env is None:
                import os
                env = os.environ.copy()
                # Use the shared config file
                env["PGBACKREST_CONFIG"] = "/Users/aarthiprashanth/pgbackrest.conf"
            else:
                # Merge with default environment
                import os
                default_env = os.environ.copy()
                default_env.update(env)
                env = default_env
            
            result = subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)
            
            return {
                "status": "success",
                "stdout": result.stdout,
                "stderr": result.stderr,
                "command": ' '.join(cmd)
            }
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed with return code {e.returncode}: {e.stderr}")
            return {
                "status": "error",
                "error": f"Command failed with return code {e.returncode}",
                "stdout": e.stdout,
                "stderr": e.stderr,
                "command": ' '.join(cmd)
            }
        except Exception as e:
            logger.error(f"Command execution error: {e}")
            return {
                "status": "error",
                "error": str(e),
                "command": ' '.join(cmd)
            }
    
    @staticmethod
    def full_backup(stanza: str, backup_path: str, server_name: str = "customerServer") -> Dict[str, Any]:
        """Create a full backup using pgBackRest."""
        try:
            logger.info(f"Creating full backup for stanza: {stanza}")
            
            cmd = [
                "pgbackrest",
                "--config=/Users/aarthiprashanth/pgbackrest.conf",
                f"--stanza={stanza}",
                "--type=full",
                "backup"
            ]
            
            result = PGBackRestServerTools.run_command(cmd, server_name=server_name)
            
            if result["status"] == "success":
                # Parse backup info from stdout
                backup_info = PGBackRestServerTools._parse_backup_info(result["stdout"], server_name)
                result["backup_info"] = backup_info
                logger.info(f"Full backup completed successfully for {stanza}")
            else:
                logger.error(f"Full backup failed for {stanza}: {result.get('error', 'Unknown error')}")
            
            return result
            
        except Exception as e:
            logger.error(f"Full backup error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    @staticmethod
    def incremental_backup(stanza: str, backup_path: str, server_name: str = "customerServer") -> Dict[str, Any]:
        """Create an incremental backup using pgBackRest."""
        try:
            logger.info(f"Creating incremental backup for stanza: {stanza}")
            
            cmd = [
                "pgbackrest",
                "--config=/Users/aarthiprashanth/pgbackrest.conf",
                f"--stanza={stanza}",
                "--type=incr",
                "backup"
            ]
            
            result = PGBackRestServerTools.run_command(cmd, server_name=server_name)
            
            if result["status"] == "success":
                # Parse backup info from stdout
                backup_info = PGBackRestServerTools._parse_backup_info(result["stdout"], server_name)
                result["backup_info"] = backup_info
                logger.info(f"Incremental backup completed successfully for {stanza}")
            else:
                logger.error(f"Incremental backup failed for {stanza}: {result.get('error', 'Unknown error')}")
            
            return result
            
        except Exception as e:
            logger.error(f"Incremental backup error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    @staticmethod
    def restore(stanza: str, pgdata: str, backup_path: str, backup_type: str, server_name: str = "customerServer") -> Dict[str, Any]:
        """Restore from a pgBackRest backup."""
        try:
            logger.info(f"Restoring from backup for stanza: {stanza}, backup: {backup_type}")
            
            cmd = [
                "pgbackrest",
                "--config=/Users/aarthiprashanth/pgbackrest.conf",
                f"--stanza={stanza}",
                f"--pg1-path={pgdata}",
                f"--repo1-path={backup_path}",
                "restore",
                f"--set={backup_type}"
            ]
            
            result = PGBackRestServerTools.run_command(cmd, server_name=server_name)
            
            if result["status"] == "success":
                logger.info(f"Restore completed successfully for {stanza}")
            else:
                logger.error(f"Restore failed for {stanza}: {result.get('error', 'Unknown error')}")
            
            return result
            
        except Exception as e:
            logger.error(f"Restore error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    @staticmethod
    def list_backups(stanza: str, backup_path: str, server_name: str = "customerServer") -> Dict[str, Any]:
        """List available pgBackRest backups."""
        try:
            logger.info(f"Listing backups for stanza: {stanza}")
            
            cmd = [
                "pgbackrest",
                "--config=/Users/aarthiprashanth/pgbackrest.conf",
                f"--stanza={stanza}",
                "info"
            ]
            
            result = PGBackRestServerTools.run_command(cmd, server_name=server_name)
            
            if result["status"] == "success":
                # Parse backup list from stdout
                backup_list = PGBackRestServerTools._parse_backup_list(result["stdout"], server_name)
                result["backups"] = backup_list
                logger.info(f"Found {len(backup_list)} backups for {stanza}")
            else:
                logger.error(f"List backups failed for {stanza}: {result.get('error', 'Unknown error')}")
            
            return result
            
        except Exception as e:
            logger.error(f"List backups error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    @staticmethod
    def get_info(stanza: str, backup_path: str, server_name: str = "customerServer") -> Dict[str, Any]:
        """Get pgBackRest information and status."""
        try:
            logger.info(f"Getting info for stanza: {stanza}")
            
            cmd = [
                "pgbackrest",
                "--config=/Users/aarthiprashanth/pgbackrest.conf",
                f"--stanza={stanza}",
                "info"
            ]
            
            result = PGBackRestServerTools.run_command(cmd, server_name=server_name)
            
            if result["status"] == "success":
                logger.info(f"Info retrieved successfully for {stanza}")
            else:
                logger.error(f"Get info failed for {stanza}: {result.get('error', 'Unknown error')}")
            
            return result
            
        except Exception as e:
            logger.error(f"Get info error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    @staticmethod
    def get_recommended_backups(stanza: str, backup_path: str, server_name: str = "customerServer") -> Dict[str, Any]:
        """Get the 3 most recent incremental backups with consistent timestamps and provide recommendation."""
        try:
            logger.info(f"Getting recommended backups for stanza: {stanza}")
            
            cmd = [
                "pgbackrest",
                "--config=/Users/aarthiprashanth/pgbackrest.conf",
                f"--stanza={stanza}",
                "info"
            ]
            
            result = PGBackRestServerTools.run_command(cmd, server_name=server_name)
            
            if result["status"] == "success":
                # Parse backup list and get recommendations
                backup_list = PGBackRestServerTools._parse_backup_list(result["stdout"], server_name)
                recommendations = PGBackRestServerTools._get_backup_recommendations(backup_list)
                result["recommendations"] = recommendations
                logger.info(f"Generated recommendations for {stanza}")
            else:
                logger.error(f"Get recommended backups failed for {stanza}: {result.get('error', 'Unknown error')}")
            
            return result
            
        except Exception as e:
            logger.error(f"Get recommended backups error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    @staticmethod
    def pitr_restore(stanza: str, pgdata: str, backup_path: str, target_time: str, server_name: str = "customerServer") -> Dict[str, Any]:
        """Perform Point-in-Time Recovery (PITR) restore to a specific target time."""
        try:
            logger.info(f"Performing PITR restore for stanza: {stanza}, target time: {target_time}")
            
            cmd = [
                "pgbackrest",
                "--config=/Users/aarthiprashanth/pgbackrest.conf",
                f"--stanza={stanza}",
                f"--pg1-path={pgdata}",
                f"--repo1-path={backup_path}",
                "restore",
                f"--target-time={target_time}"
            ]
            
            result = PGBackRestServerTools.run_command(cmd, server_name=server_name)
            
            if result["status"] == "success":
                logger.info(f"PITR restore completed successfully for {stanza}")
            else:
                logger.error(f"PITR restore failed for {stanza}: {result.get('error', 'Unknown error')}")
            
            return result
            
        except Exception as e:
            logger.error(f"PITR restore error: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    @staticmethod
    def _parse_backup_info(stdout: str, server_name: str) -> Dict[str, Any]:
        """Parse backup information from pgBackRest output."""
        try:
            # Extract backup size and other info from stdout
            size_match = re.search(r'backup size = ([\d.]+MB)', stdout)
            files_match = re.search(r'file total = (\d+)', stdout)
            label_match = re.search(r'new backup label = ([^\s]+)', stdout)
            
            backup_info = {
                "server_name": server_name,
                "size": size_match.group(1) if size_match else "Unknown",
                "file_count": int(files_match.group(1)) if files_match else 0,
                "backup_id": label_match.group(1) if label_match else "Unknown"
            }
            
            return backup_info
            
        except Exception as e:
            logger.error(f"Error parsing backup info: {e}")
            return {
                "server_name": server_name,
                "size": "Unknown",
                "file_count": 0,
                "backup_id": "Unknown"
            }
    
    @staticmethod
    def _parse_backup_list(stdout: str, server_name: str) -> List[Dict[str, Any]]:
        """Parse backup list from pgBackRest info output."""
        try:
            backups = []
            lines = stdout.split('\n')
            
            for line in lines:
                # Look for backup lines like "full backup: 20250908-121412F"
                if 'backup:' in line and ('full' in line or 'incr' in line):
                    parts = line.strip().split(':')
                    if len(parts) >= 2:
                        backup_type = parts[0].strip()
                        backup_id = parts[1].strip()
                        
                        backups.append({
                            "server_name": server_name,
                            "backup_id": backup_id,
                            "type": backup_type,
                            "timestamp": PGBackRestServerTools._extract_timestamp(backup_id)
                        })
            
            return backups
            
        except Exception as e:
            logger.error(f"Error parsing backup list: {e}")
            return []
    
    @staticmethod
    def _extract_timestamp(backup_id: str) -> str:
        """Extract timestamp from backup ID."""
        try:
            # Backup ID format: 20250908-121412F or 20250908-121412F_20250908-121413I
            if '_' in backup_id:
                timestamp_part = backup_id.split('_')[0]
            else:
                timestamp_part = backup_id
            
            # Convert 20250908-121412 to 2025-09-08 12:14:12
            if len(timestamp_part) >= 13:
                date_part = timestamp_part[:8]
                time_part = timestamp_part[9:15]
                
                year = date_part[:4]
                month = date_part[4:6]
                day = date_part[6:8]
                hour = time_part[:2]
                minute = time_part[2:4]
                second = time_part[4:6]
                
                return f"{year}-{month}-{day} {hour}:{minute}:{second}"
            
            return backup_id
            
        except Exception as e:
            logger.error(f"Error extracting timestamp: {e}")
            return backup_id
    
    @staticmethod
    def _get_backup_recommendations(backup_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get backup recommendations based on available backups."""
        try:
            if not backup_list:
                return {
                    "recommendation": "No backups available",
                    "backups": []
                }
            
            # Sort by timestamp (newest first)
            sorted_backups = sorted(backup_list, key=lambda x: x.get('timestamp', ''), reverse=True)
            
            # Get the 3 most recent backups
            recent_backups = sorted_backups[:3]
            
            # Check if we have both full and incremental backups
            has_full = any(b.get('type', '').startswith('full') for b in recent_backups)
            has_incr = any(b.get('type', '').startswith('incr') for b in recent_backups)
            
            if has_full and has_incr:
                recommendation = "Recommended: Use the most recent full backup with incremental backups for optimal restore"
            elif has_full:
                recommendation = "Recommended: Use the most recent full backup"
            else:
                recommendation = "Warning: Only incremental backups available, may need full backup for complete restore"
            
            return {
                "recommendation": recommendation,
                "backups": recent_backups,
                "total_backups": len(backup_list)
            }
            
        except Exception as e:
            logger.error(f"Error generating recommendations: {e}")
            return {
                "recommendation": "Error generating recommendations",
                "backups": [],
                "total_backups": 0
            }
