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
        """Compare timestamps of incremental backups from both customer_demo and employee_demo, and recommend the closest matching timestamps using Ollama."""
        try:
            logger.info(f"Getting coordinated backup recommendations for both stanzas")
            
            # Get backup info for both stanzas
            customer_cmd = [
                "pgbackrest",
                "--config=/Users/aarthiprashanth/pgbackrest.conf",
                "--stanza=customer_demo",
                "info"
            ]
            
            employee_cmd = [
                "pgbackrest",
                "--config=/Users/aarthiprashanth/pgbackrest.conf",
                "--stanza=employee_demo",
                "info"
            ]
            
            customer_result = PGBackRestServerTools.run_command(customer_cmd, server_name="customerServer")
            employee_result = PGBackRestServerTools.run_command(employee_cmd, server_name="employeeServer")
            
            if customer_result["status"] != "success" or employee_result["status"] != "success":
                return {
                    "status": "error",
                    "error": "Failed to get backup info from one or both stanzas"
                }
            
            # Parse backup lists for both stanzas
            customer_backups = PGBackRestServerTools._parse_backup_list(customer_result["stdout"], "customerServer")
            employee_backups = PGBackRestServerTools._parse_backup_list(employee_result["stdout"], "employeeServer")
            
            if not customer_backups or not employee_backups:
                return {
                    "status": "error",
                    "error": "No backups found in one or both stanzas"
                }
            
            # Use Ollama to find closest timestamp matches
            recommendations = PGBackRestServerTools._get_coordinated_backup_recommendations_with_ollama(
                customer_backups, employee_backups
            )
            
            return {
                "status": "success",
                "recommendations": recommendations,
                "customer_backups": customer_backups[:5],  # Latest 5 backups
                "employee_backups": employee_backups[:5]   # Latest 5 backups
            }
            
        except Exception as e:
            logger.error(f"Get coordinated backup recommendations error: {e}")
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
                f"--type=time",
                f"--target={target_time}",
                "--delta"
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
    def pitr_restore_with_workflow(stanza: str, pgdata: str, backup_path: str, backup_id: str = None, target_time: str = None, server_name: str = "customerServer") -> Dict[str, Any]:
        """Perform complete PITR restore workflow with PostgreSQL stop/start and port management."""
        try:
            logger.info(f"Starting complete PITR restore workflow for {server_name}: stanza={stanza}")
            
            # Determine the correct port based on server name
            if server_name == "customerServer":
                postgres_port = "5433"
            elif server_name == "employeeServer":
                postgres_port = "5434"
            else:
                postgres_port = "5432"  # Default
            
            workflow_steps = []
            
            # Step 1: Stop PostgreSQL
            workflow_steps.append("Stopping PostgreSQL server...")
            stop_cmd = ["pg_ctl", "stop", "-D", pgdata]
            stop_result = PGBackRestServerTools.run_command(stop_cmd, server_name=server_name)
            
            if stop_result["status"] != "success":
                logger.warning(f"PostgreSQL stop returned non-zero exit code, but this might be expected if already stopped")
                workflow_steps.append("PostgreSQL stop completed (may have been already stopped)")
            else:
                workflow_steps.append("PostgreSQL stopped successfully")
            
            # Step 2: Perform PITR restore
            workflow_steps.append(f"Performing PITR restore...")
            
            if backup_id and not target_time:
                # Standard restore to specific backup
                restore_cmd = [
                    "pgbackrest",
                    "--config=/Users/aarthiprashanth/pgbackrest.conf",
                    f"--stanza={stanza}",
                    f"--pg1-path={pgdata}",
                    f"--repo1-path={backup_path}",
                    "restore",
                    f"--set={backup_id}",
                    "--delta"
                ]
                workflow_steps.append(f"Restoring to backup: {backup_id}")
            elif target_time:
                # PITR restore to specific time
                restore_cmd = [
                    "pgbackrest",
                    "--config=/Users/aarthiprashanth/pgbackrest.conf",
                    f"--stanza={stanza}",
                    f"--pg1-path={pgdata}",
                    f"--repo1-path={backup_path}",
                    "restore",
                    f"--type=time",
                    f"--target={target_time}",
                    "--delta"
                ]
                workflow_steps.append(f"Restoring to time: {target_time}")
            else:
                # Latest backup restore
                restore_cmd = [
                    "pgbackrest",
                    "--config=/Users/aarthiprashanth/pgbackrest.conf",
                    f"--stanza={stanza}",
                    f"--pg1-path={pgdata}",
                    f"--repo1-path={backup_path}",
                    "restore",
                    "--delta"
                ]
                workflow_steps.append("Restoring to latest backup")
            
            restore_result = PGBackRestServerTools.run_command(restore_cmd, server_name=server_name)
            
            if restore_result["status"] != "success":
                workflow_steps.append(f"❌ RESTORE FAILED: {restore_result.get('error', 'Unknown error')}")
                # Try to restart PostgreSQL even if restore failed
                start_cmd = ["pg_ctl", "start", "-D", pgdata, "-o", f"-p {postgres_port}"]
                start_result = PGBackRestServerTools.run_command(start_cmd, server_name=server_name)
                if start_result["status"] == "success":
                    workflow_steps.append("PostgreSQL restarted after failed restore")
                else:
                    workflow_steps.append("❌ CRITICAL: Failed to restart PostgreSQL after failed restore")
                
                return {
                    "status": "error",
                    "error": f"PITR restore failed: {restore_result.get('error', 'Unknown error')}",
                    "workflow_steps": workflow_steps
                }
            
            workflow_steps.append("✅ PITR restore completed successfully")
            
            # Step 3: Start PostgreSQL on correct port
            workflow_steps.append(f"Starting PostgreSQL on port {postgres_port}...")
            start_cmd = ["pg_ctl", "start", "-D", pgdata, "-o", f"-p {postgres_port}"]
            start_result = PGBackRestServerTools.run_command(start_cmd, server_name=server_name)
            
            if start_result["status"] != "success":
                workflow_steps.append(f"❌ Failed to start PostgreSQL: {start_result.get('error', 'Unknown error')}")
                return {
                    "status": "error",
                    "error": f"PostgreSQL startup failed: {start_result.get('error', 'Unknown error')}",
                    "workflow_steps": workflow_steps
                }
            
            workflow_steps.append(f"✅ PostgreSQL started successfully on port {postgres_port}")
            
            # Step 4: Verify database connection
            workflow_steps.append("Verifying database connection...")
            
            return {
                "status": "success",
                "workflow_steps": workflow_steps,
                "server_name": server_name,
                "postgres_port": postgres_port,
                "restore_target": backup_id or target_time or "latest",
                "message": f"PITR restore workflow completed successfully for {server_name}"
            }
            
        except Exception as e:
            logger.error(f"PITR restore workflow error: {e}")
            return {
                "status": "error",
                "error": str(e),
                "workflow_steps": [f"❌ Workflow failed with exception: {str(e)}"]
            }
    
    @staticmethod
    def find_coordinated_backup_recommendations(customer_stanza: str = "customer_demo", employee_stanza: str = "employee_demo") -> Dict[str, Any]:
        """Find coordinated backup recommendations with matching timestamps between customer and employee servers."""
        try:
            logger.info("Finding coordinated backup recommendations for both servers")
            
            # Get backup info for both servers
            customer_result = PGBackRestServerTools.list_backups(customer_stanza, "/Users/aarthiprashanth/pgbackrest-repo", "customerServer")
            employee_result = PGBackRestServerTools.list_backups(employee_stanza, "/Users/aarthiprashanth/pgbackrest-repo", "employeeServer")
            
            if customer_result["status"] != "success" or employee_result["status"] != "success":
                return {
                    "status": "error",
                    "error": "Failed to retrieve backup information from one or both servers"
                }
            
            customer_backups = customer_result.get("backups", [])
            employee_backups = employee_result.get("backups", [])
            
            if not customer_backups or not employee_backups:
                return {
                    "status": "error",
                    "error": "No backups found on one or both servers"
                }
            
            # Find matching backup pairs
            matching_pairs = []
            
            for customer_backup in customer_backups:
                customer_backup_id = customer_backup.get("backup_id", "")
                customer_timestamp = customer_backup.get("timestamp", "")
                
                # Look for employee backups with similar timestamps
                for employee_backup in employee_backups:
                    employee_backup_id = employee_backup.get("backup_id", "")
                    employee_timestamp = employee_backup.get("timestamp", "")
                    
                    # Check if timestamps match closely (within 5 minutes)
                    if customer_timestamp and employee_timestamp:
                        try:
                            from datetime import datetime
                            customer_dt = datetime.strptime(customer_timestamp, "%Y-%m-%d %H:%M:%S")
                            employee_dt = datetime.strptime(employee_timestamp, "%Y-%m-%d %H:%M:%S")
                            
                            time_diff = abs((customer_dt - employee_dt).total_seconds())
                            
                            if time_diff <= 300:  # Within 5 minutes
                                matching_pairs.append({
                                    "customer_backup_id": customer_backup_id,
                                    "employee_backup_id": employee_backup_id,
                                    "customer_timestamp": customer_timestamp,
                                    "employee_timestamp": employee_timestamp,
                                    "time_difference_seconds": time_diff,
                                    "match_quality": "exact" if time_diff == 0 else "close"
                                })
                        except Exception as e:
                            logger.warning(f"Error parsing timestamps: {e}")
                            continue
            
            # Sort by time difference (best matches first)
            matching_pairs.sort(key=lambda x: x["time_difference_seconds"])
            
            if not matching_pairs:
                # If no close matches, find the closest available
                closest_pair = PGBackRestServerTools._find_closest_backup_pair(customer_backups, employee_backups)
                if closest_pair:
                    matching_pairs = [closest_pair]
            
            return {
                "status": "success",
                "coordinated_recommendations": matching_pairs[:3],  # Top 3 matches
                "customer_backups_count": len(customer_backups),
                "employee_backups_count": len(employee_backups),
                "total_matches": len(matching_pairs)
            }
            
        except Exception as e:
            logger.error(f"Error finding coordinated recommendations: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    @staticmethod
    def _find_closest_backup_pair(customer_backups: List[Dict[str, Any]], employee_backups: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Find the closest backup pair when no exact matches exist."""
        try:
            from datetime import datetime
            
            min_diff = float('inf')
            closest_pair = None
            
            for customer_backup in customer_backups:
                customer_backup_id = customer_backup.get("backup_id", "")
                customer_timestamp = customer_backup.get("timestamp", "")
                
                if not customer_timestamp:
                    continue
                
                for employee_backup in employee_backups:
                    employee_backup_id = employee_backup.get("backup_id", "")
                    employee_timestamp = employee_backup.get("timestamp", "")
                    
                    if not employee_timestamp:
                        continue
                    
                    try:
                        customer_dt = datetime.strptime(customer_timestamp, "%Y-%m-%d %H:%M:%S")
                        employee_dt = datetime.strptime(employee_timestamp, "%Y-%m-%d %H:%M:%S")
                        
                        time_diff = abs((customer_dt - employee_dt).total_seconds())
                        
                        if time_diff < min_diff:
                            min_diff = time_diff
                            closest_pair = {
                                "customer_backup_id": customer_backup_id,
                                "employee_backup_id": employee_backup_id,
                                "customer_timestamp": customer_timestamp,
                                "employee_timestamp": employee_timestamp,
                                "time_difference_seconds": time_diff,
                                "match_quality": "approximate"
                            }
                    except Exception as e:
                        logger.warning(f"Error comparing timestamps: {e}")
                        continue
            
            return closest_pair
            
        except Exception as e:
            logger.error(f"Error finding closest pair: {e}")
            return None
    
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
    
    @staticmethod
    def _get_coordinated_backup_recommendations_with_ollama(customer_backups: List[Dict[str, Any]], 
                                                           employee_backups: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Use Ollama to find the closest timestamp matches between customer and employee backups."""
        try:
            import requests
            import json
            from datetime import datetime
            
            # Prepare backup data for Ollama analysis
            customer_data = []
            for backup in customer_backups[:10]:  # Latest 10 backups
                customer_data.append({
                    "label": backup.get("label", ""),
                    "type": backup.get("type", ""),
                    "timestamp": backup.get("timestamp", ""),
                    "server": "customer_demo"
                })
            
            employee_data = []
            for backup in employee_backups[:10]:  # Latest 10 backups
                employee_data.append({
                    "label": backup.get("label", ""),
                    "type": backup.get("type", ""),
                    "timestamp": backup.get("timestamp", ""),
                    "server": "employee_demo"
                })
            
            # Create prompt for Ollama
            prompt = f"""
            You are a PostgreSQL backup expert. I have backup data from two database servers:
            
            Customer Server Backups:
            {json.dumps(customer_data, indent=2)}
            
            Employee Server Backups:
            {json.dumps(employee_data, indent=2)}
            
            Please analyze these backups and find the best matching pairs based on:
            1. Exact timestamp matches (preferred)
            2. Closest timestamps (within minutes)
            3. Same backup type preferences (full with full, incremental with incremental)
            
            Return your recommendation in this JSON format:
            {{
                "best_matches": [
                    {{
                        "customer_backup": "backup_label",
                        "employee_backup": "backup_label",
                        "match_quality": "exact|close|approximate",
                        "time_difference": "time difference description",
                        "recommendation": "why this is a good match"
                    }}
                ],
                "overall_recommendation": "Your overall recommendation for coordinated restore"
            }}
            """
            
            # Call Ollama API
            ollama_response = requests.post(
                "http://localhost:11434/api/generate",
                json={
                    "model": "llama3.2:3b",
                    "prompt": prompt,
                    "stream": False,
                    "format": "json"
                },
                timeout=30
            )
            
            if ollama_response.status_code == 200:
                ollama_result = ollama_response.json()
                try:
                    # Parse Ollama's JSON response
                    recommendation_data = json.loads(ollama_result.get("response", "{}"))
                    
                    return {
                        "status": "success",
                        "matches": recommendation_data.get("best_matches", []),
                        "recommendation": recommendation_data.get("overall_recommendation", "No specific recommendation available"),
                        "analysis_method": "ollama_ai"
                    }
                except json.JSONDecodeError:
                    # Fallback to simple analysis if Ollama response isn't valid JSON
                    logger.warning("Ollama response wasn't valid JSON, falling back to simple analysis")
                    return PGBackRestServerTools._fallback_timestamp_analysis(customer_backups, employee_backups)
            else:
                logger.error(f"Ollama API call failed with status {ollama_response.status_code}")
                return PGBackRestServerTools._fallback_timestamp_analysis(customer_backups, employee_backups)
                
        except Exception as e:
            logger.error(f"Error in Ollama analysis: {e}")
            return PGBackRestServerTools._fallback_timestamp_analysis(customer_backups, employee_backups)
    
    @staticmethod
    def _fallback_timestamp_analysis(customer_backups: List[Dict[str, Any]], 
                                   employee_backups: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Fallback method to find closest timestamp matches without Ollama."""
        try:
            from datetime import datetime
            import re
            
            matches = []
            
            # Find closest matches by comparing timestamps
            for customer_backup in customer_backups[:5]:
                customer_ts = customer_backup.get("timestamp", "")
                customer_label = customer_backup.get("label", "")
                
                if not customer_ts:
                    continue
                
                closest_employee = None
                min_diff = float('inf')
                
                for employee_backup in employee_backups[:5]:
                    employee_ts = employee_backup.get("timestamp", "")
                    employee_label = employee_backup.get("label", "")
                    
                    if not employee_ts:
                        continue
                    
                    try:
                        # Parse timestamps and calculate difference
                        customer_time = datetime.strptime(customer_ts, "%Y-%m-%d %H:%M:%S")
                        employee_time = datetime.strptime(employee_ts, "%Y-%m-%d %H:%M:%S")
                        
                        diff = abs((customer_time - employee_time).total_seconds())
                        
                        if diff < min_diff:
                            min_diff = diff
                            closest_employee = {
                                "label": employee_label,
                                "timestamp": employee_ts,
                                "difference_seconds": diff
                            }
                    except:
                        continue
                
                if closest_employee:
                    match_quality = "exact" if min_diff == 0 else "close" if min_diff < 300 else "approximate"
                    matches.append({
                        "customer_backup": customer_label,
                        "employee_backup": closest_employee["label"],
                        "match_quality": match_quality,
                        "time_difference": f"{int(min_diff)} seconds",
                        "recommendation": f"Timestamps are {match_quality} match"
                    })
            
            return {
                "status": "success",
                "matches": matches[:3],  # Top 3 matches
                "recommendation": "Recommended: Use the closest timestamp matches for coordinated restore",
                "analysis_method": "fallback_timestamp"
            }
            
        except Exception as e:
            logger.error(f"Error in fallback analysis: {e}")
            return {
                "status": "error",
                "matches": [],
                "recommendation": "Unable to analyze backup timestamps",
                "analysis_method": "error"
            }
