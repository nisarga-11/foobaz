#!/usr/bin/env python3
"""
Natural Language Interface for PostgreSQL MCP Backup System using Ollama.
Enhanced with auto-restore and improved multi-server support.
"""

import sys
import json
import os
from datetime import datetime, timedelta
from fastapi_client import MCPBackupClient 
from typing import List, Dict, Optional, Tuple
import asyncio
import time

try:
    import ollama
except ImportError:
    print("Error: ollama package not found. Install it with: pip install ollama")
    sys.exit(1)


# ============================================================================
# Audit Logger
# ============================================================================

class AuditLogger:
    """Logs all commands for security and debugging."""
    
    def __init__(self, log_file="backup_audit.log"):
        self.log_file = log_file
        
    def log_command(self, user_input: str, command: Dict, success: bool, result: str):
        """Log a command execution."""
        timestamp = datetime.now().isoformat()
        log_entry = {
            "timestamp": timestamp,
            "user_input": user_input,
            "parsed_command": command,
            "success": success,
            "result_preview": result[:200] if result else None
        }
        
        try:
            with open(self.log_file, 'a') as f:
                f.write(json.dumps(log_entry) + "\n")
        except Exception as e:
            print(f"⚠️ Failed to write audit log: {e}")
    
    def show_recent_logs(self, count=10):
        """Show recent command history."""
        try:
            with open(self.log_file, 'r') as f:
                lines = f.readlines()
                recent = lines[-count:] if len(lines) > count else lines
                
                print("\n📜 Recent Command History:\n")
                for line in recent:
                    try:
                        entry = json.loads(line)
                        print(f"[{entry['timestamp']}]")
                        print(f"  Input: {entry['user_input']}")
                        print(f"  Operation: {entry['parsed_command'].get('operation')}")
                        print(f"  Success: {'✅' if entry['success'] else '❌'}")
                        print()
                    except:
                        continue
                        
        except FileNotFoundError:
            print("📜 No audit log found yet.")
        except Exception as e:
            print(f"❌ Error reading logs: {e}")


# ============================================================================
# Core Assistant Logic
# ============================================================================

class BackupAssistant:
    """Natural language assistant for backup operations."""
    
    def __init__(self, client, model="llama3.1"):
        self.client = client
        self.model = model
        self.conversation_history = []
        self.audit_logger = AuditLogger()
        
        # Dynamically discovered structure
        self.server_databases = {}
        self.database_to_servers = {}
        
        # Safety settings
        self.require_confirmation = True
        self.show_preview = True
        
        # Discover system structure
        self._discover_system()
        
    def _get_databases_for_server(self, server_name: str) -> List[str]:
        """Synchronous wrapper: Calls the FastAPI API to get the list of databases."""
        try:
            response = self.client.list_server_databases(server_name)
            return response.get('databases', [])
        except Exception as e:
            print(f"  ⚠️ Failed to list databases for {server_name}: {e}")
            return ['postgres'] 
        
    def _discover_system(self):
        """
        Dynamically discover all servers and their databases.
        Uses asyncio.run_in_executor to execute sync HTTP calls concurrently.
        """
        print("🔍 Discovering system structure...")
        
        # Clear previous state
        self.server_databases = {}
        self.database_to_servers = {}
        
        try:
            servers_response = self.client.list_servers()
            servers = servers_response.get('servers', [])
            
            if not servers:
                print("  ⚠️ No servers returned from API")
                print("  💡 Make sure FastAPI server is running on http://localhost:8000")
                return
            
            print(f"  📡 Found {len(servers)} server(s) in configuration")
            
            async def fetch_all_databases():
                tasks = []
                connected_servers = [s for s in servers if s.get('connected')]
                disconnected_servers = [s for s in servers if not s.get('connected')]
                
                if disconnected_servers:
                    print(f"  ⚠️ {len(disconnected_servers)} server(s) disconnected:")
                    for s in disconnected_servers:
                        print(f"     • {s['name']}: Not connected")
                
                if not connected_servers:
                    print("  ❌ No connected servers found!")
                    return
                
                print(f"  ✓ {len(connected_servers)} server(s) connected")
                
                for server_info in connected_servers:
                    server_name = server_info['name']
                    tasks.append(
                        asyncio.get_event_loop().run_in_executor(
                            None, self._get_databases_for_server, server_name
                        )
                    )
                
                # Handle disconnected servers early
                for server_info in disconnected_servers:
                    self.server_databases[server_info['name']] = []

                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results
                server_names_connected = [s['name'] for s in connected_servers]
                for server_name, databases_result in zip(server_names_connected, results):
                    if isinstance(databases_result, list):
                        databases = set(databases_result)
                        self.server_databases[server_name] = sorted(list(databases))
                        
                        # Build reverse mapping
                        for db in databases:
                            if db not in self.database_to_servers:
                                self.database_to_servers[db] = []
                            if server_name not in self.database_to_servers[db]:
                                self.database_to_servers[db].append(server_name)
                        
                        print(f"  ✅ {server_name}: {len(databases)} database(s) found")
                    elif isinstance(databases_result, Exception):
                         print(f"  ❌ {server_name}: Error during database fetch: {databases_result}")

            # Run the async function using a suitable event loop
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
            loop.run_until_complete(fetch_all_databases())
            
            if self.server_databases:
                print(f"\n📊 Discovery complete: {len(self.server_databases)} server(s) ready")
            else:
                print(f"\n⚠️ Discovery complete but no servers available")
            
        except Exception as e:
            print(f"❌ Failed to discover system: {e}")
            print("💡 Troubleshooting:")
            print("   1. Is the FastAPI server running? (python fastapi_server.py)")
            print("   2. Is it accessible at http://localhost:8000?")
            print("   3. Try: curl http://localhost:8000/servers")
            print("Continuing with empty structure...")
            
    def _refresh_database_list(self):
        """Refreshes the database list by re-running discovery."""
        self._discover_system()
        
    def _parse_timestamp(self, time_str: str) -> str:
        """
        Parse various timestamp formats into ISO8601.
        NO REGEX - uses string methods only.
        """
        if not time_str or time_str.lower() in ['now', 'recent', 'latest']:
            return datetime.now().isoformat()
        
        # Clean up the string
        time_str = time_str.strip()
        
        # Try to handle common formats using string splitting
        # Format: "2024-12-08 10:20" or "2024-12-08 10:20:30"
        if ' ' in time_str and '-' in time_str:
            parts = time_str.split(' ')
            if len(parts) == 2:
                date_part = parts[0]
                time_part = parts[1]
                
                # Add seconds if missing
                if time_part.count(':') == 1:
                    time_part = time_part + ':00'
                
                return f"{date_part}T{time_part}"
        
        # Format: "2024-12-08" (add midnight)
        if '-' in time_str and ':' not in time_str and len(time_str) == 10:
            return f"{time_str}T00:00:00"
        
        # Try parsing as-is
        try:
            dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            return dt.isoformat()
        except ValueError:
            print(f"⚠️ Warning: Could not parse timestamp '{time_str}', using 'now'")
            return datetime.now().isoformat()
    
    def get_system_prompt(self):
        """Generate system prompt dynamically based on discovered structure."""
        
        server_info = []
        for server, databases in self.server_databases.items():
            db_list = ', '.join(databases)
            server_info.append(f"  * {server}: {db_list}")
        
        servers_str = '\n'.join(server_info) if server_info else "  (No servers discovered)"
        
        all_servers = list(self.server_databases.keys())
        servers_list = ', '.join([f'"{s}"' for s in all_servers]) if all_servers else ''
        
        return f"""You are a helpful assistant for a PostgreSQL backup system. 
Your job is to understand user requests and convert them into structured JSON commands.

DISCOVERED SYSTEM STRUCTURE:
{servers_str}

CRITICAL PARSING RULES:
1. BACKUP TYPE: Default to 'full' unless user explicitly says 'incremental' or 'diff'
   - "backup pg2" → type: "full"
   - "incremental backup of pg2" → type: "incremental"
   - "do incremental backup" → type: "incremental"

2. TIMESTAMPS: Convert to ISO8601 format (YYYY-MM-DDTHH:MM:SS)
   - "at 2024-12-08 10:20" → "2024-12-08T10:20:00"
   - "now", "recent", "latest" → "now"
   - "2024-12-08" → "2024-12-08T00:00:00"

3. AUTO-RESTORE (NEW):
   - "restore [database]" → auto_restore with target_time: "now"
   - "restore [database] now" → auto_restore with target_time: "now"
   - "restore [database] to [time]" → auto_restore with parsed timestamp
   - This automatically finds best backup and executes restore

4. RESTORE OPERATIONS:
   - "show restore point" → recommend_restore with target_time: "now"
   - "find restore point at [time]" → recommend_restore with parsed timestamp
   - "execute restore [db] on [server] with ID [id]" → automated_restore

5. MULTI-SERVER OPERATIONS:
   - "backup [db] on all servers" → multi_server_backup
   - "backup [db] on both servers" → multi_server_backup
   - If servers list is empty, use all available servers

Available operations:
1. list_servers - List all available servers
2. list_backups - List backups for a database (requires: server, database)
3. trigger_backup - Create a backup (requires: server, database, type: full|incremental)
4. multi_server_backup - Backup across multiple servers (requires: database, optional: servers)
5. recommend_restore - Find best restore point (requires: database, target_time, optional: servers)
6. auto_restore - Automatically find and execute restore (requires: database, target_time, optional: servers)
7. automated_restore - Execute restore with specific backup (requires: server, database, backup_id)
8. enable_schedules - Enable automated schedules
9. health_check - Check system health
10. help - Show help
11. show_logs - Show recent command history

RESPONSE FORMAT (JSON only):
{{
    "operation": "operation_name",
    "parameters": {{
        "server": "server_name",      
        "database": "db_name",         
        "type": "full",                
        "target_time": "ISO8601|now",  
        "backup_id": "id",             
        "servers": ["s1", "s2"]        
    }},
    "confidence": 0.95
}}

Available servers: {servers_list}

For ambiguous requests, set confidence < 0.7 and include "clarification" field.
"""

    def parse_user_input(self, user_input: str) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Use Ollama to parse natural language into structured commands.
        NO REGEX - relies entirely on LLM understanding.
        """
        
        self.conversation_history.append({
            "role": "user",
            "content": user_input
        })
        
        try:
            response = ollama.chat(
                model=self.model,
                messages=[
                    {"role": "system", "content": self.get_system_prompt()},
                    {"role": "user", "content": user_input}
                ],
                options={
                    "temperature": 0.1,
                    "num_predict": 300,
                }
            )
            
            assistant_message = response['message']['content']
            
            self.conversation_history.append({
                "role": "assistant",
                "content": assistant_message
            })
            
            # Extract JSON - find first { and last }
            start_idx = assistant_message.find('{')
            end_idx = assistant_message.rfind('}')
            
            if start_idx == -1 or end_idx == -1:
                return None, "Could not parse response into a command"
            
            json_str = assistant_message[start_idx:end_idx+1]
            command = json.loads(json_str)
            
            # Post-process: Ensure proper formatting
            command = self._post_process_command(command)
            
            return command, None
            
        except Exception as e:
            return None, f"Error parsing input: {str(e)}"

    def _post_process_command(self, command: Dict) -> Dict:
        """
        Post-process parsed command to ensure all parameters are correctly formatted.
        """
        if not command or 'parameters' not in command:
            return command
        
        params = command['parameters']
        
        # Ensure backup type defaults to 'full'
        if command.get('operation') == 'trigger_backup':
            if 'type' not in params or not params['type']:
                params['type'] = 'full'
        
        # Parse timestamps
        if 'target_time' in params:
            params['target_time'] = self._parse_timestamp(params['target_time'])
        
        # Handle multi-server operations - if servers is empty, use all
        if command.get('operation') == 'multi_server_backup':
            if 'servers' not in params or not params['servers']:
                params['servers'] = list(self.server_databases.keys())
        
        # Infer missing server/database from context if possible
        if command.get('operation') in ['trigger_backup', 'list_backups']:
            if 'server' not in params or not params['server']:
                if len(self.server_databases) == 1:
                    params['server'] = list(self.server_databases.keys())[0]
            
            if 'database' not in params or not params['database']:
                server = params.get('server')
                if server and server in self.server_databases:
                    if self.server_databases[server]:
                        params['database'] = self.server_databases[server][0]
        
        return command
    
    def _get_human_confirmation(self, operation: str, params: Dict) -> bool:
        """
        Ask for human confirmation before dangerous operations.
        """
        print("\n" + "="*70)
        print("⚠️  CONFIRMATION REQUIRED")
        print("="*70)
        print(f"Operation: {operation}")
        print(f"Parameters:")
        for key, value in params.items():
            if value:
                print(f"  • {key}: {value}")
        print("="*70)
        
        while True:
            response = input("\n🔴 Type 'YES' to confirm, 'NO' to cancel: ").strip().upper()
            if response == 'YES':
                return True
            elif response == 'NO':
                return False
            else:
                print("⚠️ Please type 'YES' or 'NO'")
    
    def _show_restore_preview(self, server: str, database: str, backup_id: str) -> bool:
        """
        Show a preview of what will happen during restore.
        Returns True if user wants to proceed.
        """
        print("\n" + "="*70)
        print("👁️  RESTORE PREVIEW")
        print("="*70)
        
        try:
            # Get backup details
            backups = self.client.list_backups(server, database, limit=50)
            backup_info = None
            
            for backup in backups.get('backups', []):
                if backup.get('id') == backup_id:
                    backup_info = backup
                    break
            
            if backup_info:
                print(f"\n📦 Backup Information:")
                print(f"  • ID: {backup_info.get('id')}")
                print(f"  • Type: {backup_info.get('type')}")
                print(f"  • Created: {backup_info.get('timestamp')}")
                print(f"  • Size: {backup_info.get('size_bytes', 0):,} bytes")
                print(f"  • Status: {backup_info.get('status')}")
            else:
                print(f"\n⚠️ Backup ID '{backup_id}' not found in recent backups")
            
            print(f"\n🎯 Target:")
            print(f"  • Server: {server}")
            print(f"  • Database: {database}")
            
            print(f"\n⚠️  WARNING:")
            print(f"  • Current database will be OVERWRITTEN")
            print(f"  • All current data will be LOST")
            print(f"  • PostgreSQL will be STOPPED during restore")
            print(f"  • This operation CANNOT be undone")
            
            print("\n" + "="*70)
            
            return True
            
        except Exception as e:
            print(f"\n❌ Error fetching preview: {e}")
            return False
    
    def _start_rollback_timer(self, duration_seconds: int = 300):
        """
        Start a rollback timer for dangerous operations.
        User can cancel during this window.
        """
        print(f"\n⏲️  Rollback timer: {duration_seconds} seconds")
        print(f"   Press Ctrl+C within {duration_seconds}s to CANCEL the operation")
        
        try:
            for remaining in range(duration_seconds, 0, -1):
                if remaining % 10 == 0 or remaining <= 5:
                    print(f"   ⏳ {remaining} seconds remaining...", end='\r')
                time.sleep(1)
            print("\n   ✅ Timer expired, proceeding with operation...")
            return True
        except KeyboardInterrupt:
            print("\n\n   🛑 Operation CANCELLED by user")
            return False
    
    def execute_command(self, command, user_input: str):
        """Execute the parsed command with safety checks."""
        operation = command.get('operation')
        params = command.get('parameters', {})
        confidence = command.get('confidence', 0.0)
        
        # Check if clarification is needed
        if confidence < 0.7 and 'clarification' in command:
            clarification = command.get('clarification', 'The request is unclear')
            available_info = self._format_available_resources()
            result = f"⚠️ {clarification}\n\n{available_info}"
            self.audit_logger.log_command(user_input, command, False, result)
            return False, result
        
        # Validate parameters
        if operation in ["trigger_backup", "list_backups", "enable_schedules", "automated_restore"]:
            server = params.get('server')
            database = params.get('database')
            
            if server and server not in self.server_databases:
                # Try case-insensitive match
                server_lower = server.lower()
                matched_server = None
                for known_server in self.server_databases.keys():
                    if known_server.lower() == server_lower:
                        matched_server = known_server
                        params['server'] = matched_server  # Fix the case
                        server = matched_server
                        print(f"   ℹ️ Auto-corrected server name: '{server}'")
                        break
                
                if not matched_server:
                    available_servers = ', '.join(self.server_databases.keys()) if self.server_databases else 'None'
                    result = f"❌ Server '{server}' is not a known or connected server.\n"
                    result += f"   Available servers: {available_servers}\n"
                    result += f"\n💡 Tips:\n"
                    result += f"   • Check if FastAPI server is running (http://localhost:8000)\n"
                    result += f"   • Type 'refresh' to rediscover servers\n"
                    result += f"   • Type 'list servers' to see current status"
                    self.audit_logger.log_command(user_input, command, False, result)
                    return False, result

            if server and database:
                if server in self.server_databases:
                    if database not in self.server_databases[server]:
                        available = ', '.join(self.server_databases[server]) or 'None'
                        result = f"❌ Database '{database}' does not exist on {server}.\nAvailable on {server}: {available}"
                        self.audit_logger.log_command(user_input, command, False, result)
                        return False, result
        
        try:
            if operation == "list_servers":
                success, result = self._list_servers()
            elif operation == "list_backups":
                success, result = self._list_backups(params)
            elif operation == "trigger_backup":
                success, result = self._trigger_backup(params)
            elif operation == "multi_server_backup":
                success, result = self._multi_server_backup(params)
            elif operation == "recommend_restore":
                success, result = self._recommend_restore(params)
            elif operation == "auto_restore":
                success, result = self._auto_restore(params, user_input)
            elif operation == "automated_restore": 
                success, result = self._automated_restore(params, user_input)
            elif operation == "enable_schedules":
                success, result = self._enable_schedules(params)
            elif operation == "health_check":
                success, result = self._health_check()
            elif operation == "show_logs":
                self.audit_logger.show_recent_logs()
                return True, "Logs displayed above"
            elif operation == "help":
                success, result = self._show_help()
            else:
                success = False
                result = f"Unknown operation: {operation}"
            
            # Log the command
            self.audit_logger.log_command(user_input, command, success, result)
            return success, result
                
        except Exception as e:
            import traceback
            result = f"Error executing command: {str(e)}\n{traceback.format_exc()}"
            self.audit_logger.log_command(user_input, command, False, result)
            return False, result
    
    def _format_available_resources(self):
        """Format available servers and databases."""
        if not self.server_databases:
            return "No servers discovered."
        
        result = "Available resources:\n"
        for server, databases in self.server_databases.items():
            db_list = ', '.join(databases)
            result += f"  {server}: {db_list}\n"
        return result
    
    def _list_servers(self):
        """List available servers."""
        servers = self.client.list_servers()
        result = "📊 Available Servers:\n"
        for server in servers['servers']:
            status = "✓ Connected" if server['connected'] else "✗ Disconnected"
            result += f"  • {server['name']}: {status}\n"
            if server['name'] in self.server_databases:
                dbs = self.server_databases[server['name']]
                if dbs:
                    result += f"    Databases: {', '.join(dbs)}\n"
        return True, result
    
    def _list_backups(self, params):
        """List backups for a database."""
        server = params.get('server')
        database = params.get('database')
        limit = params.get('limit', 10)
        
        if not server or not database:
            return False, "Missing server or database parameter"
        
        backups = self.client.list_backups(server, database, limit=limit)
        backup_list = backups.get('backups', [])
        
        if not backup_list:
            return True, f"📋 No backups found for '{database}' on {server}"
        
        result = f"📋 Backups for '{database}' on {server} (Last {len(backup_list)}):\n\n"
        for i, backup in enumerate(backup_list, 1):
            result += f"{i}. {backup.get('id')}\n"
            result += f"    Type: {backup.get('type')}\n"
            result += f"    Time: {backup.get('timestamp')}\n"
            result += f"    Size: {backup.get('size_bytes', 0):,} bytes\n"
            result += f"    Status: {backup.get('status')}\n\n"
        
        return True, result
    
    def _trigger_backup(self, params):
        """Trigger a backup."""
        server = params.get('server')
        database = params.get('database')
        backup_type = params.get('type', 'full')
        
        if not server or not database:
            return False, "Missing server or database parameter"
        
        print(f"🔄 Triggering {backup_type} backup for '{database}' on {server}...")
        
        result = self.client.trigger_backup(server, database, backup_type)
        content = result.get('result', {}).get('content', [])
        
        if content:
            text = content[0].get('text', '{}')
            data = json.loads(text)
            
            if data.get('success'):
                msg = f"✅ Backup completed successfully!\n"
                msg += f"    Backup ID: {data.get('backup_id')}\n"
                msg += f"    Type: {backup_type}\n"
                msg += f"    Size: {data.get('size_bytes', 0):,} bytes\n"
                msg += f"    Timestamp: {data.get('timestamp')}"
                return True, msg
            else:
                return False, f"❌ Backup failed: {data.get('error')}"
        
        return False, "❌ Unexpected response format"
    
    def _multi_server_backup(self, params):
        """Trigger multi-server backup."""
        servers = params.get('servers', [])
        database = params.get('database')
        backup_type = params.get('type', 'full')
        
        if not servers:
            servers = list(self.server_databases.keys())
        
        if not database:
            return False, "Missing database parameter"
        
        print(f"🔄 Triggering coordinated {backup_type} backup of '{database}' across {len(servers)} server(s)...")
        print(f"   Servers: {', '.join(servers)}")
        
        result = self.client.multi_server_backup(servers, database, backup_type)
        
        msg = f"📊 Multi-Server Backup Results for '{database}':\n\n"
        success_count = 0
        fail_count = 0
        
        for server, status in result['results'].items():
            if status['status'] == 'success':
                success_count += 1
                res = status.get('result', {})
                if isinstance(res, dict) and res.get('success'): 
                    msg += f"✅ {server}: Success\n"
                    msg += f"    Backup ID: {res.get('backup_id')}\n"
                    msg += f"    Size: {res.get('size_bytes', 0):,} bytes\n"
                else:
                    msg += f"✅ {server}: Completed\n"
            else:
                fail_count += 1
                msg += f"❌ {server}: Failed\n"
                msg += f"    Error: {status.get('error', 'Unknown')}\n"
            msg += "\n"
        
        msg += f"📈 Summary: {success_count} successful, {fail_count} failed\n"
        
        return not result.get('has_errors', True), msg
    
    def _recommend_restore(self, params):
        """Get restore recommendations."""
        database = params.get('database')
        target_time = params.get('target_time', 'now')
        servers = params.get('servers', [])
        
        if not database:
            return False, "Missing database parameter"
        
        if not servers:
            servers = list(self.server_databases.keys())
        
        # Parse timestamp
        target_time = self._parse_timestamp(target_time)
        
        print(f"🔍 Searching for restore points for '{database}' at {target_time}...")
        
        recommendations = self.client.recommend_restore_point(
            database, target_time, servers
        )
        
        msg = f"🎯 Restore Recommendations for database '{database}':\n"
        msg += f"Target Time: {target_time}\n\n"
        
        found_any = False
        best_server = None
        best_backup = None
        
        for server, rec in recommendations['recommendations'].items():
            backup = rec.get('recommended_backup')
            time_diff = rec.get('time_difference_seconds')
            
            if backup:
                found_any = True
                if not best_backup:
                    best_server = server
                    best_backup = backup
                
                msg += f"✅ {server}:\n"
                msg += f"    Backup ID: {backup.get('id')}\n"
                msg += f"    Timestamp: {backup.get('timestamp')}\n"
                msg += f"    Time Difference: {time_diff:.0f} seconds\n"
                msg += f"    Type: {backup.get('type')}\n"
                msg += f"    Location: backups/{server}/{database}/{backup.get('id')}\n\n"
            else:
                msg += f"❌ {server}: No suitable backup found\n\n"
        
        if found_any:
            msg += "\n" + "="*60 + "\n"
            msg += "📖 HOW TO RESTORE:\n"
            msg += "="*60 + "\n\n"
            msg += "✅ EASIEST - AUTO-RESTORE:\n"
            msg += f"   Type: restore {database}\n"
            msg += f"   (Automatically finds best backup and executes restore)\n\n"
            msg += "✅ MANUAL - SPECIFIC BACKUP:\n"
            msg += f"   Type: execute restore {database} on {best_server} with ID {best_backup.get('id')}\n\n"
            
            msg += "⚠️ WARNING: This will overwrite your current database!\n"
        else:
            msg += "\n❌ No backups available for restore.\n"
        
        return True, msg
    
    def _auto_restore(self, params, user_input: str):
        """
        Automatic restore: finds best backup and executes it.
        This is the easiest way to restore - just say "restore mydb".
        """
        database = params.get('database')
        target_time = params.get('target_time', 'now')
        servers = params.get('servers', [])
        
        if not database:
            return False, "❌ Missing database parameter.\n   Format: restore [database] or restore [database] to [time]"
        
        if not servers:
            servers = list(self.server_databases.keys())
        
        # Parse timestamp
        target_time = self._parse_timestamp(target_time)
        
        print(f"\n🤖 AUTO-RESTORE MODE")
        print(f"="*70)
        print(f"Database: {database}")
        print(f"Target Time: {target_time}")
        print(f"Searching across: {', '.join(servers)}")
        print(f"="*70)
        
        # Step 1: Find best backup
        print(f"\n🔍 Step 1/3: Finding best backup...")
        
        recommendations = self.client.recommend_restore_point(
            database, target_time, servers
        )
        
        best_server = None
        best_backup = None
        best_time_diff = float('inf')
        
        for server, rec in recommendations['recommendations'].items():
            backup = rec.get('recommended_backup')
            time_diff = rec.get('time_difference_seconds', float('inf'))
            
            if backup and time_diff < best_time_diff:
                best_server = server
                best_backup = backup
                best_time_diff = time_diff
        
        if not best_backup:
            return False, f"❌ No suitable backup found for '{database}'"
        
        backup_id = best_backup.get('id')
        
        print(f"✅ Found best backup:")
        print(f"   Server: {best_server}")
        print(f"   Backup ID: {backup_id}")
        print(f"   Timestamp: {best_backup.get('timestamp')}")
        print(f"   Time Difference: {best_time_diff:.0f} seconds")
        print(f"   Type: {best_backup.get('type')}")
        
        # Step 2: Show preview and get confirmation
        print(f"\n👁️  Step 2/3: Preview and confirmation...")
        
        if self.show_preview:
            preview_ok = self._show_restore_preview(best_server, database, backup_id)
            if not preview_ok:
                return False, "❌ Restore cancelled: Could not generate preview"
        
        if self.require_confirmation:
            confirmed = self._get_human_confirmation("auto_restore", {
                'database': database,
                'server': best_server,
                'backup_id': backup_id,
                'target_time': target_time
            })
            if not confirmed:
                return False, "🛑 Auto-restore cancelled by user"
        
        # Step 3: Execute restore
        print(f"\n🚀 Step 3/3: Executing restore...")
        
        # Safety timer
        timer_ok = self._start_rollback_timer(duration_seconds=10)
        if not timer_ok:
            return False, "🛑 Restore cancelled during safety timer"
        
        try:
            result = self.client.automated_restore(best_server, database, backup_id)
            
            if result.get('status') == 'Restore command sent':
                msg = f"✅ AUTO-RESTORE completed successfully!\n\n"
                msg += f"📊 Details:\n"
                msg += f"   Database: {database}\n"
                msg += f"   Server: {best_server}\n"
                msg += f"   Backup ID: {backup_id}\n"
                msg += f"   Target Time: {target_time}\n"
                msg += f"   Backup Timestamp: {best_backup.get('timestamp')}\n"
                msg += f"\n⏳ Restore is in progress on {best_server}...\n"
                msg += f"📋 Check MCP agent logs on the server to confirm completion.\n"
                msg += f"\n💾 Audit: Logged to {self.audit_logger.log_file}"
                return True, msg
            else:
                return False, f"❌ Auto-restore failed: {result.get('result', 'Unknown error')}"

        except Exception as e:
            return False, f"❌ Failed to execute auto-restore: {e}"
    
    def _automated_restore(self, params, user_input: str):
        """
        Execute the automated restore with safety features:
        - Human confirmation
        - Restore preview
        - Rollback timer
        """
        server = params.get('server')
        database = params.get('database')
        backup_id = params.get('backup_id')
        
        if not server or not database or not backup_id:
            return False, "❌ Missing server, database, or backup_id parameter.\n   Format: execute restore [db] on [server] with ID [id]"

        # SAFETY CHECK 1: Show Preview
        if self.show_preview:
            preview_ok = self._show_restore_preview(server, database, backup_id)
            if not preview_ok:
                return False, "❌ Restore cancelled: Could not generate preview"
        
        # SAFETY CHECK 2: Human Confirmation
        if self.require_confirmation:
            confirmed = self._get_human_confirmation("automated_restore", params)
            if not confirmed:
                return False, "🛑 Restore cancelled by user"
        
        # SAFETY CHECK 3: Rollback Timer (10 seconds for demo, normally 30-300s)
        print("\n⏰ Starting safety timer...")
        timer_ok = self._start_rollback_timer(duration_seconds=10)
        if not timer_ok:
            return False, "🛑 Restore cancelled during safety timer"
        
        # Execute restore
        print(f"\n🚀 Executing restore now...")
        
        try:
            result = self.client.automated_restore(server, database, backup_id)
            
            if result.get('status') == 'Restore command sent':
                msg = f"✅ Automated Restore command successfully sent to {server}.\n"
                msg += f"   Restoring database '{database}' from backup ID: {backup_id}\n"
                msg += "   ⏳ Restore is in progress...\n"
                msg += "   📋 Check MCP agent logs on the server to confirm completion.\n"
                msg += f"\n💾 Audit: Logged to {self.audit_logger.log_file}"
                return True, msg
            else:
                return False, f"❌ Automated Restore failed: {result.get('result', 'Unknown error')}"

        except Exception as e:
            return False, f"❌ Failed to execute automated restore: {e}"

    def _enable_schedules(self, params):
        """Enable backup schedules."""
        server = params.get('server')
        incremental = params.get('incremental_every', 'PT2M')
        full_cron = params.get('full_cron', '0 3 * * 0')
        
        if not server:
            return False, "Server parameter required"
        
        result = self.client.enable_schedules(server, incremental, full_cron)
        content = result.get('result', {}).get('content', [])
        
        if content:
            text = content[0].get('text', '{}')
            data = json.loads(text)
            
            if data.get('success'):
                msg = f"✅ Schedules configured for {server}\n"
                msg += f"    Incremental: {incremental}\n"
                msg += f"    Full: {full_cron}\n"
                msg += f"    Note: {data.get('note', '')}"
                return True, msg
        
        return False, "❌ Failed to configure schedules"
    
    def _health_check(self):
        """Check system health."""
        health = self.client.health_check()
        
        msg = "🏥 System Health:\n\n"
        for server, status in health.items():
            details = status.get('details', {})
            total_backups = details.get('total_backups', 0)
            msg += f"{'✅' if status['status'] == 'healthy' else '❌'} {server}: "
            msg += f"{status['status']} ({total_backups} backups)\n"
            if server in self.server_databases:
                dbs = self.server_databases[server]
                if dbs:
                    msg += f"    Databases: {', '.join(dbs)}\n"
        
        return True, msg
    
    def _show_help(self):
        """Show help information."""
        structure = self._format_available_resources()
        
        help_text = f"""
📚 Natural Language Commands - PostgreSQL Backup System

{structure}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔵 BACKUP OPERATIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Full Backup (Default):
  💬 "backup pg2"
  💬 "create a full backup of mydb"
  💬 "do full backup of postgres on pg1"

Incremental Backup:
  💬 "do incremental backup of pg2"
  💬 "incremental backup mydb on pg1"

Multi-Server Backup (NEW):
  💬 "backup postgres on all servers"
  💬 "backup mydb on both servers"
  💬 "multi server backup of postgres"
  
List Backups:
  💬 "list backups for mydb on pg1"
  💬 "show backups of postgres"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔵 RESTORE OPERATIONS (WITH SAFETY CHECKS)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🚀 Auto-Restore (NEW - Easiest Way):
  💬 "restore postgres"
  💬 "restore postgres now"
  💬 "restore mydb to 2024-12-08 10:20"
  
  ✨ What it does:
     • Automatically finds the best backup
     • Shows preview with details
     • Requires confirmation (type YES)
     • Has 10-second safety timer
     • Executes restore automatically

Find Restore Point (Just Recommendations):
  💬 "show restore point for postgres"
  💬 "find restore point for mydb at 2024-12-08 10:20"
  💬 "restore mydb to recent timestamp"

Execute with Specific Backup ID:
  💬 "execute restore postgres on PG1 with ID full_20251208_051619"
  
  🛡️ Safety Features (All Restore Operations):
    ✓ Preview before execution
    ✓ Human confirmation required (type YES)
    ✓ 10-second rollback timer (Ctrl+C to cancel)
    ✓ All actions logged to audit file

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔵 SYSTEM OPERATIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  💬 "list servers" - Show all available servers
  💬 "health check" - Check system health
  💬 "show logs" - Show recent command history (audit trail)
  💬 "enable schedules for pg1" - Configure automated backups
  
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔵 SPECIAL COMMANDS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  • "refresh" - Rediscover servers and databases
  • "help" - Show this help message
  • "quit" or "exit" - Exit the program

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💡 TIPS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ✓ Use natural language - the AI understands context
  ✓ Timestamps: "now", "recent", "2024-12-08 10:20", or "latest"
  ✓ Default backup type is "full" unless you say "incremental"
  ✓ Multi-server: "all servers" or "both servers" works automatically
  ✓ Auto-restore: Just say "restore [database]" - it does everything!
  ✓ All dangerous operations require confirmation
  ✓ All commands are logged for audit purposes

📁 Audit Log: backup_audit.log

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎯 QUICK EXAMPLES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Backup:
  "backup postgres on all servers"
  "do incremental backup of mydb"

Restore (Easiest):
  "restore postgres"              ← Automatic, finds best backup
  "restore mydb to yesterday"     ← Finds backup closest to time

Restore (Advanced):
  "show restore point for postgres"  ← Just shows options
  "execute restore postgres on PG1 with ID full_20241208_102030"

"""
        return True, help_text


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main interactive loop."""
    print("=" * 70)
    print("  PostgreSQL Backup System - Natural Language Interface")
    print("  🚀 Enhanced with Auto-Restore & Multi-Server Support")
    print("=" * 70)
    
    # Check Ollama availability
    print("\n🔍 Checking Ollama...")
    try:
        models = ollama.list()
        print(f"✅ Ollama is running ({len(models.get('models', []))} models available)")
    except Exception as e:
        print(f"❌ Ollama not available: {e}")
        print("\nPlease ensure Ollama is installed and running: 'ollama serve'")
        sys.exit(1)
    
    # Initialize client and assistant
    print("\n🔧 Initializing backup system...")
    try:
        client = MCPBackupClient("http://localhost:8000")
        assistant = BackupAssistant(client, model="llama3.1")
        print("✅ System ready!\n")
        print(f"🛡️ Safety features enabled:")
        print(f"   ✓ Human confirmation: {assistant.require_confirmation}")
        print(f"   ✓ Restore preview: {assistant.show_preview}")
        print(f"   ✓ Audit logging: {assistant.audit_logger.log_file}")
        print()
    except Exception as e:
        print(f"❌ Failed to initialize: {e}")
        sys.exit(1)
    
    # Warm up the model
    print("🔥 Warming up AI model...")
    try:
        warmup_start = datetime.now()
        ollama.chat(
            model="llama3.1",
            messages=[{"role": "user", "content": "hello"}],
            options={"num_predict": 10}
        )
        warmup_time = (datetime.now() - warmup_start).total_seconds()
        print(f"✅ Model loaded in {warmup_time:.1f}s\n")
    except Exception as e:
        print(f"⚠️ Model warmup failed: {e}\n")
    
    # Show discovered structure
    print(assistant._format_available_resources())
    
    # Debug: Show what servers were actually discovered
    if assistant.server_databases:
        print("\n🔍 Debug - Discovered servers:")
        for server_name in assistant.server_databases.keys():
            print(f"   • '{server_name}' (type: {type(server_name).__name__})")
    else:
        print("\n⚠️ WARNING: No servers were discovered!")
        print("   Please check if the FastAPI server is running and accessible")
    
    print()
    print("💡 NEW: Type 'restore [database]' for automatic restore!")
    print("💡 NEW: Type 'backup [db] on all servers' for multi-server backup!")
    print("💡 Type 'help' for all commands, 'show logs' for history, 'quit' to exit.")
    print("💡 Type 'refresh' to rediscover servers if they're missing.")
    print("=" * 70)
    
    # Interactive loop
    while True:
        try:
            user_input = input("\n💬 You: ").strip()
            
            if not user_input:
                continue
            
            if user_input.lower() in ['quit', 'exit', 'bye']:
                print("\n👋 Goodbye!")
                break
            
            if user_input.lower() == 'refresh':
                print("🔄 Rediscovering system...")
                assistant._discover_system()
                print(assistant._format_available_resources())
                continue
            
            # Parse input with AI (NO REGEX)
            print("🤔 Understanding your request...")
            command, error = assistant.parse_user_input(user_input)
            
            if error:
                print(f"❌ {error}")
                continue
            
            if not command:
                print("❌ Could not understand the request. Type 'help' for examples.")
                continue
            
            # Show what was understood
            op = command.get('operation')
            params = command.get('parameters', {})
            confidence = command.get('confidence', 0.0)
            
            print(f"📝 Interpreted as: {op}", end="")
            if params:
                param_str = ", ".join([f"{k}={v}" for k, v in params.items() if v and k not in ['limit']])
                if param_str:
                    print(f" ({param_str})", end="")
            print(f" [confidence: {confidence:.0%}]")
            
            # Execute command with audit logging
            success, result = assistant.execute_command(command, user_input)
            
            print(f"\n{'✅' if success else '❌'} Result:")
            print(result)
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Unexpected Error: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()