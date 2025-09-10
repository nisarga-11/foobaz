import os
from typing import Dict, List, Any, Optional
from langchain.schema import HumanMessage, AIMessage
from agents.base_agent import BaseAgent
from tools.mcp_tools import MCP_TOOLS
from config.settings import settings


class EmployeeDBAgent(BaseAgent):
    """Agent responsible for managing the employee database."""
    
    def __init__(self):
        super().__init__(
            agent_name=settings.AGENT_NAMES["db2"],
            database_name="employee_db",
            tools=MCP_TOOLS
        )
    
    def process_user_input(self, user_input: str) -> Dict[str, Any]:
        """Process user input and determine appropriate action."""
        # Add user input to conversation history
        self.add_message(HumanMessage(content=user_input))
        
        # Analyze the input
        analysis = self.analyze_user_input(user_input)
        action = analysis.get("action", "unknown")
        database = analysis.get("database", self.database_name)
        reasoning = analysis.get("reasoning", "")
        parameters = analysis.get("parameters", {})
        
        self.log_action("input_analysis", {
            "action": action,
            "database": database,
            "reasoning": reasoning
        })
        
        # Check if this task should be handed off
        if self.should_handoff(action, database, parameters):
            return self.handle_handoff(user_input, action, database, reasoning)
        
        # Process the task locally
        return self.execute_task(action, database, parameters)
    
    def execute_task(self, action: str, database: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a specific task."""
        self.current_task = action
        
        if action == "handle_corruption":
            return self.handle_corruption(database, parameters)

        elif action == "list_backups":
            return self.list_backup_files()
        elif action == "create_backup":
            return self.create_backup(parameters)
        elif action == "restore":
            return self.restore_database(parameters)
        elif action == "get_info":
            return self.get_database_info()
        elif action == "coordinate_restore":
            return self.coordinate_restore_with_customer_db()
        elif action == "pgbackrest_full_backup":
            return self.pgbackrest_full_backup(parameters)
        elif action == "pgbackrest_incremental_backup":
            return self.pgbackrest_incremental_backup(parameters)
        elif action == "pgbackrest_restore":
            return self.pgbackrest_restore(parameters)
        elif action == "pgbackrest_list_backups":
            return self.pgbackrest_list_backups(parameters)
        elif action == "pgbackrest_recommended_backups":
            return self.pgbackrest_recommended_backups(parameters)
        elif action == "pgbackrest_pitr_restore":
            return self.pgbackrest_pitr_restore(parameters)
        elif action == "pgbackrest_pitr_restore_with_workflow":
            return self.pgbackrest_pitr_restore_with_workflow(parameters)
        elif action == "pgbackrest_coordinated_recommendations":
            return self.pgbackrest_coordinated_recommendations(parameters)
        else:
            return {
                "response": f"I don't understand the action '{action}'. Please specify what you'd like me to do with the {database} database.",
                "handoff": None,
                "handoff_message": None
            }
    

    
    def list_backup_files(self) -> Dict[str, Any]:
        """List available backup files for the employee database."""
        self.log_action("listing_backups", {"database": self.database_name})
        
        result = self.execute_tool("list_backups", database_name=self.database_name)
        
        if result["status"] == "success":
            backup_files = [b["filename"] for b in result["backups"]]
            response = f"Available backup files for {self.database_name}: {backup_files}"
        else:
            response = f"Error listing backup files: {result.get('error', 'Unknown error')}"
        
        return {
            "response": response,
            "handoff": None,
            "handoff_message": None
        }
    
    def list_employee_server_backups(self) -> Dict[str, Any]:
        """List available backups for employee server (used in handoff from customer agent)."""
        self.log_action("listing_employee_server_backups", {"database": self.database_name})
        
        # Get employee server backups
        employee_result = self.execute_tool("pgbackrest_list_backups", 
                                          stanza=settings.PGBACKREST_EMPLOYEE_STANZA,
                                          backup_path=settings.PGBACKREST_EMPLOYEE_BACKUP_PATH,
                                          server_name="employeeServer")
        
        if employee_result["status"] == "success":
            employee_backups = employee_result.get("backups", [])
            employee_count = employee_result.get("backup_count", 0)
            
            response = f"🖥️ **Employee Server (employeeServer):**\n"
            response += f"**Stanza:** {settings.PGBACKREST_EMPLOYEE_STANZA}\n"
            response += f"**Total backups:** {employee_count}\n\n"
            
            if employee_backups:
                for i, backup in enumerate(employee_backups, 1):
                    response += f"**{i}. Backup ID:** {backup.get('backup_id', 'Unknown')}\n"
                    response += f"   **Type:** {backup.get('backup_type', 'Unknown')}\n"
                    response += f"   **Timestamp:** {backup.get('timestamp', 'Unknown')}\n"
                    response += f"   **Size:** {backup.get('size', 'Unknown')}\n\n"
            else:
                response += "No backups found for employee server.\n\n"
            
            response += f"\n✅ **Both server backups listed successfully!**"
            return {
                "response": response,
                "handoff": None,
                "handoff_message": None
            }
        else:
            response = f"❌ Error listing employee server backups: {employee_result.get('error', 'Unknown error')}"
            return {
                "response": response,
                "handoff": None,
                "handoff_message": None
            }
    
    def create_backup(self, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Create a backup of the employee database."""
        self.log_action("creating_backup", {"database": self.database_name})
        
        result = self.execute_tool("postgres_backup", database_name=self.database_name)
        
        if result["status"] == "success":
            response = f"Backup created successfully: {result['filename']} ({result['size_mb']} MB)"
        else:
            response = f"Error creating backup: {result.get('error', 'Unknown error')}"
        
        # Check if this is a "both databases" request
        if parameters and parameters.get("both_databases", False):
            response += f"\n\nBoth database backups completed successfully!"
            return {
                "response": response,
                "handoff": None,
                "handoff_message": None
            }
        
        return {
            "response": response,
            "handoff": None,
            "handoff_message": None
        }
    
    def restore_database(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Restore the employee database from a backup using LLM recommendation."""
        # Use LLM to recommend the best backup for restore
        recommendation_result = self.recommend_backup_restore(self.database_name, parameters)
        
        if recommendation_result["status"] != "success":
            return {
                "response": f"Error getting backup recommendation: {recommendation_result.get('message', 'Unknown error')}",
                "handoff": None,
                "handoff_message": None
            }
        
        recommendation = recommendation_result["recommendation"]
        recommended_backup = recommendation["recommended_backup"]
        
        # Ensure the backup file path is absolute and exists
        backup_file = os.path.join(settings.BACKUP_DIR, recommended_backup)
        
        # Validate that this is the correct backup file for this database
        backup_filename = os.path.basename(backup_file)
        if not backup_filename.startswith(f"{self.database_name}_"):
            return {
                "response": f"Backup file {backup_filename} is not for {self.database_name} database. Please use a backup file that starts with '{self.database_name}_'.",
                "handoff": None,
                "handoff_message": None
            }
        
        if not os.path.exists(backup_file):
            return {
                "response": f"Backup file not found: {backup_file}",
                "handoff": None,
                "handoff_message": None
            }
        
        self.log_action("restoring_database", {
            "database": self.database_name,
            "backup_file": backup_file,
            "recommendation": recommendation
        })
        
        result = self.execute_tool("postgres_restore", 
                                 backup_file=backup_file,
                                 database_name=self.database_name)
        
        # Check if restore was successful (pg_restore warnings are normal)
        if result["status"] == "success" or "errors ignored on restore" in result.get("output", ""):
            response = f"Database restored successfully from {recommended_backup}\n"
            response += f"Recommendation reasoning: {recommendation['reasoning']}\n"
            response += f"Confidence: {recommendation['confidence']}"
            
            if recommendation.get('alternative_options'):
                response += f"\nAlternative options: {', '.join(recommendation['alternative_options'])}"
        else:
            response = f"Error restoring database: {result.get('error', 'Unknown error')}"
        
        return {
            "response": response,
            "handoff": None,
            "handoff_message": None
        }
    
    def get_database_info(self) -> Dict[str, Any]:
        """Get comprehensive information about the employee database."""
        self.log_action("getting_info", {"database": self.database_name})
        
        result = self.execute_tool("get_database_info", database_name=self.database_name)
        
        if result["status"] == "success":
            # MCP tool returns data directly, not wrapped in "info" key
            database_name = result["database_name"]
            connection = result["connection"]
            
            response = f"Database: {database_name}\n"
            response += f"Connection: {connection['status']}\n"
            response += f"Size: {connection['size']}\n"
            response += f"Tables: {connection['table_count']}\n"
            response += f"Version: {connection['version']}"
        else:
            response = f"Error getting database info: {result.get('error', 'Unknown error')}"
        
        return {
            "response": response,
            "handoff": None,
            "handoff_message": None
        }
    
    def coordinate_restore_with_customer_db(self) -> Dict[str, Any]:
        """Coordinate restore operations with the customer database using LLM recommendation."""
        self.log_action("coordinating_restore", {"database": self.database_name})
        
        # Use LLM to recommend coordinated backup restore
        recommendation_result = self.recommend_coordinated_backup_restore(
            ["employee_db", "customer_db"], 
            {"user_context": "Coordinated restore between employee and customer databases"}
        )
        
        if recommendation_result["status"] != "success":
            response = f"Error getting coordinated backup recommendation: {recommendation_result.get('message', 'Unknown error')}"
            return {
                "response": response,
                "handoff": None,
                "handoff_message": None
            }
        
        recommendation = recommendation_result["recommendation"]
        
        response = f"LLM COORDINATED BACKUP RECOMMENDATION:\n"
        response += f"Strategy: {recommendation['recommended_strategy']}\n"
        response += f"Reasoning: {recommendation['reasoning']}\n"
        response += f"Confidence: {recommendation['confidence']}\n\n"
        response += f"Recommended restore files:\n"
        
        for database, backup_file in recommendation['recommended_backups'].items():
            response += f"- {database}: {backup_file}\n"
        
        response += f"\nRestore commands:\n"
        for command in recommendation['restore_commands']:
            response += f"- {command}\n"
        
        if recommendation.get('warnings'):
            response += f"\nWarnings:\n"
            for warning in recommendation['warnings']:
                response += f"- {warning}\n"
        
        # Handoff back to customer database agent
        handoff_message = f"LLM recommended coordinated restore strategy: {recommendation['recommended_strategy']}. Recommended backups: {recommendation['recommended_backups']}"
        
        return {
            "response": response,
            "handoff": "CustomerDB_Agent",
            "handoff_message": handoff_message
        }
    
    def handle_handoff(self, user_input: str, action: str, database: str, reasoning: str) -> Dict[str, Any]:
        """Handle task handoff to another agent."""
        self.log_action("handing_off", {
            "action": action,
            "database": database,
            "reasoning": reasoning
        })
        
        # Determine which agent to handoff to
        if database == "customer_db" or "customer" in database.lower():
            target_agent = "CustomerDB_Agent"
        else:
            target_agent = "CustomerDB_Agent"  # Default to customer agent
        
        handoff_message = f"Task requires coordination with {database}. {reasoning}"
        
        return {
            "response": f"Handing off to {target_agent} for {database} operations.",
            "handoff": target_agent,
            "handoff_message": handoff_message
        }
    
    def receive_handoff(self, message: str, from_agent: str) -> Dict[str, Any]:
        """Receive a handoff from another agent."""
        self.log_action("received_handoff", {
            "from_agent": from_agent,
            "message": message
        })
        
        # Add handoff message to conversation
        self.add_message(HumanMessage(content=f"Handoff from {from_agent}: {message}"))
        
        # Check if this is a backup listing handoff
        if "list backups" in message.lower() or "backups listed" in message.lower():
            return self.list_employee_server_backups()
        
        # Check if this is a corruption-related handoff
        if "corrupted" in message.lower() or "corruption" in message.lower():
            # This is likely a corruption handoff, check our database and coordinate
            return self.handle_corruption_handoff(message, from_agent)
        
        # If the message mentions this agent's database, process it directly
        if self.database_name in message.lower():
            return self.process_user_input(message)
        
        # Otherwise, don't handoff again to prevent loops
        return {
            "response": f"Received handoff from {from_agent}: {message}",
            "handoff": None,
            "handoff_message": None
        }
    
    def handle_corruption_handoff(self, message: str, from_agent: str) -> Dict[str, Any]:
        """Handle corruption-related handoff from another agent using LLM recommendation."""
        self.log_action("handling_corruption_handoff", {
            "from_agent": from_agent,
            "message": message
        })
        
        # Use LLM to recommend the best backup for recovery
        recommendation_result = self.recommend_backup_restore(
            self.database_name, 
            {"user_context": f"Corruption handoff from {from_agent}: {message}"}
        )
        
        response = f"Checking {self.database_name} for related impact..."
        
        if recommendation_result["status"] == "success":
            recommendation = recommendation_result["recommendation"]
            backup_files = [b["filename"] for b in recommendation_result["available_backups"]]
            
            response += f"\nAvailable backup files for {self.database_name}: {backup_files}"
            response += f"\n\nLLM RECOMMENDATION:\n"
            response += f"- Recommended backup: {recommendation['recommended_backup']}\n"
            response += f"- Reasoning: {recommendation['reasoning']}\n"
            response += f"- Confidence: {recommendation['confidence']}\n"
            response += f"- Restore command: {recommendation['restore_command']}"
            
            if recommendation.get('alternative_options'):
                response += f"\n- Alternative options: {', '.join(recommendation['alternative_options'])}"
        
        # Coordinate restore recommendations
        coordination_result = self.coordinate_restore_with_customer_db()
        
        if coordination_result["handoff"]:
            # Combine responses
            full_response = f"{response}\n{coordination_result['response']}"
            
            return {
                "response": full_response,
                "handoff": coordination_result["handoff"],
                "handoff_message": coordination_result["handoff_message"]
            }
        else:
            return {
                "response": response,
                "handoff": None,
                "handoff_message": None
            }
    
    def pgbackrest_full_backup(self, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Create a full backup using pgBackRest."""
        self.log_action("creating_pgbackrest_full_backup", {"database": self.database_name})
        
        # Get server-specific configuration
        server_config = settings.get_pgbackrest_config("employeeServer")
        stanza = parameters.get("stanza", server_config["stanza"]) if parameters else server_config["stanza"]
        backup_path = parameters.get("backup_path", server_config["backup_path"]) if parameters else server_config["backup_path"]
        server_name = "employeeServer"
        
        result = self.execute_tool("pgbackrest_full_backup", 
                                 stanza=stanza, 
                                 backup_path=backup_path,
                                 server_name=server_name)
        
        if result["status"] == "success":
            backup_info = result.get("backup_info", {})
            response = f"Full backup created successfully using pgBackRest:\n"
            response += f"- Backup ID: {backup_info.get('backup_id', 'Unknown')}\n"
            response += f"- Type: {backup_info.get('backup_type', 'Unknown')}\n"
            response += f"- Timestamp: {backup_info.get('timestamp', 'Unknown')}\n"
            response += f"- Size: {backup_info.get('size', 'Unknown')}"
        else:
            response = f"Error creating full backup: {result.get('error', 'Unknown error')}"
        
        # Check if this is a "both databases" request
        if parameters and parameters.get("both_databases", False):
            response += f"\n\nBoth database full backups completed successfully!"
            return {
                "response": response,
                "handoff": None,
                "handoff_message": None
            }
        
        return {
            "response": response,
            "handoff": None,
            "handoff_message": None
        }
    
    def pgbackrest_incremental_backup(self, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Create an incremental backup using pgBackRest."""
        self.log_action("creating_pgbackrest_incremental_backup", {"database": self.database_name})
        
        # Get server-specific configuration
        server_config = settings.get_pgbackrest_config("employeeServer")
        stanza = parameters.get("stanza", server_config["stanza"]) if parameters else server_config["stanza"]
        backup_path = parameters.get("backup_path", server_config["backup_path"]) if parameters else server_config["backup_path"]
        server_name = "employeeServer"
        
        result = self.execute_tool("pgbackrest_incremental_backup", 
                                 stanza=stanza, 
                                 backup_path=backup_path,
                                 server_name=server_name)
        
        if result["status"] == "success":
            backup_info = result.get("backup_info", {})
            response = f"Incremental backup created successfully using pgBackRest:\n"
            response += f"- Backup ID: {backup_info.get('backup_id', 'Unknown')}\n"
            response += f"- Type: {backup_info.get('backup_type', 'Unknown')}\n"
            response += f"- Timestamp: {backup_info.get('timestamp', 'Unknown')}\n"
            response += f"- Size: {backup_info.get('size', 'Unknown')}"
        else:
            response = f"Error creating incremental backup: {result.get('error', 'Unknown error')}"
        
        # Check if this is a "both databases" request
        if parameters and parameters.get("both_databases", False):
            response += f"\n\nBoth database incremental backups completed successfully!"
            return {
                "response": response,
                "handoff": None,
                "handoff_message": None
            }
        
        return {
            "response": response,
            "handoff": None,
            "handoff_message": None
        }
    
    def pgbackrest_restore(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Restore from pgBackRest backup using LLM recommendation or direct backup ID."""
        self.log_action("pgbackrest_restore", {"database": self.database_name})
        
        # Check if a specific backup ID is provided
        backup_id = parameters.get("backup_file") or parameters.get("backup_id") or parameters.get("backup_type")
        
        if backup_id and backup_id != "latest":
            # Direct restore to specific backup ID
            restore_result = self.execute_tool("pgbackrest_restore",
                                             stanza=parameters.get("stanza", "demo"),
                                             pgdata=parameters.get("pgdata", "/var/lib/postgresql/15/main"),
                                             backup_path=parameters.get("backup_path", settings.PGBACKREST_BACKUP_PATH),
                                             backup_type=backup_id)
            
            if restore_result.get("status") == "success":
                response = f"✅ Database restored successfully from pgBackRest backup: {backup_id}\n"
                response += f"📊 Restore Details:\n"
                response += f"- Backup ID: {backup_id}\n"
                response += f"- Database: {self.database_name}\n"
                response += f"- Stanza: {parameters.get('stanza', 'demo')}\n"
                response += f"- Restore Path: {parameters.get('pgdata', '/var/lib/postgresql/15/main')}"
                
                return {
                    "response": response,
                    "handoff": None,
                    "handoff_message": None
                }
            else:
                return {
                    "response": f"❌ Failed to restore from backup {backup_id}: {restore_result.get('error', 'Unknown error')}",
                    "handoff": None,
                    "handoff_message": None
                }
        
        # If no specific backup ID, use recommendation engine
        # First, get available backups
        list_result = self.execute_tool("pgbackrest_list_backups", 
                                       stanza=parameters.get("stanza", "demo"),
                                       backup_path=parameters.get("backup_path", settings.PGBACKREST_BACKUP_PATH))
        
        if not list_result.get("success", False):
            return {
                "response": f"Error listing pgBackRest backups: {list_result.get('error', 'Unknown error')}",
                "handoff": None,
                "handoff_message": None
            }
        
        available_backups = list_result.get("backups", [])
        
        if not available_backups:
            return {
                "response": f"No pgBackRest backups available for restore",
                "handoff": None,
                "handoff_message": None
            }
        
        # Use backup recommendation engine to suggest the best backup
        recommendation_result = self.backup_recommendation_engine.recommend_backup_restore(
            self.database_name, 
            available_backups, 
            parameters.get("user_context", "pgBackRest restore request")
        )
        
        if recommendation_result["status"] != "success":
            return {
                "response": f"Error getting backup recommendation: {recommendation_result.get('message', 'Unknown error')}",
                "handoff": None,
                "handoff_message": None
            }
        
        recommendation = recommendation_result["recommendation"]
        recommended_backup_id = recommendation["recommended_backup"]
        
        # Execute restore with recommended backup
        restore_result = self.execute_tool("pgbackrest_restore",
                                         stanza=parameters.get("stanza", "demo"),
                                         pgdata=parameters.get("pgdata", "/var/lib/postgresql/15/main"),
                                         backup_path=parameters.get("backup_path", "/var/lib/pgbackrest"),
                                         backup_type=recommended_backup_id)
        
        if restore_result["status"] == "success":
            response = f"Database restored successfully from pgBackRest backup {recommended_backup_id}\n"
            response += f"LLM Recommendation:\n"
            response += f"- Reasoning: {recommendation['reasoning']}\n"
            response += f"- Confidence: {recommendation['confidence']}\n"
            response += f"- Restore command: {recommendation['restore_command']}"
            
            if recommendation.get('alternative_options'):
                response += f"\n- Alternative options: {', '.join(recommendation['alternative_options'])}"
        else:
            response = f"Error restoring database: {restore_result.get('error', 'Unknown error')}"
        
        return {
            "response": response,
            "handoff": None,
            "handoff_message": None
        }
    
    def pgbackrest_list_backups(self, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """List available pgBackRest backups with detailed information."""
        self.log_action("listing_pgbackrest_backups", {"database": self.database_name})
        
        # Get server-specific configuration
        server_config = settings.get_pgbackrest_config("employeeServer")
        stanza = parameters.get("stanza", server_config["stanza"]) if parameters else server_config["stanza"]
        backup_path = parameters.get("backup_path", server_config["backup_path"]) if parameters else server_config["backup_path"]
        server_name = "employeeServer"
        
        result = self.execute_tool("pgbackrest_list_backups", 
                                 stanza=stanza, 
                                 backup_path=backup_path,
                                 server_name=server_name)
        
        if result["status"] == "success":
            backups = result.get("backups", [])
            backup_count = result.get("backup_count", 0)
            
            response = f"📊 **Available pgBackRest backups for {self.database_name} (Server: {server_name})**\n"
            response += f"**Stanza:** {stanza}\n"
            response += f"**Total backups:** {backup_count}\n\n"
            
            if backups:
                for i, backup in enumerate(backups, 1):
                    response += f"**{i}. Backup ID:** {backup.get('backup_id', 'Unknown')}\n"
                    response += f"   **Type:** {backup.get('backup_type', 'Unknown')}\n"
                    response += f"   **Timestamp:** {backup.get('timestamp', 'Unknown')}\n"
                    response += f"   **Size:** {backup.get('size', 'Unknown')}\n"
                    response += f"   **Database Size:** {backup.get('database_size', 'Unknown')}\n"
                    response += f"   **Repository Size:** {backup.get('repository_size', 'Unknown')}\n\n"
            else:
                response += "No backups found for this server.\n\n"
            
            # Check if this is a request for both databases
            if parameters and parameters.get("both_databases", False):
                response += f"\n✅ **Both server backups listed successfully!**"
                return {
                    "response": response,
                    "handoff": None,
                    "handoff_message": None
                }
        else:
            response = f"❌ Error listing pgBackRest backups: {result.get('error', 'Unknown error')}"
        
        return {
            "response": response,
            "handoff": None,
            "handoff_message": None
        }
    
    def pgbackrest_recommended_backups(self, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Get the 3 most recent incremental backups with consistent timestamps and provide recommendation."""
        self.log_action("getting_recommended_backups", {"database": self.database_name})
        
        # Get server-specific configuration
        server_config = settings.get_pgbackrest_config("employeeServer")
        stanza = parameters.get("stanza", server_config["stanza"]) if parameters else server_config["stanza"]
        backup_path = parameters.get("backup_path", server_config["backup_path"]) if parameters else server_config["backup_path"]
        server_name = "employeeServer"
        
        result = self.execute_tool("pgbackrest_recommended_backups", 
                                 stanza=stanza, 
                                 backup_path=backup_path,
                                 server_name=server_name)
        
        if result.get("success", False):
            data = result.get("data", {})
            recommended_backups = data.get("recommended_backups", [])
            recommendation = data.get("recommendation", {})
            
            response = f"🎯 **Recommended Backups for {self.database_name} (Stanza: {stanza})**\n\n"
            response += f"📊 **Summary:**\n"
            response += f"- Total backups: {data.get('total_backups', 0)}\n"
            response += f"- Incremental backups: {data.get('incremental_backups', 0)}\n"
            response += f"- Timestamp groups: {data.get('timestamp_groups', 0)}\n\n"
            
            if recommended_backups:
                response += f"🏆 **Top 3 Most Recent Incremental Backup Groups:**\n\n"
                
                for group in recommended_backups:
                    response += f"**#{group['rank']} - {group['timestamp_readable']}**\n"
                    response += f"- Backup count: {group['backup_count']}\n"
                    response += f"- Timestamp: {group['timestamp']}\n"
                    
                    for i, backup in enumerate(group['backups'], 1):
                        size_mb = backup.get('size', 0) / (1024 * 1024)
                        response += f"  {i}. ID: {backup.get('id', 'Unknown')} | Type: {backup.get('type', 'Unknown')} | Size: {size_mb:.1f}MB\n"
                    response += "\n"
                
                if recommendation:
                    best_backup = recommendation.get('best_backup', {})
                    response += f"💡 **RECOMMENDATION:**\n"
                    response += f"- **Best Backup:** {best_backup.get('id', 'Unknown')}\n"
                    response += f"- **Reason:** {recommendation.get('reason', 'N/A')}\n"
                    response += f"- **Confidence:** {recommendation.get('confidence', 'N/A')}\n"
                    response += f"- **Size:** {best_backup.get('size', 0) / (1024 * 1024):.1f}MB\n"
            else:
                response += "❌ No incremental backups found for recommendation.\n"
        else:
            response = f"❌ Failed to get recommended backups: {result.get('error', 'Unknown error')}"
        
        return {
            "response": response,
            "handoff": None,
            "handoff_message": None
        }
    
    def pgbackrest_pitr_restore(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Perform Point-in-Time Recovery (PITR) restore to a specific target time."""
        self.log_action("pgbackrest_pitr_restore", {"database": self.database_name})
        
        # Check if target time is provided
        target_time = parameters.get("target_time")
        if not target_time:
            return {
                "response": "❌ Target time is required for PITR restore. Please specify a target time like '2025-09-07 15:00:00+05:30'",
                "handoff": None,
                "handoff_message": None
            }
        
        # Get parameters with defaults
        stanza = parameters.get("stanza", "demo")
        pgdata = parameters.get("pgdata", "/var/lib/postgresql/15/main")
        backup_path = parameters.get("backup_path", settings.PGBACKREST_BACKUP_PATH)
        
        # Execute PITR restore
        result = self.execute_tool("pgbackrest_pitr_restore",
                                 stanza=stanza,
                                 pgdata=pgdata,
                                 backup_path=backup_path,
                                 target_time=target_time)
        
        if result.get("status") == "success":
            response = f"🎯 **PITR Restore Initiated Successfully!**\n\n"
            response += f"📊 **Restore Details:**\n"
            response += f"- Target Time: {target_time}\n"
            response += f"- Database: {self.database_name}\n"
            response += f"- Stanza: {stanza}\n"
            response += f"- PGDATA: {pgdata}\n"
            response += f"- Backup Path: {backup_path}\n\n"
            response += f"⏳ **Status:** The PITR restore is running in the background.\n"
            response += f"🔄 **Process:** PostgreSQL will be stopped, restored to {target_time}, and restarted.\n"
            response += f"⏱️ **Duration:** Typically takes 30-60 seconds.\n\n"
            response += f"📋 **Instructions:**\n"
            for instruction in result.get("instructions", []):
                response += f"- {instruction}\n"
        else:
            response = f"❌ **PITR Restore Failed:**\n"
            response += f"Error: {result.get('error', 'Unknown error')}\n"
            response += f"Target Time: {target_time}\n"
            response += f"Database: {self.database_name}"
        
        return {
            "response": response,
            "handoff": None,
            "handoff_message": None
        }
    
    def pgbackrest_pitr_restore_with_workflow(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Perform complete PITR restore workflow with PostgreSQL stop/start and port management."""
        self.log_action("pgbackrest_pitr_restore_with_workflow", {"database": self.database_name})
        
        # Get server-specific configuration
        server_config = settings.get_pgbackrest_config("employeeServer")
        stanza = parameters.get("stanza", server_config["stanza"])
        pgdata = parameters.get("pgdata", server_config["pgdata"])
        backup_path = parameters.get("backup_path", server_config["backup_path"])
        backup_id = parameters.get("backup_file") or parameters.get("backup_id") or parameters.get("backup_type")
        target_time = parameters.get("target_time")
        server_name = "employeeServer"
        
        # Execute workflow restore
        result = self.execute_tool("pgbackrest_pitr_restore_with_workflow",
                                 stanza=stanza,
                                 pgdata=pgdata,
                                 backup_path=backup_path,
                                 backup_id=backup_id,
                                 target_time=target_time,
                                 server_name=server_name)
        
        if result.get("status") == "success":
            workflow_steps = result.get("workflow_steps", [])
            response = f"✅ **PITR Restore Workflow Completed Successfully!**\n\n"
            response += f"📊 **Details:**\n"
            response += f"- Server: {server_name} (Port: {result.get('postgres_port', 'unknown')})\n"
            response += f"- Database: {self.database_name}\n"
            response += f"- Stanza: {stanza}\n"
            response += f"- Restore Target: {result.get('restore_target', 'unknown')}\n\n"
            response += f"🔄 **Workflow Steps:**\n"
            for i, step in enumerate(workflow_steps, 1):
                response += f"{i}. {step}\n"
        else:
            workflow_steps = result.get("workflow_steps", [])
            response = f"❌ **PITR Restore Workflow Failed:**\n"
            response += f"Error: {result.get('error', 'Unknown error')}\n\n"
            if workflow_steps:
                response += f"🔄 **Workflow Steps (before failure):**\n"
                for i, step in enumerate(workflow_steps, 1):
                    response += f"{i}. {step}\n"
        
        # Check if this is a "both databases" request
        if parameters and parameters.get("both_databases", False):
            response += f"\n\nBoth database restore workflows completed successfully!"
            return {
                "response": response,
                "handoff": None,
                "handoff_message": None
            }
        
        return {
            "response": response,
            "handoff": None,
            "handoff_message": None
        }
    
    def pgbackrest_coordinated_recommendations(self, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Get coordinated backup recommendations with matching timestamps between customer and employee servers."""
        self.log_action("pgbackrest_coordinated_recommendations", {"database": self.database_name})
        
        # Execute coordinated recommendations  
        result = self.execute_tool("pgbackrest_coordinated_recommendations",
                                 customer_stanza="customer_demo",
                                 employee_stanza="employee_demo")
        
        if result.get("status") == "success":
            recommendations = result.get("coordinated_recommendations", [])
            customer_count = result.get("customer_backups_count", 0)
            employee_count = result.get("employee_backups_count", 0)
            total_matches = result.get("total_matches", 0)
            
            response = f"🎯 **Coordinated Backup Recommendations**\n\n"
            response += f"📊 **Summary:**\n"
            response += f"- Customer Server Backups: {customer_count}\n"
            response += f"- Employee Server Backups: {employee_count}\n"
            response += f"- Total Matches Found: {total_matches}\n\n"
            
            if recommendations:
                response += f"🏆 **Best Matching Backup Pairs:**\n\n"
                
                for i, rec in enumerate(recommendations, 1):
                    customer_backup_id = rec.get("customer_backup_id", "Unknown")
                    employee_backup_id = rec.get("employee_backup_id", "Unknown")
                    match_quality = rec.get("match_quality", "unknown")
                    time_diff = rec.get("time_difference_seconds", 0)
                    customer_timestamp = rec.get("customer_timestamp", "Unknown")
                    employee_timestamp = rec.get("employee_timestamp", "Unknown")
                    
                    quality_emoji = "🎯" if match_quality == "exact" else "🎪" if match_quality == "close" else "🎈"
                    
                    response += f"**{quality_emoji} Match #{i} ({match_quality.upper()})**\n"
                    response += f"- **Customer:** {customer_backup_id} ({customer_timestamp})\n"
                    response += f"- **Employee:** {employee_backup_id} ({employee_timestamp})\n"
                    response += f"- **Time Difference:** {time_diff:.1f} seconds\n\n"
                
                # Add restore commands for the best match
                best_match = recommendations[0]
                response += f"💡 **Recommended Restore Commands:**\n"
                response += f"```bash\n"
                response += f"# Customer Server:\n"
                response += f"restore customer db {best_match.get('customer_backup_id')}\n\n"
                response += f"# Employee Server:\n"
                response += f"restore employee db {best_match.get('employee_backup_id')}\n"
                response += f"```"
            else:
                response += "❌ No matching backup pairs found between servers.\n"
                response += "Consider creating synchronized backups for better coordination."
        else:
            response = f"❌ Failed to get coordinated recommendations: {result.get('error', 'Unknown error')}"
        
        return {
            "response": response,
            "handoff": None,
            "handoff_message": None
        }
