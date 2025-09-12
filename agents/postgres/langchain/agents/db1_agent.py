import os
from typing import Dict, List, Any, Optional
from langchain.schema import HumanMessage, AIMessage
from agents.base_agent import BaseAgent
from tools.mcp_tools import MCP_TOOLS
from config.settings import settings

class CustomerDBAgent(BaseAgent):
    """Agent responsible for managing the customer database."""
    
    def __init__(self):
        super().__init__(
            agent_name=settings.AGENT_NAMES["db1"],
            database_name="customer_db",
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
            # Check if this is a request for both databases
            if parameters and parameters.get("both_databases", False):
                return self.list_both_server_backups()
            else:
                return self.list_backup_files()
        elif action == "create_backup":
            return self.create_backup(parameters)
        elif action == "restore":
            return self.restore_database(parameters)
        elif action == "pgbackrest_full_backup":
            return self.pgbackrest_full_backup(parameters)
        elif action == "pgbackrest_incremental_backup":
            return self.pgbackrest_incremental_backup(parameters)
        elif action == "pgbackrest_restore":
            return self.pgbackrest_restore(parameters)
        elif action == "pgbackrest_restore_with_stop":
            return self.pgbackrest_restore_with_stop(parameters)
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
        elif action == "get_info":
            return self.get_database_info()
        else:
            return {
                "response": f"I don't understand the action '{action}'. Please specify what you'd like me to do with the {database} database.",
                "handoff": None,
                "handoff_message": None
            }
    

    
    def list_backup_files(self) -> Dict[str, Any]:
        """List available backup files for the customer database."""
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
    
    def list_both_server_backups(self) -> Dict[str, Any]:
        """List available backups for both customer and employee servers."""
        self.log_action("listing_both_server_backups", {"database": self.database_name})
        
        # Get customer server backups
        customer_result = self.execute_tool("pgbackrest_list_backups", 
                                          stanza=settings.PGBACKREST_CUSTOMER_STANZA,
                                          backup_path=settings.PGBACKREST_CUSTOMER_BACKUP_PATH,
                                          server_name="customerServer")
        
        if customer_result["status"] == "success":
            customer_backups = customer_result.get("backups", [])
            customer_count = customer_result.get("backup_count", 0)
            
            response = f"📊 **Available pgBackRest backups for both servers:**\n\n"
            response += f"🖥️ **Customer Server (customerServer):**\n"
            response += f"**Stanza:** {settings.PGBACKREST_CUSTOMER_STANZA}\n"
            response += f"**Total backups:** {customer_count}\n\n"
            
            if customer_backups:
                for i, backup in enumerate(customer_backups, 1):
                    response += f"**{i}. Backup ID:** {backup.get('backup_id', 'Unknown')}\n"
                    response += f"   **Type:** {backup.get('backup_type', 'Unknown')}\n"
                    response += f"   **Timestamp:** {backup.get('timestamp', 'Unknown')}\n"
                    response += f"   **Size:** {backup.get('size', 'Unknown')}\n\n"
            else:
                response += "No backups found for customer server.\n\n"
            
            # Handoff to employee agent to get employee server backups
            return {
                "response": response,
                "handoff": "EmployeeDB_Agent",
                "handoff_message": f"Customer server backups listed. Please list backups for employeeServer to complete the 'both databases' request."
            }
        else:
            response = f"❌ Error listing customer server backups: {customer_result.get('error', 'Unknown error')}"
            return {
                "response": response,
                "handoff": None,
                "handoff_message": None
            }
    
    def create_backup(self, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Create a backup of the customer database."""
        self.log_action("creating_backup", {"database": self.database_name})
        
        result = self.execute_tool("postgres_backup", database_name=self.database_name)
        
        if result["status"] == "success":
            response = f"Backup created successfully: {result['filename']} ({result['size_mb']} MB)"
        else:
            response = f"Error creating backup: {result.get('error', 'Unknown error')}"
        
        # Check if this is a "both databases" request
        if parameters and parameters.get("both_databases", False):
            response += f"\n\nHanding off to EmployeeDB_Agent to create backup of employee_db as well."
            return {
                "response": response,
                "handoff": "EmployeeDB_Agent",
                "handoff_message": f"Customer database backup completed. Please create backup of employee_db to complete the 'both databases' request."
            }
        
        return {
            "response": response,
            "handoff": None,
            "handoff_message": None
        }
    
    def restore_database(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Restore the customer database from a backup using LLM recommendation."""
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
        """Get comprehensive information about the customer database."""
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
    
    def handle_handoff(self, user_input: str, action: str, database: str, reasoning: str) -> Dict[str, Any]:
        """Handle task handoff to another agent."""
        self.log_action("handing_off", {
            "action": action,
            "database": database,
            "reasoning": reasoning
        })
        
        # Determine which agent to handoff to
        if database == "employee_db" or "employee" in database.lower():
            target_agent = "EmployeeDB_Agent"
        else:
            target_agent = "EmployeeDB_Agent"  # Default to employee agent
        
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
        coordination_result = self.coordinate_restore_with_employee_db()
        
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
    
    def coordinate_restore_with_employee_db(self) -> Dict[str, Any]:
        """Coordinate restore operations with the employee database using LLM recommendation."""
        self.log_action("coordinating_restore", {"database": self.database_name})
        
        # Use LLM to recommend coordinated backup restore
        recommendation_result = self.recommend_coordinated_backup_restore(
            ["customer_db", "employee_db"], 
            {"user_context": "Coordinated restore between customer and employee databases"}
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
        
        # Handoff to employee database agent
        handoff_message = f"LLM recommended coordinated restore strategy: {recommendation['recommended_strategy']}. Recommended backups: {recommendation['recommended_backups']}"
        
        return {
            "response": response,
            "handoff": "EmployeeDB_Agent",
            "handoff_message": handoff_message
        }
    
    def pgbackrest_full_backup(self, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Create a full backup using pgBackRest."""
        self.log_action("creating_pgbackrest_full_backup", {"database": self.database_name})
        
        # Get server-specific configuration
        server_config = settings.get_pgbackrest_config("customerServer")
        stanza = parameters.get("stanza", server_config["stanza"]) if parameters else server_config["stanza"]
        backup_path = parameters.get("backup_path", server_config["backup_path"]) if parameters else server_config["backup_path"]
        server_name = "customerServer"
        
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
            response += f"\n\nHanding off to EmployeeDB_Agent to create full backup of employee_db as well."
            return {
                "response": response,
                "handoff": "EmployeeDB_Agent",
                "handoff_message": f"Customer database full backup completed. Please create full backup of employee_db to complete the 'both databases' request."
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
        server_config = settings.get_pgbackrest_config("customerServer")
        stanza = parameters.get("stanza", server_config["stanza"]) if parameters else server_config["stanza"]
        backup_path = parameters.get("backup_path", server_config["backup_path"]) if parameters else server_config["backup_path"]
        server_name = "customerServer"
        
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
            response += f"\n\nHanding off to EmployeeDB_Agent to create incremental backup of employee_db as well."
            return {
                "response": response,
                "handoff": "EmployeeDB_Agent",
                "handoff_message": f"Customer database incremental backup completed. Please create incremental backup of employee_db to complete the 'both databases' request."
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
                                             stanza=parameters.get("stanza", settings.PGBACKREST_CUSTOMER_STANZA),
                                             pgdata=parameters.get("pgdata", settings.PGBACKREST_CUSTOMER_PGDATA),
                                             backup_path=parameters.get("backup_path", settings.PGBACKREST_CUSTOMER_BACKUP_PATH),
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
                                       stanza=parameters.get("stanza", settings.PGBACKREST_CUSTOMER_STANZA),
                                       backup_path=parameters.get("backup_path", settings.PGBACKREST_CUSTOMER_BACKUP_PATH))
        
        if list_result.get("status") != "success":
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
                                         stanza=parameters.get("stanza", settings.PGBACKREST_CUSTOMER_STANZA),
                                         pgdata=parameters.get("pgdata", settings.PGBACKREST_CUSTOMER_PGDATA),
                                         backup_path=parameters.get("backup_path", settings.PGBACKREST_CUSTOMER_BACKUP_PATH),
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
    
    def pgbackrest_restore_with_stop(self, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Restore from pgBackRest backup with automatic PostgreSQL stop/start workflow."""
        self.log_action("pgbackrest_restore_with_stop", {"database": self.database_name})
        
        # Get server-specific configuration
        server_config = settings.get_pgbackrest_config("customerServer")
        stanza = parameters.get("stanza", server_config["stanza"]) if parameters else server_config["stanza"]
        backup_path = parameters.get("backup_path", server_config["backup_path"]) if parameters else server_config["backup_path"]
        pgdata = parameters.get("pgdata", server_config["pgdata"]) if parameters else server_config["pgdata"]
        backup_id = parameters.get("backup_file") or parameters.get("backup_id") or parameters.get("backup_type", "latest")
        
        response = f"🔄 **Starting Complete Restore Workflow for {self.database_name}**\n\n"
        
        try:
            # Step 1: Get backup recommendations if needed
            if backup_id == "latest":
                response += "📋 **Step 1: Getting backup recommendations...**\n"
                list_result = self.execute_tool("pgbackrest_list_backups", 
                                               stanza=stanza,
                                               backup_path=backup_path)
                
                if list_result.get("status") != "success":
                    return {
                        "response": f"❌ Error listing backups: {list_result.get('error', 'Unknown error')}",
                        "handoff": None,
                        "handoff_message": None
                    }
                
                available_backups = list_result.get("backups", [])
                if not available_backups:
                    return {
                        "response": "❌ No backups available for restore",
                        "handoff": None,
                        "handoff_message": None
                    }
                
                # Use recommendation engine
                recommendation_result = self.backup_recommendation_engine.recommend_backup_restore(
                    self.database_name, available_backups, "Complete restore workflow"
                )
                
                if recommendation_result["status"] != "success":
                    return {
                        "response": f"❌ Error getting backup recommendation: {recommendation_result.get('message', 'Unknown error')}",
                        "handoff": None,
                        "handoff_message": None
                    }
                
                backup_id = recommendation_result["recommendation"]["recommended_backup"]
                response += f"✅ Recommended backup: {backup_id}\n"
                response += f"💡 Reasoning: {recommendation_result['recommendation']['reasoning']}\n\n"
            else:
                response += f"📋 **Step 1: Using specified backup: {backup_id}**\n\n"
            
            # Step 2: Stop PostgreSQL
            response += "🛑 **Step 2: Stopping PostgreSQL...**\n"
            stop_result = self.execute_tool("postgres_stop", database_name=self.database_name)
            
            if stop_result.get("status") != "success":
                response += f"❌ Failed to stop PostgreSQL: {stop_result.get('error', 'Unknown error')}\n"
                response += "⚠️ Cannot proceed with restore while PostgreSQL is running.\n"
                return {
                    "response": response,
                    "handoff": None,
                    "handoff_message": None
                }
            
            response += f"✅ PostgreSQL stopped successfully\n\n"
            
            # Step 3: Clear data directory for clean restore
            response += f"🧹 **Step 3: Clearing data directory for clean restore...**\n"
            import os
            import shutil
            data_dir = parameters.get("pgdata", settings.PGBACKREST_CUSTOMER_PGDATA)
            
            try:
                # Remove all contents of the data directory
                for filename in os.listdir(data_dir):
                    file_path = os.path.join(data_dir, filename)
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                
                response += f"✅ Data directory cleared successfully\n\n"
            except Exception as e:
                response += f"⚠️ Warning: Could not clear data directory completely: {str(e)}\n"
                response += f"🔄 Proceeding with restore (pgBackRest may use --delta mode)\n\n"
            
            # Step 4: Perform restore
            response += f"🔄 **Step 4: Restoring from backup {backup_id}...**\n"
            restore_result = self.execute_tool("pgbackrest_restore",
                                             stanza=stanza,
                                             pgdata=pgdata,
                                             backup_path=backup_path,
                                             backup_type=backup_id)
            
            if restore_result.get("status") != "success":
                response += f"❌ Restore failed: {restore_result.get('error', 'Unknown error')}\n"
                response += "⚠️ Attempting to restart PostgreSQL...\n"
                
                # Try to restart PostgreSQL even if restore failed
                start_result = self.execute_tool("postgres_start", database_name=self.database_name)
                if start_result.get("status") == "success":
                    response += "✅ PostgreSQL restarted successfully\n"
                else:
                    response += f"❌ Failed to restart PostgreSQL: {start_result.get('error', 'Unknown error')}\n"
                    response += "🚨 MANUAL INTERVENTION REQUIRED: PostgreSQL is down!\n"
                
                return {
                    "response": response,
                    "handoff": None,
                    "handoff_message": None
                }
            
            response += f"✅ Database restored successfully from backup {backup_id}\n\n"
            
            # Step 5: Start PostgreSQL
            response += "🚀 **Step 5: Starting PostgreSQL...**\n"
            start_result = self.execute_tool("postgres_start", database_name=self.database_name)
            
            if start_result.get("status") != "success":
                response += f"❌ Failed to start PostgreSQL: {start_result.get('error', 'Unknown error')}\n"
                response += "🚨 MANUAL INTERVENTION REQUIRED: PostgreSQL is down!\n"
                return {
                    "response": response,
                    "handoff": None,
                    "handoff_message": None
                }
            
            response += f"✅ PostgreSQL started successfully\n\n"
            
            # Step 6: Verify restore
            response += "🔍 **Step 6: Verifying restore...**\n"
            verify_result = self.execute_tool("get_database_info", database_name=self.database_name)
            
            if verify_result.get("status") == "success":
                response += "✅ Database verification successful\n"
                response += f"📊 Database is accessible and ready for use\n\n"
            else:
                response += f"⚠️ Database verification failed: {verify_result.get('error', 'Unknown error')}\n"
                response += "🔍 Please check database status manually\n\n"
            
            response += "🎉 **Restore Workflow Completed Successfully!**\n"
            response += f"📋 **Summary:**\n"
            response += f"- Backup used: {backup_id}\n"
            response += f"- Database: {self.database_name}\n"
            response += f"- Stanza: {stanza}\n"
            response += f"- Status: Ready for use"
            
            return {
                "response": response,
                "handoff": None,
                "handoff_message": None
            }
            
        except Exception as e:
            response += f"❌ **Unexpected error during restore workflow: {str(e)}**\n"
            response += "🚨 Attempting emergency PostgreSQL restart...\n"
            
            # Emergency restart
            try:
                start_result = self.execute_tool("postgres_start", database_name=self.database_name)
                if start_result.get("status") == "success":
                    response += "✅ Emergency restart successful\n"
                else:
                    response += f"❌ Emergency restart failed: {start_result.get('error', 'Unknown error')}\n"
                    response += "🚨 MANUAL INTERVENTION REQUIRED!\n"
            except:
                response += "🚨 CRITICAL: Cannot restart PostgreSQL - MANUAL INTERVENTION REQUIRED!\n"
            
            return {
                "response": response,
                "handoff": None,
                "handoff_message": None
            }
    
    def pgbackrest_list_backups(self, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """List available pgBackRest backups with detailed information."""
        self.log_action("listing_pgbackrest_backups", {"database": self.database_name})
        
        # Get server-specific configuration
        server_config = settings.get_pgbackrest_config("customerServer")
        stanza = parameters.get("stanza", server_config["stanza"]) if parameters else server_config["stanza"]
        backup_path = parameters.get("backup_path", server_config["backup_path"]) if parameters else server_config["backup_path"]
        server_name = "customerServer"
        
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
                response += f"\n🔄 **Handing off to EmployeeDB_Agent to list backups for employeeServer...**"
                return {
                    "response": response,
                    "handoff": "EmployeeDB_Agent",
                    "handoff_message": f"Customer server backups listed. Please list backups for employeeServer to complete the 'both databases' request."
                }
        else:
            response = f"❌ Error listing pgBackRest backups: {result.get('error', 'Unknown error')}"
        
        return {
            "response": response,
            "handoff": None,
            "handoff_message": None
        }
    
    def pgbackrest_recommended_backups(self, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Compare timestamps of incremental backups from both customer_demo and employee_demo, and recommend the closest matching timestamps using Ollama."""
        self.log_action("getting_recommended_backups", {"database": self.database_name})
        
        # Get server-specific configuration
        server_config = settings.get_pgbackrest_config("customerServer")
        stanza = parameters.get("stanza", server_config["stanza"]) if parameters else server_config["stanza"]
        backup_path = parameters.get("backup_path", server_config["backup_path"]) if parameters else server_config["backup_path"]
        server_name = "customerServer"
        
        result = self.execute_tool("pgbackrest_recommended_backups", 
                                 stanza=stanza, 
                                 backup_path=backup_path,
                                 server_name=server_name)
        
        if result.get("status") == "success":
            recommendations = result.get("recommendations", {})
            customer_backups = result.get("customer_backups", [])
            employee_backups = result.get("employee_backups", [])
            
            response = f"🎯 **Coordinated Backup Recommendations (Customer & Employee Servers)**\n\n"
            
            # Show analysis method
            analysis_method = recommendations.get("analysis_method", "unknown")
            if analysis_method == "ollama_ai":
                response += f"🤖 **Analysis Method:** Ollama AI-powered timestamp matching\n\n"
            else:
                response += f"🔍 **Analysis Method:** Fallback timestamp comparison\n\n"
            
            # Show overall recommendation
            overall_rec = recommendations.get("recommendation", "No recommendation available")
            response += f"💡 **Overall Recommendation:**\n{overall_rec}\n\n"
            
            # Show matching pairs
            matches = recommendations.get("matches", [])
            if matches:
                response += f"🔗 **Best Matching Backup Pairs:**\n\n"
                
                for i, match in enumerate(matches, 1):
                    customer_backup = match.get("customer_backup", "Unknown")
                    employee_backup = match.get("employee_backup", "Unknown")
                    match_quality = match.get("match_quality", "unknown")
                    time_diff = match.get("time_difference", "unknown")
                    recommendation = match.get("recommendation", "")
                    
                    quality_emoji = "🎯" if match_quality == "exact" else "🎪" if match_quality == "close" else "🎈"
                    
                    response += f"**{quality_emoji} Match #{i} ({match_quality.upper()})**\n"
                    response += f"- **Customer Server:** {customer_backup}\n"
                    response += f"- **Employee Server:** {employee_backup}\n"
                    response += f"- **Time Difference:** {time_diff}\n"
                    response += f"- **Why:** {recommendation}\n\n"
            else:
                response += "❌ No matching backup pairs found.\n\n"
            
            # Show backup summaries
            response += f"📊 **Available Backups Summary:**\n"
            response += f"- **Customer Server (customer_demo):** {len(customer_backups)} recent backups\n"
            response += f"- **Employee Server (employee_demo):** {len(employee_backups)} recent backups\n\n"
            
            # Show recent backups for context
            if customer_backups:
                response += f"🔵 **Latest Customer Backups:**\n"
                for backup in customer_backups[:3]:
                    label = backup.get("label", "Unknown")
                    timestamp = backup.get("timestamp", "Unknown")
                    backup_type = backup.get("type", "Unknown")
                    response += f"- {label} ({backup_type}) - {timestamp}\n"
                response += "\n"
            
            if employee_backups:
                response += f"🟢 **Latest Employee Backups:**\n"
                for backup in employee_backups[:3]:
                    label = backup.get("label", "Unknown")
                    timestamp = backup.get("timestamp", "Unknown")
                    backup_type = backup.get("type", "Unknown")
                    response += f"- {label} ({backup_type}) - {timestamp}\n"
                
        else:
            response = f"❌ Failed to get coordinated backup recommendations: {result.get('error', 'Unknown error')}"
        
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
        server_config = settings.get_pgbackrest_config("customerServer")
        stanza = parameters.get("stanza", server_config["stanza"])
        pgdata = parameters.get("pgdata", server_config["pgdata"])
        backup_path = parameters.get("backup_path", server_config["backup_path"])
        backup_id = parameters.get("backup_file") or parameters.get("backup_id") or parameters.get("backup_type")
        target_time = parameters.get("target_time")
        server_name = "customerServer"
        
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
            response += f"\n\nHanding off to EmployeeDB_Agent to perform the same restore for employee database."
            return {
                "response": response,
                "handoff": "EmployeeDB_Agent",
                "handoff_message": f"Customer database restore workflow completed. Please perform similar restore for employee database. Backup ID: {backup_id or 'latest'}, Target Time: {target_time or 'none'}"
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
