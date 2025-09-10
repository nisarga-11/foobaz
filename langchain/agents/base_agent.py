#!/usr/bin/env python3
"""
Base Agent with LLM Processing
"""

from typing import Dict, List, Any, Optional
from langchain_ollama import OllamaLLM
from langchain.tools import BaseTool
from langchain.schema import BaseMessage, HumanMessage, AIMessage
from config.settings import settings
from utils.backup_recommendation import BackupRecommendationEngine
from config.logging_config import get_agent_logger, get_backup_logger
import json
import re
import os

class BaseAgent:
    """Base class for database agents with LLM processing capabilities."""
    
    def __init__(self, agent_name: str, database_name: str, tools: List[BaseTool]):
        self.agent_name = agent_name
        self.database_name = database_name
        self.tools = {tool.name: tool for tool in tools}
        
        # Initialize Ollama LLM
        self.llm = OllamaLLM(
            base_url=settings.OLLAMA_BASE_URL,
            model=settings.OLLAMA_MODEL,
            temperature=0.1
        )
        
        # Agent state
        self.conversation_history: List[BaseMessage] = []
        self.current_task: Optional[str] = None
        self.task_results: Dict[str, Any] = {}
        
        # Initialize backup recommendation engine
        self.backup_recommendation_engine = BackupRecommendationEngine()
        
        # Initialize loggers
        self.agent_logger = get_agent_logger()
        self.backup_logger = get_backup_logger()
    
    def add_message(self, message: BaseMessage):
        """Add a message to the conversation history."""
        self.conversation_history.append(message)
    
    def get_conversation_context(self) -> str:
        """Get formatted conversation context for the agent."""
        context = f"Agent: {self.agent_name}\nDatabase: {self.database_name}\n\n"
        
        if self.conversation_history:
            context += "Recent conversation:\n"
            for msg in self.conversation_history[-5:]:  # Last 5 messages
                if isinstance(msg, HumanMessage):
                    context += f"Human: {msg.content}\n"
                elif isinstance(msg, AIMessage):
                    context += f"Agent: {msg.content}\n"
        
        return context
    
    def get_available_tools(self) -> str:
        """Get description of available tools."""
        tool_descriptions = []
        for tool_name, tool in self.tools.items():
            tool_descriptions.append(f"- {tool_name}: {tool.description}")
        
        return "\n".join(tool_descriptions)
    
    def execute_tool(self, tool_name: str, **kwargs) -> Dict[str, Any]:
        """Execute a tool and return the result."""
        if tool_name not in self.tools:
            return {
                "status": "error",
                "message": f"Tool '{tool_name}' not found",
                "error": "Unknown tool"
            }
        
        try:
            tool = self.tools[tool_name]
            # Pass kwargs as individual arguments to the tool
            if kwargs:
                result = tool.run(**kwargs)
            else:
                result = tool.run()
            return result
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error executing tool '{tool_name}'",
                "error": str(e)
            }
    
    def analyze_user_input(self, user_input: str) -> Dict[str, Any]:
        """Analyze user input using Ollama LLM to determine the required action."""
        try:
            # Concise system prompt that returns only JSON
            system_prompt = f"""
            You are a database management assistant. Analyze this user input and return ONLY a JSON response.

            User Input: "{user_input}"

            Available actions: create_backup, list_backups, restore, get_info, handle_corruption, pgbackrest_full_backup, pgbackrest_incremental_backup, pgbackrest_restore, pgbackrest_list_backups, pgbackrest_recommended_backups, pgbackrest_pitr_restore, pgbackrest_pitr_restore_with_workflow, pgbackrest_coordinated_recommendations
            Available databases: customer_db, employee_db, {self.database_name}

            Rules:
            - "backup" → create_backup
            - "restore" → restore (needs backup_file parameter)
            - "restore" + backup_id (format: 20250908-125120F_20250908-130131I) → pgbackrest_pitr_restore_with_workflow
            - "list backups" → list_backups
            - "pgbackrest full backup" → pgbackrest_full_backup
            - "pgbackrest incremental backup" → pgbackrest_incremental_backup
            - "pgbackrest restore" → pgbackrest_restore
            - "pgbackrest restore" + "with stop" OR "stop start" → pgbackrest_pitr_restore_with_workflow
            - "pgbackrest list" OR "pgbackrest list backups" OR "pgbackrest info" → pgbackrest_list_backups
            - "pgbackrest recommended" OR "recommended backups" OR "best backup" → pgbackrest_recommended_backups
            - "coordinated" OR "both servers" OR "matching timestamps" → pgbackrest_coordinated_recommendations
            - "pgbackrest pitr restore" OR "point in time restore" OR "restore to time" → pgbackrest_pitr_restore (needs target_time parameter)
            - "corrupted" → handle_corruption
            - "customer" → customer_db
            - "employee" → employee_db
            - "both databases" or "both the databases" → {self.database_name} with both_databases=true in parameters
            - Extract backup filenames (format: *_db_YYYYMMDDTHHMMSS.tar)
            - Extract backup IDs (format: 20250908-125120F_20250908-130131I)
            - Extract target times (format: YYYY-MM-DD HH:MM:SS+05:30 or similar)
            
            CRITICAL: If user input contains "pgbackrest" anywhere, use the pgbackrest_* actions, not the regular backup actions.

            IMPORTANT: If user mentions "both databases" or "both the databases", set both_databases=true in parameters.

            Return ONLY valid JSON with: action, database, reasoning, parameters
            Example: {{"action": "create_backup", "database": "customer_db", "reasoning": "User wants to create backups of both databases", "parameters": {{"both_databases": true}}}}
            """
            
            # Use Ollama LLM for intelligent input analysis
            response = self.llm.invoke(system_prompt)
            
            # Parse JSON response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                analysis = json.loads(json_match.group())
            else:
                # Fallback to keyword parsing if LLM fails
                return self._fallback_keyword_analysis(user_input)
            
            # Validate and set defaults
            action = analysis.get("action", "unknown")
            database = analysis.get("database", self.database_name)
            reasoning = analysis.get("reasoning", f"LLM analyzed: {user_input}")
            parameters = analysis.get("parameters", {})
            
            # Normalise for keyword checks
            user_input_lower = user_input.lower()
            
            # Enforce the pgBackRest rule: if the user said 'pgbackrest' but
            # the LLM returned a non-pgBackRest action, override with fallback analysis
            if "pgbackrest" in user_input_lower and not action.startswith("pgbackrest"):
                fallback_result = self._fallback_keyword_analysis(user_input)
                self.log_action("llm_pgbackrest_override", {
                    "input": user_input,
                    "llm_action": action,
                    "fallback_action": fallback_result["action"]
                })
                return fallback_result
            
            # Extract backup file if mentioned in reasoning or parameters
            backup_file_match = re.search(r'(\w+_db_\d{8}T\d{6}\.tar)', user_input)
            if backup_file_match and "backup_file" not in parameters:
                parameters["backup_file"] = backup_file_match.group(1)
            
            # Extract timestamp if mentioned
            timestamp_match = re.search(r'(\d{8}T\d{6})', user_input)
            if timestamp_match and "timestamp" not in parameters:
                parameters["timestamp"] = timestamp_match.group(1)
            
            # Check for "both databases" requests
            if any(phrase in user_input_lower for phrase in ["both databases", "both the databases", "both db", "both dbs"]):
                parameters["both_databases"] = True
            
            self.log_action("llm_analysis", {
                "input": user_input,
                "llm_response": response,
                "parsed_action": action,
                "parsed_database": database,
                "parsed_parameters": parameters
            })
            
            return {
                "action": action,
                "database": database,
                "reasoning": reasoning,
                "parameters": parameters
            }
            
        except Exception as e:
            self.log_action("llm_analysis_error", {
                "input": user_input,
                "error": str(e)
            })
            
            # Fallback to keyword parsing if LLM fails
            return self._fallback_keyword_analysis(user_input)
    
    def _fallback_keyword_analysis(self, user_input: str) -> Dict[str, Any]:
        """Fallback keyword-based analysis if LLM fails."""
        user_input_lower = user_input.lower()
        
        # Determine action - check for pgbackrest first
        action = "unknown"
        
        # Check for backup ID patterns to determine restore type
        backup_id_match = re.search(r'(\d{8}-\d{6}[FD]_\d{8}-\d{6}[I])', user_input)
        
        if "pgbackrest" in user_input_lower:
            # pgBackRest specific actions
            if "full" in user_input_lower and "backup" in user_input_lower:
                action = "pgbackrest_full_backup"
            elif "incremental" in user_input_lower and "backup" in user_input_lower:
                action = "pgbackrest_incremental_backup"
            elif "coordinated" in user_input_lower or "both servers" in user_input_lower or "matching timestamps" in user_input_lower:
                action = "pgbackrest_coordinated_recommendations"
            elif "pitr" in user_input_lower or "point in time" in user_input_lower or "restore to time" in user_input_lower:
                action = "pgbackrest_pitr_restore"
            elif "restore" in user_input_lower and ("with stop" in user_input_lower or "stop start" in user_input_lower or "workflow" in user_input_lower):
                action = "pgbackrest_pitr_restore_with_workflow"
            elif "restore" in user_input_lower and backup_id_match:
                action = "pgbackrest_pitr_restore_with_workflow"  # Backup ID implies workflow restore
            elif "restore" in user_input_lower:
                action = "pgbackrest_restore"
            elif "list" in user_input_lower or "info" in user_input_lower:
                action = "pgbackrest_list_backups"
            elif "recommended" in user_input_lower or "best" in user_input_lower:
                action = "pgbackrest_recommended_backups"
            elif "backup" in user_input_lower:
                action = "pgbackrest_full_backup"  # Default to full backup
        elif "restore" in user_input_lower and backup_id_match:
            # Any restore with backup ID should use workflow restore
            action = "pgbackrest_pitr_restore_with_workflow"
        elif "coordinated" in user_input_lower or ("both" in user_input_lower and "server" in user_input_lower):
            action = "pgbackrest_coordinated_recommendations"
        elif "corrupt" in user_input_lower:
            action = "handle_corruption"
        elif "backup" in user_input_lower and ("create" in user_input_lower or "make" in user_input_lower):
            action = "create_backup"
        elif "backup" in user_input_lower and ("want" in user_input_lower or "need" in user_input_lower):
            action = "create_backup"
        elif "backup" in user_input_lower and not ("list" in user_input_lower or "show" in user_input_lower):
            action = "create_backup"
        elif "list" in user_input_lower and "backup" in user_input_lower:
            action = "list_backups"
        elif "restore" in user_input_lower:
            action = "restore"
        elif "info" in user_input_lower or "information" in user_input_lower:
            action = "get_info"
        
        # Determine database
        database = self.database_name
        if "customer" in user_input_lower or "customer_db" in user_input_lower:
            database = "customer_db"
        elif "employee" in user_input_lower or "employee_db" in user_input_lower:
            database = "employee_db"
        
        # Check for "both databases" or "all databases" requests
        both_databases = False
        if any(word in user_input_lower for word in ["both", "all", "each", "every"]):
            both_databases = True
        
        # Extract parameters
        parameters = {}
        
        # Look for pgBackRest backup IDs (format: 20250907-144732F_20250907-145745I)
        pgbackrest_backup_match = re.search(r'(\d{8}-\d{6}[FD]_\d{8}-\d{6}[I])', user_input)
        if pgbackrest_backup_match:
            parameters["backup_type"] = pgbackrest_backup_match.group(1)
            parameters["backup_id"] = pgbackrest_backup_match.group(1)
        
        # Look for backup filenames
        backup_file_match = re.search(r'(\w+_db_\d{8}T\d{6}\.tar)', user_input)
        if backup_file_match:
            parameters["backup_file"] = backup_file_match.group(1)
        
        # Look for timestamps
        timestamp_match = re.search(r'(\d{8}T\d{6})', user_input)
        if timestamp_match:
            parameters["timestamp"] = timestamp_match.group(1)
        
        # Look for target times for PITR restore (format: YYYY-MM-DD HH:MM:SS+05:30)
        target_time_match = re.search(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2})', user_input)
        if target_time_match:
            parameters["target_time"] = target_time_match.group(1)
        
        # Add both_databases flag to parameters
        parameters["both_databases"] = both_databases
        
        reasoning = f"Fallback keyword parsing: '{user_input}' as {action} for {database}"
        
        return {
            "action": action,
            "database": database,
            "reasoning": reasoning,
            "parameters": parameters
        }
    
    def format_response(self, message: str, handoff_to: Optional[str] = None) -> str:
        """Format agent response using LLM for more natural language."""
        try:
            # Use LLM to enhance the response
            response_prompt = f"""
            You are a helpful database management assistant. Format this technical response to be more natural and user-friendly.

            Original response: "{message}"
            Agent name: {self.agent_name}
            Handoff target: {handoff_to if handoff_to else "None"}

            Make the response more conversational and helpful while keeping all the technical information.
            If there's a handoff, explain what the next agent will do in a friendly way.

            Guidelines:
            - Be conversational but professional
            - Explain technical terms in simple language
            - Provide clear next steps when applicable
            - Make handoffs sound like helpful coordination, not just passing the buck

            Return only the formatted response:
            """
            
            enhanced_response = self.llm.invoke(response_prompt)
            
            self.log_action("llm_response_formatting", {
                "original": message,
                "enhanced": enhanced_response
            })
            
            return enhanced_response
            
        except Exception as e:
            # Fallback to original formatting
            self.log_action("llm_response_formatting_error", {
                "error": str(e),
                "fallback": True
            })
            
            response = f"{self.agent_name}: {message}"
            
            if handoff_to:
                response += f"\n{self.agent_name} -> {handoff_to}: "
            
            return response
    
    def log_action(self, action: str, details: Dict[str, Any]):
        """Log an action for debugging and tracking."""
        log_message = f"[{self.agent_name}] {action}: {details}"
        print(log_message)  # Keep console output for immediate feedback
        self.agent_logger.info(log_message)  # Also log to file
    
    def get_related_databases(self) -> List[str]:
        """Get list of related databases for this agent."""
        return settings.get_related_databases(self.database_name)
    
    def should_handoff(self, task_type: str, database_mentioned: str, parameters: Dict[str, Any] = None) -> bool:
        """Determine if task should be handed off to another agent."""
        related_dbs = self.get_related_databases()
        
        # For "both databases" requests, don't handoff immediately
        # Let the agent complete its task first, then handoff in the task method
        if parameters and parameters.get("both_databases", False):
            return False
        
        # Never handoff for simple operations (list, backup, restore, get_info)
        if task_type in ["list_backups", "create_backup", "restore", "get_info"]:
            return False
        
        # Only handoff if:
        # 1. Task involves a different database than this agent's
        # 2. Task requires coordination between databases (corruption checks)
        
        # If the task is for a different database, handoff
        if database_mentioned != self.database_name:
            return True
        
        # Only handoff for corruption handling that needs coordination
        if task_type in ["handle_corruption"] and related_dbs:
            return True
        
        return False
    
    def should_process_handoff(self, message: str, from_agent: str) -> bool:
        """Determine if this agent should process a handoff message."""
        # If the message mentions this agent's database, process it
        if self.database_name in message.lower():
            return True
        
        # If the message mentions corruption and this agent has related databases, process it
        if "corrupted" in message.lower() or "corruption" in message.lower():
            return True
        
        return False
    
    def handle_corruption(self, database: str, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Handle database corruption by providing recovery options using LLM recommendation."""
        self.log_action("handling_corruption", {"database": database})
        
        # Use LLM to recommend the best backup for recovery
        recommendation_result = self.recommend_backup_restore(database, parameters)
        
        if recommendation_result["status"] == "success":
            recommendation = recommendation_result["recommendation"]
            backup_files = [b["filename"] for b in recommendation_result["available_backups"]]
            
            response = f"Database {database} corruption detected. Recovery options available:\n"
            response += f"Available backup files: {backup_files}\n\n"
            response += f"LLM RECOMMENDATION:\n"
            response += f"- Recommended backup: {recommendation['recommended_backup']}\n"
            response += f"- Reasoning: {recommendation['reasoning']}\n"
            response += f"- Confidence: {recommendation['confidence']}\n"
            response += f"- Restore command: {recommendation['restore_command']}"
            
            if recommendation.get('alternative_options'):
                response += f"\n- Alternative options: {', '.join(recommendation['alternative_options'])}"
            
            # Handoff to coordinate with related databases
            handoff_message = f"{database} is corrupted. Please check related databases for potential impact and coordinate recovery."
            
            return {
                "response": response,
                "handoff": "EmployeeDB_Agent" if database == "customer_db" else "CustomerDB_Agent",
                "handoff_message": handoff_message
            }
        elif recommendation_result["status"] == "no_backups":
            response = f"Database {database} is corrupted but no backup files found. Manual intervention required."
            return {
                "response": response,
                "handoff": None,
                "handoff_message": None
            }
        else:
            response = f"Database {database} is corrupted. Error accessing backup files: {recommendation_result.get('error', 'Unknown error')}"
            return {
                "response": response,
                "handoff": None,
                "handoff_message": None
            }

    def recommend_backup_restore(self, database: str, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Use Ollama LLM to recommend the best backup for restore operations."""
        self.log_action("recommending_backup", {"database": database})
        
        # Get backup files for the specified database
        backup_result = self.execute_tool("list_backups", database_name=database)
        
        if backup_result["status"] != "success":
            return {
                "status": "error",
                "message": f"Error retrieving backup information for {database}",
                "error": backup_result.get("error", "Unknown error")
            }
        
        backup_files = backup_result["backups"]
        
        # If no backup files available
        if not backup_files:
            return {
                "status": "no_backups",
                "message": f"No backup files found for {database}",
                "recommendation": None
            }
        
        # Prepare backup information for LLM analysis
        backup_info = []
        for backup in backup_files:
            backup_info.append({
                "filename": backup["filename"],
                "file_path": backup["file_path"],
                "size_mb": backup.get("size_mb", 0),
                "timestamp": backup.get("timestamp", "Unknown")
            })
        
        # Create prompt for LLM to recommend the best backup
        recommendation_prompt = f"""
        You are a database backup expert. Analyze the available backup files and recommend the best one for restore.

        Database: {database}
        Available backup files:
        {json.dumps(backup_info, indent=2)}

        User context: {parameters.get('user_context', 'General restore request')}
        Specific timestamp requested: {parameters.get('timestamp', 'None')}
        Specific backup file requested: {parameters.get('backup_file', 'None')}

        Consider these factors when recommending:
        1. If user specified a timestamp or backup file, prioritize that
        2. For general restore requests, recommend the most recent backup
        3. Consider backup file size and completeness
        4. If multiple databases are involved, consider data consistency

        Return ONLY a JSON response with:
        {{
            "recommended_backup": "filename.tar",
            "reasoning": "Why this backup is recommended",
            "confidence": "high/medium/low",
            "alternative_options": ["list of other good options"],
            "restore_command": "suggested restore command"
        }}
        """
        
        try:
            # Use Ollama LLM for intelligent backup recommendation
            response = self.llm.invoke(recommendation_prompt)
            
            # Parse JSON response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                recommendation = json.loads(json_match.group())
                
                self.log_action("llm_backup_recommendation", {
                    "database": database,
                    "llm_response": response,
                    "parsed_recommendation": recommendation
                })
                
                return {
                    "status": "success",
                    "recommendation": recommendation,
                    "available_backups": backup_files
                }
            else:
                # Fallback to simple logic if LLM fails
                return self._fallback_backup_recommendation(backup_files, parameters)
                
        except Exception as e:
            self.log_action("llm_backup_recommendation_error", {
                "database": database,
                "error": str(e)
            })
            
            # Fallback to simple logic if LLM fails
            return self._fallback_backup_recommendation(backup_files, parameters)
    
    def _fallback_backup_recommendation(self, backup_files: List[Dict], parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Fallback backup recommendation logic if LLM fails."""
        if not backup_files:
            return {
                "status": "no_backups",
                "message": "No backup files available",
                "recommendation": None
            }
        
        # Check if user specified a specific backup file
        requested_backup = parameters.get("backup_file")
        if requested_backup:
            for backup in backup_files:
                if backup["filename"] == requested_backup:
                    return {
                        "status": "success",
                        "recommendation": {
                            "recommended_backup": backup["filename"],
                            "reasoning": f"User specifically requested {requested_backup}",
                            "confidence": "high",
                            "alternative_options": [b["filename"] for b in backup_files if b["filename"] != requested_backup],
                            "restore_command": f"restore from {backup['filename']}"
                        },
                        "available_backups": backup_files
                    }
        
        # Check if user specified a timestamp
        requested_timestamp = parameters.get("timestamp")
        if requested_timestamp:
            for backup in backup_files:
                if requested_timestamp in backup["filename"]:
                    return {
                        "status": "success",
                        "recommendation": {
                            "recommended_backup": backup["filename"],
                            "reasoning": f"User requested timestamp {requested_timestamp}",
                            "confidence": "high",
                            "alternative_options": [b["filename"] for b in backup_files if b["filename"] != backup["filename"]],
                            "restore_command": f"restore from {backup['filename']}"
                        },
                        "available_backups": backup_files
                    }
        
        # Default to most recent backup
        most_recent = backup_files[-1]  # Assuming backups are sorted by timestamp
        return {
            "status": "success",
            "recommendation": {
                "recommended_backup": most_recent["filename"],
                "reasoning": "Most recent backup available",
                "confidence": "medium",
                "alternative_options": [b["filename"] for b in backup_files[:-1]],
                "restore_command": f"restore from {most_recent['filename']}"
            },
            "available_backups": backup_files
        }
    
    def recommend_coordinated_backup_restore(self, databases: List[str], parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Use Ollama LLM to recommend coordinated backup restore across multiple databases."""
        self.log_action("recommending_coordinated_backup", {"databases": databases})
        
        # Get backup files for all databases
        all_backup_data = {}
        for database in databases:
            # Check if this is a pgBackRest request by looking at the action context
            if hasattr(self, 'current_task') and 'pgbackrest' in self.current_task:
                # Use pgBackRest list backups for server-specific backups
                server_name = "customerServer" if database == "customer_db" else "employeeServer"
                server_config = settings.get_pgbackrest_config(server_name)
                
                backup_result = self.execute_tool("pgbackrest_list_backups", 
                                                stanza=server_config["stanza"],
                                                backup_path=server_config["backup_path"],
                                                server_name=server_name)
                if backup_result["status"] == "success":
                    all_backup_data[database] = backup_result["backups"]
                else:
                    return {
                        "status": "error",
                        "message": f"Error retrieving pgBackRest backup information for {database} on {server_name}",
                        "error": backup_result.get("error", "Unknown error")
                    }
            else:
                # Use traditional backup listing
                backup_result = self.execute_tool("list_backups", database_name=database)
                if backup_result["status"] == "success":
                    all_backup_data[database] = backup_result["backups"]
                else:
                    return {
                        "status": "error",
                        "message": f"Error retrieving backup information for {database}",
                        "error": backup_result.get("error", "Unknown error")
                    }
        
        # Prepare comprehensive backup information for LLM analysis
        backup_analysis = {}
        for database, backups in all_backup_data.items():
            backup_analysis[database] = []
            for backup in backups:
                backup_analysis[database].append({
                    "filename": backup["filename"],
                    "file_path": backup["file_path"],
                    "size_mb": backup.get("size_mb", 0),
                    "timestamp": backup.get("timestamp", "Unknown")
                })
        
        # Create prompt for LLM to recommend coordinated backup restore
        coordination_prompt = f"""
        You are a database backup coordination expert. Analyze backup files across multiple databases and recommend the best coordinated restore strategy.

        Databases: {databases}
        Available backup files:
        {json.dumps(backup_analysis, indent=2)}

        User context: {parameters.get('user_context', 'Coordinated restore request')}
        Specific timestamp requested: {parameters.get('timestamp', 'None')}

        Consider these factors when recommending:
        1. Data consistency across databases (matching timestamps are preferred)
        2. If no exact matches, find the closest timestamps
        3. Consider the most recent backups if no specific requirements
        4. Ensure all databases have valid backup files
        5. Provide clear reasoning for the recommendation

        Return ONLY a JSON response with:
        {{
            "recommended_strategy": "matching_timestamps/closest_timestamps/most_recent",
            "recommended_backups": {{
                "database_name": "backup_filename.tar"
            }},
            "reasoning": "Why this strategy is recommended",
            "confidence": "high/medium/low",
            "restore_commands": ["list of restore commands for each database"],
            "warnings": ["any warnings about data consistency or timing differences"]
        }}
        """
        
        try:
            # Use Ollama LLM for intelligent coordinated backup recommendation
            response = self.llm.invoke(coordination_prompt)
            
            # Parse JSON response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                recommendation = json.loads(json_match.group())
                
                self.log_action("llm_coordinated_backup_recommendation", {
                    "databases": databases,
                    "llm_response": response,
                    "parsed_recommendation": recommendation
                })
                
                return {
                    "status": "success",
                    "recommendation": recommendation,
                    "available_backups": all_backup_data
                }
            else:
                # Fallback to simple logic if LLM fails
                return self._fallback_coordinated_backup_recommendation(all_backup_data, parameters)
                
        except Exception as e:
            self.log_action("llm_coordinated_backup_recommendation_error", {
                "databases": databases,
                "error": str(e)
            })
            
            # Fallback to simple logic if LLM fails
            return self._fallback_coordinated_backup_recommendation(all_backup_data, parameters)
    
    def _fallback_coordinated_backup_recommendation(self, all_backup_data: Dict[str, List[Dict]], parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Fallback coordinated backup recommendation logic if LLM fails."""
        from utils.helpers import find_matching_timestamps, find_closest_timestamps, find_matching_pgbackrest_timestamps, find_closest_pgbackrest_timestamps
        
        databases = list(all_backup_data.keys())
        if len(databases) != 2:
            return {
                "status": "error",
                "message": "Coordinated backup restore currently supports exactly 2 databases",
                "recommendation": None
            }
        
        db1, db2 = databases
        
        # Check if these are pgBackRest backups (have server_name field)
        is_pgbackrest = any(b.get("server_name") for b in all_backup_data[db1] + all_backup_data[db2])
        
        if is_pgbackrest:
            # Use pgBackRest timestamp matching
            customer_backups = all_backup_data.get("customer_db", [])
            employee_backups = all_backup_data.get("employee_db", [])
            
            # Try to find matching timestamps first
            matching_pairs = find_matching_pgbackrest_timestamps(customer_backups, employee_backups)
            
            if matching_pairs:
                # Found matching timestamps
                best_pair = matching_pairs[0]  # Take the first (most recent) matching pair
                customer_backup = best_pair["customer_backup"]
                employee_backup = best_pair["employee_backup"]
                
                return {
                    "status": "success",
                    "recommendation": {
                        "recommended_strategy": "matching_timestamps",
                        "recommended_backups": {
                            "customer_db": customer_backup["backup_id"],
                            "employee_db": employee_backup["backup_id"]
                        },
                        "reasoning": f"Found pgBackRest backups with matching timestamps (diff: {best_pair['timestamp_diff_seconds']:.1f}s) for data consistency",
                        "confidence": "high",
                        "restore_commands": [
                            f"restore customer_db from {customer_backup['backup_id']}",
                            f"restore employee_db from {employee_backup['backup_id']}"
                        ],
                        "warnings": [],
                        "timestamp_info": {
                            "customer_timestamp": best_pair["customer_timestamp"],
                            "employee_timestamp": best_pair["employee_timestamp"],
                            "time_difference_seconds": best_pair["timestamp_diff_seconds"]
                        }
                    },
                    "available_backups": all_backup_data
                }
            else:
                # No matching timestamps, find closest
                closest_pair = find_closest_pgbackrest_timestamps(customer_backups, employee_backups)
                
                if closest_pair:
                    customer_backup = closest_pair["customer_backup"]
                    employee_backup = closest_pair["employee_backup"]
                    
                    return {
                        "status": "success",
                        "recommendation": {
                            "recommended_strategy": "closest_timestamps",
                            "recommended_backups": {
                                "customer_db": customer_backup["backup_id"],
                                "employee_db": employee_backup["backup_id"]
                            },
                            "reasoning": f"No exact matching timestamps found, using closest available pgBackRest backups (diff: {closest_pair['timestamp_diff_seconds']:.1f}s)",
                            "confidence": "medium",
                            "restore_commands": [
                                f"restore customer_db from {customer_backup['backup_id']}",
                                f"restore employee_db from {employee_backup['backup_id']}"
                            ],
                            "warnings": [f"These backups have a timestamp difference of {closest_pair['timestamp_diff_seconds']:.1f} seconds"],
                            "timestamp_info": {
                                "customer_timestamp": closest_pair["customer_timestamp"],
                                "employee_timestamp": closest_pair["employee_timestamp"],
                                "time_difference_seconds": closest_pair["timestamp_diff_seconds"]
                            }
                        },
                        "available_backups": all_backup_data
                    }
                else:
                    return {
                        "status": "error",
                        "message": "Unable to find suitable pgBackRest backup pairs for coordinated restore",
                        "recommendation": None
                    }
        else:
            # Use traditional file-based backup matching
            db1_files = [b["file_path"] for b in all_backup_data[db1]]
            db2_files = [b["file_path"] for b in all_backup_data[db2]]
            
            # Try to find matching timestamps first
            matching_pairs = find_matching_timestamps(db1_files, db2_files)
            
            if matching_pairs:
                # Found matching timestamps
                best_pair = matching_pairs[0]  # Take the first (most recent) matching pair
                db1_file, db2_file = best_pair
                
                return {
                    "status": "success",
                    "recommendation": {
                        "recommended_strategy": "matching_timestamps",
                        "recommended_backups": {
                            db1: os.path.basename(db1_file),
                            db2: os.path.basename(db2_file)
                        },
                        "reasoning": "Found backup files with matching timestamps for data consistency",
                        "confidence": "high",
                        "restore_commands": [
                            f"restore {db1} from {os.path.basename(db1_file)}",
                            f"restore {db2} from {os.path.basename(db2_file)}"
                        ],
                        "warnings": []
                    },
                    "available_backups": all_backup_data
                }
            else:
                # No matching timestamps, find closest
                closest_db1, closest_db2 = find_closest_timestamps(db1_files, db2_files)
                
                if closest_db1 and closest_db2:
                    return {
                        "status": "success",
                        "recommendation": {
                            "recommended_strategy": "closest_timestamps",
                            "recommended_backups": {
                                db1: os.path.basename(closest_db1),
                                db2: os.path.basename(closest_db2)
                            },
                            "reasoning": "No exact matching timestamps found, using closest available backups",
                            "confidence": "medium",
                            "restore_commands": [
                                f"restore {db1} from {os.path.basename(closest_db1)}",
                                f"restore {db2} from {os.path.basename(closest_db2)}"
                            ],
                            "warnings": ["These backups may have slight timestamp differences"]
                        },
                        "available_backups": all_backup_data
                    }
                else:
                    return {
                        "status": "error",
                        "message": "Unable to find suitable backup pairs for coordinated restore",
                        "recommendation": None
                    }
