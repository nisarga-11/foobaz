"""LangGraph implementation for multi-server PostgreSQL backup/restore orchestration."""

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple, TypedDict

from dotenv import load_dotenv
from langgraph.graph import END, StateGraph
from langchain.schema import HumanMessage, AIMessage

from agents.backup_agent_pg1 import BackupAgentPG1, create_pg1_agent_from_env
from agents.backup_agent_pg2 import BackupAgentPG2, create_pg2_agent_from_env
from llm.ollama_helper import create_supervisor_llm
from supervisor.restore_planner import create_restore_plan, group_selections_by_server

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class GraphState(TypedDict):
    """State for the LangGraph workflow."""
    
    # Input
    user_input: str
    
    # Supervisor analysis
    intent: Optional[str]  # "list_backups", "full_backup", "incremental_backup", "enable_schedules", "restore"
    target_servers: List[str]  # ["PG1", "PG2"]
    databases: List[str]  # Requested database names
    target_timestamp: Optional[str]  # For restore operations
    backup_id: Optional[str]  # Specific backup ID for restore
    
    # Backup data collection
    backup_data: Dict[str, Any]  # Results from list_backups calls
    
    # Restore planning
    restore_plan: Optional[Dict[str, Any]]  # Generated restore plan
    user_confirmed: Optional[bool]  # User confirmation for restore
    
    # Execution results
    pg1_results: List[Dict[str, Any]]
    pg2_results: List[Dict[str, Any]]
    
    # Final output
    final_output: str
    
    # Messages for conversation history
    messages: List[Dict[str, str]]


class BackupOrchestrator:
    """Main orchestrator using LangGraph for backup/restore operations."""

    def __init__(
        self,
        mcp1_base_url: str = None,
        mcp1_api_key: str = None,
        mcp2_base_url: str = None,
        mcp2_api_key: str = None,
    ):
        """
        Initialize the backup orchestrator.

        Args:
            mcp1_base_url: Base URL for MCP1 server
            mcp1_api_key: API key for MCP1 server  
            mcp2_base_url: Base URL for MCP2 server
            mcp2_api_key: API key for MCP2 server
        """
        # Load from environment if not provided
        self.mcp1_base_url = mcp1_base_url or os.getenv("MCP1_BASE_URL")
        self.mcp1_api_key = mcp1_api_key or os.getenv("MCP1_API_KEY")
        self.mcp2_base_url = mcp2_base_url or os.getenv("MCP2_BASE_URL")
        self.mcp2_api_key = mcp2_api_key or os.getenv("MCP2_API_KEY")

        if not self.mcp1_base_url or not self.mcp2_base_url:
            raise ValueError("MCP1_BASE_URL and MCP2_BASE_URL are required")

        # Create supervisor LLM
        self.supervisor_llm = create_supervisor_llm()

        # Create backup agents
        self.pg1_agent = BackupAgentPG1(self.mcp1_base_url, self.mcp1_api_key)
        self.pg2_agent = BackupAgentPG2(self.mcp2_base_url, self.mcp2_api_key)

        # Load supervisor system prompt
        with open("supervisor/system_prompt.txt", "r") as f:
            self.supervisor_prompt = f.read().strip()

        # Create the graph
        self.graph = self._create_graph()

    def _create_graph(self) -> StateGraph:
        """Create the LangGraph workflow."""
        workflow = StateGraph(GraphState)

        # Add nodes
        workflow.add_node("supervisor", self._supervisor_node)
        workflow.add_node("collect_backup_data", self._collect_backup_data_node)
        workflow.add_node("create_restore_plan", self._create_restore_plan_node)
        workflow.add_node("backup_pg1", self._backup_pg1_node)
        workflow.add_node("backup_pg2", self._backup_pg2_node)
        workflow.add_node("finalize", self._finalize_node)

        # Set entry point
        workflow.set_entry_point("supervisor")

        # Add conditional edges from supervisor
        workflow.add_conditional_edges(
            "supervisor",
            self._route_from_supervisor,
            {
                "collect_backup_data": "collect_backup_data",
                "pg1_only": "backup_pg1",
                "pg2_only": "backup_pg2",
                "both_servers": "backup_pg1",  # Will route to pg2 after pg1
                "end": END,
            }
        )

        # Add edges from backup data collection
        workflow.add_edge("collect_backup_data", "create_restore_plan")
        workflow.add_edge("create_restore_plan", "backup_pg1")

        # Add conditional edges from backup agents
        workflow.add_conditional_edges(
            "backup_pg1",
            self._route_from_pg1,
            {
                "pg2": "backup_pg2",
                "finalize": "finalize",
            }
        )

        workflow.add_edge("backup_pg2", "finalize")
        workflow.add_edge("finalize", END)

        return workflow.compile()

    def _supervisor_node(self, state: GraphState) -> GraphState:
        """Supervisor node for intent detection and routing decisions."""
        user_input = state["user_input"]
        
        # Create supervisor prompt
        messages = [
            {"role": "system", "content": self.supervisor_prompt},
            {"role": "user", "content": user_input}
        ]
        
        # Add conversation history
        for msg in state.get("messages", []):
            messages.append({"role": msg["role"], "content": msg["content"]})
        
        # Get supervisor analysis
        prompt_text = f"""
        Analyze this user request for PostgreSQL backup/restore operations: "{user_input}"
        
        Determine:
        1. Intent: list_backups, full_backup, incremental_backup, enable_schedules, or restore
        2. Target servers: PG1, PG2, or both  
        3. Database names mentioned (if any)
        4. Target timestamp for restore (if any)
        5. Backup ID if specifically mentioned (e.g., "inventory_db_wal_20250913_202646")
        
        Respond in JSON format:
        {{
            "intent": "...",
            "target_servers": ["PG1", "PG2"],
            "databases": ["db1", "db2"],
            "target_timestamp": "2025-09-10T10:30:00Z or null",
            "backup_id": "backup_id_if_mentioned or null"
        }}
        """
        
        try:
            response = self.supervisor_llm.invoke(prompt_text)
            
            # Get response content - handle both string and object responses
            if hasattr(response, 'content'):
                response_content = response.content
            else:
                response_content = str(response)
            
            logger.info(f"Raw Ollama response: {response_content[:500]}...")
            
            # Clean and parse JSON response
            analysis = self._parse_llm_json_response(response_content, user_input)
            
            state["intent"] = analysis.get("intent")
            state["databases"] = analysis.get("databases", [])
            state["target_timestamp"] = analysis.get("target_timestamp")
            state["backup_id"] = analysis.get("backup_id")
            
            # Smart server determination - if backup_id is provided, determine correct server
            backup_id = analysis.get("backup_id")
            databases = analysis.get("databases", [])
            if backup_id and databases:
                state["target_servers"] = self._determine_server_from_backup_id(backup_id, databases)
            else:
                state["target_servers"] = analysis.get("target_servers", [])
            
            logger.info(f"Supervisor analysis: {analysis}")
            if backup_id and databases:
                logger.info(f"Smart server detection: {backup_id} -> {state['target_servers']}")
            
        except Exception as e:
            logger.error(f"Supervisor analysis failed: {e}")
            # Fallback to simple parsing
            state["intent"] = self._parse_intent_fallback(user_input)
            state["databases"] = self._parse_databases_fallback(user_input)
            state["target_timestamp"] = self._parse_timestamp_fallback(user_input)
            state["backup_id"] = self._parse_backup_id_fallback(user_input)
            
            # Smart server determination for fallback too
            backup_id = state.get("backup_id")
            databases = state.get("databases", [])
            if backup_id and databases:
                state["target_servers"] = self._determine_server_from_backup_id(backup_id, databases)
            else:
                state["target_servers"] = self._parse_servers_fallback(user_input)

        return state

    def _parse_llm_json_response(self, response_content: str, user_input: str) -> Dict[str, Any]:
        """Parse LLM JSON response with robust error handling."""
        if not response_content or response_content.strip() == "":
            logger.warning("Empty response from LLM, using fallback parsing")
            return self._create_fallback_analysis(user_input)
        
        # Try to extract JSON from response
        try:
            # First, try direct JSON parsing
            return json.loads(response_content)
        except json.JSONDecodeError:
            pass
        
        # Try to find JSON block in response
        try:
            # Look for JSON between triple backticks
            if "```json" in response_content:
                json_start = response_content.find("```json") + 7
                json_end = response_content.find("```", json_start)
                json_str = response_content[json_start:json_end].strip()
                return json.loads(json_str)
            
            # Look for JSON between curly braces
            start_idx = response_content.find('{')
            if start_idx != -1:
                end_idx = response_content.rfind('}')
                if end_idx != -1 and end_idx > start_idx:
                    json_str = response_content[start_idx:end_idx + 1]
                    return json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON from LLM response: {e}")
        
        # If all JSON parsing fails, use fallback
        logger.warning("Could not parse JSON from LLM response, using fallback analysis")
        return self._create_fallback_analysis(user_input)
    
    def _create_fallback_analysis(self, user_input: str) -> Dict[str, Any]:
        """Create fallback analysis when LLM JSON parsing fails."""
        return {
            "intent": self._parse_intent_fallback(user_input),
            "target_servers": self._parse_servers_fallback(user_input),
            "databases": self._parse_databases_fallback(user_input),
            "target_timestamp": self._parse_timestamp_fallback(user_input)
        }

    def _parse_intent_fallback(self, user_input: str) -> str:
        """Fallback intent parsing using simple text matching."""
        user_input_lower = user_input.lower()
        
        if "restore" in user_input_lower:
            return "restore_database"
        elif "list" in user_input_lower and "backup" in user_input_lower:
            return "list_backups"
        elif ("create" in user_input_lower or "make" in user_input_lower or "trigger" in user_input_lower) and "backup" in user_input_lower:
            return "create_backup"
        elif "health" in user_input_lower or "status" in user_input_lower:
            return "health_check"
        else:
            return "list_backups"  # Default

    def _parse_servers_fallback(self, user_input: str) -> List[str]:
        """Fallback server parsing with database-to-server mapping."""
        user_input_lower = user_input.lower()
        
        # Database to server mapping
        pg1_databases = ["customer_db", "inventory_db", "analytics_db", "customer", "inventory", "analytics"]
        pg2_databases = ["hr_db", "finance_db", "reporting_db", "hr", "finance", "reporting"]
        
        # Check for specific server mentions first
        servers = []
        if "pg1" in user_input_lower or "mcp1" in user_input_lower:
            servers.append("PG1")
        if "pg2" in user_input_lower or "mcp2" in user_input_lower:
            servers.append("PG2")
        
        # If servers already found from explicit mentions, return them
        if servers:
            return servers
        
        # Map databases to their correct servers (only include servers that have the mentioned databases)
        servers = []
        if any(db in user_input_lower for db in pg1_databases):
            servers.append("PG1")
        if any(db in user_input_lower for db in pg2_databases):
            servers.append("PG2")
        
        # If no database-specific mapping found, default to both (for "all databases" queries)
        if not servers:
            servers = ["PG1", "PG2"]
        
        return servers
    
    def _determine_server_from_backup_id(self, backup_id: str, databases: List[str]) -> List[str]:
        """Determine which server should handle the backup based on backup_id and database names."""
        if not backup_id or not databases:
            return ["PG1", "PG2"]  # Fallback to both
        
        # Database to server mapping
        pg1_databases = ["customer_db", "inventory_db", "analytics_db"]
        pg2_databases = ["hr_db", "finance_db", "reporting_db"]
        
        for db_name in databases:
            if db_name in pg1_databases:
                return ["PG1"]
            elif db_name in pg2_databases:
                return ["PG2"]
        
        # If we can't determine from database name, try to infer from backup_id
        # Backup IDs typically start with database name
        for db_name in pg1_databases:
            if backup_id.startswith(db_name):
                return ["PG1"]
        
        for db_name in pg2_databases:
            if backup_id.startswith(db_name):
                return ["PG2"]
        
        return ["PG1", "PG2"]  # Fallback to both if can't determine

    def _parse_databases_fallback(self, user_input: str) -> List[str]:
        """Fallback database parsing."""
        # Simple regex to find potential database names
        db_pattern = r'\b(\w+_db|\w+db)\b'
        matches = re.findall(db_pattern, user_input, re.IGNORECASE)
        return list(set(matches))

    def _parse_timestamp_fallback(self, user_input: str) -> Optional[str]:
        """Fallback timestamp parsing."""
        # Look for ISO timestamp patterns
        timestamp_pattern = r'\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2})?[Z]?\b'
        matches = re.findall(timestamp_pattern, user_input)
        return matches[0] if matches else None
    
    def _parse_backup_id_fallback(self, user_input: str) -> Optional[str]:
        """Fallback backup ID parsing."""
        # Look for backup ID patterns (database_name_wal_YYYYMMDD_HHMMSS or database_name_base_YYYYMMDD_HHMMSS)
        backup_id_pattern = r'\b(\w+_(?:wal|base)_\d{8}_\d{6})\b'
        matches = re.findall(backup_id_pattern, user_input)
        return matches[0] if matches else None

    def _collect_backup_data_node(self, state: GraphState) -> GraphState:
        """Collect backup data from both servers for restore planning."""
        backup_data = {}
        
        databases = state.get("databases", [])
        target_servers = state.get("target_servers", ["PG1", "PG2"])
        
        # If no specific databases mentioned, try common databases from both servers
        if not databases:
            # Based on POSTGRES_SETUP.md, these are our actual databases
            databases = [
                "customer_db", "inventory_db", "product_db",  # PG1 databases
                "hr_db", "finance_db", "reporting_db"         # PG2 databases
            ]
        
        logger.info(f"Collecting backup data for databases: {databases} on servers: {target_servers}")
        
        for server in target_servers:
            backup_data[server] = {}
            agent = self.pg1_agent if server == "PG1" else self.pg2_agent
            
            for db_name in databases:
                try:
                    logger.info(f"Listing backups for {db_name} on {server}")
                    result = agent.list_backups(db_name)
                    if result["success"]:
                        # Parse the result to extract backup list
                        # The JSON is in the intermediate steps, not the final output
                        backup_list = []
                        
                        # Check intermediate steps for raw MCP tool output
                        intermediate_steps = result.get("intermediate_steps", [])
                        for step in intermediate_steps:
                            if isinstance(step, tuple) and len(step) >= 2:
                                action, observation = step[0], step[1]
                                # Look for MCP tool results that contain JSON
                                if hasattr(observation, 'strip'):
                                    backup_list = self._extract_backup_list(observation)
                                    if backup_list:  # Found valid backup data
                                        break
                                elif isinstance(observation, str):
                                    backup_list = self._extract_backup_list(observation)
                                    if backup_list:  # Found valid backup data
                                        break
                        
                        # If no backup list found in intermediate steps, try the final output
                        if not backup_list:
                            output = result["output"]
                            backup_list = self._extract_backup_list(output)
                        
                        backup_data[server][db_name] = backup_list
                        logger.info(f"Found {len(backup_list)} backups for {db_name} on {server}")
                    else:
                        logger.warning(f"Failed to list backups for {db_name} on {server}: {result.get('error')}")
                        backup_data[server][db_name] = []
                except Exception as e:
                    logger.error(f"Error collecting backup data for {db_name} on {server}: {e}")
                    backup_data[server][db_name] = []
        
        state["backup_data"] = backup_data
        return state

    def _extract_backup_list(self, mcp_output: str) -> List[Dict[str, Any]]:
        """Extract backup list from MCP tool output."""
        try:
            logger.info(f"Extracting backup list from output (length: {len(mcp_output)})")
            logger.debug(f"Output preview: {mcp_output[:300]}...")
            
            # Super simple approach - find first { and last } 
            start = mcp_output.find('{')
            end = mcp_output.rfind('}')
            
            if start != -1 and end != -1 and end > start:
                json_str = mcp_output[start:end+1]
                logger.info(f"Extracted JSON string (length: {len(json_str)})")
                logger.debug(f"JSON preview: {json_str[:200]}...")
                
                try:
                    data = json.loads(json_str)
                    logger.info(f"Successfully parsed JSON: {type(data)}")
                    
                    if isinstance(data, dict) and "backups" in data:
                        backups = data["backups"]
                        logger.info(f"Found {len(backups)} backups in JSON object")
                        return backups
                    elif isinstance(data, list):
                        logger.info(f"Found {len(data)} backups in JSON array")
                        return data
                    else:
                        logger.warning(f"JSON data is {type(data)} but doesn't contain 'backups' key")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"JSON parse error: {e}")
                    logger.debug(f"Failed JSON string: {json_str}")
            else:
                logger.warning("No JSON braces found in output")
            
            logger.warning("No valid JSON backup data found")
            return []
            
        except Exception as e:
            logger.error(f"Exception in backup extraction: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return []

    def _create_restore_plan_node(self, state: GraphState) -> GraphState:
        """Create restore plan using backup data."""
        backup_data = state.get("backup_data", {})
        target_timestamp = state.get("target_timestamp")
        
        logger.info(f"Creating restore plan with backup_data: {backup_data}")
        logger.info(f"Target timestamp: {target_timestamp}")
        
        # Transform backup data to expected format
        backups_by_db = {}
        for server, server_data in backup_data.items():
            logger.info(f"Processing server {server} with data: {server_data}")
            for db_name, backups in server_data.items():
                logger.info(f"Processing database {db_name} with {len(backups) if backups else 0} backups")
                if backups:  # Only include DBs with available backups
                    backups_by_db[(server, db_name)] = backups
        
        logger.info(f"Final backups_by_db: {backups_by_db}")
        
        if backups_by_db:
            try:
                plan = create_restore_plan(backups_by_db, target_timestamp)
                state["restore_plan"] = {
                    "summary": plan.display_summary(),
                    "selections": [selection.dict() for selection in plan.selections]
                }
                logger.info(f"Successfully created restore plan with {len(plan.selections)} selections")
            except Exception as e:
                logger.error(f"Failed to create restore plan: {e}")
                state["restore_plan"] = {"error": str(e)}
        else:
            logger.error("No backup data available for restore planning")
            state["restore_plan"] = {"error": "No backup data available"}
        
        return state

    def _backup_pg1_node(self, state: GraphState) -> GraphState:
        """Execute operations on PG1 server."""
        results = []
        intent = state.get("intent")
        target_servers = state.get("target_servers", [])
        
        if "PG1" not in target_servers:
            state["pg1_results"] = results
            return state
        
        databases = state.get("databases", [])
        
        try:
            if intent == "restore":
                # Check if we have a specific backup_id or need to use restore plan
                backup_id = state.get("backup_id")
                databases = state.get("databases", [])
                
                if backup_id and databases:
                    # Direct restore with specified backup_id
                    for db_name in databases:
                        result = self.pg1_agent.restore_database(
                            db_name=db_name,
                            backup_id=backup_id
                        )
                        results.append(result)
                else:
                    # Execute restore operations based on plan
                    restore_plan = state.get("restore_plan", {})
                    selections = restore_plan.get("selections", [])
                    
                    for selection in selections:
                        if selection["server"] == "PG1":
                            result = self.pg1_agent.restore_database(
                                db_name=selection["db_name"],
                                backup_id=selection["backup_id"]
                            )
                            results.append(result)
            
            elif intent == "full_backup":
                # If no specific databases mentioned, backup all known databases for this server
                if not databases:
                    databases = ["customer_db", "inventory_db", "analytics_db"]
                
                for db_name in databases:
                    result = self.pg1_agent.trigger_full_backup(db_name)
                    results.append(result)
            
            elif intent == "incremental_backup":
                # If no specific databases mentioned, backup all known databases for this server
                if not databases:
                    databases = ["customer_db", "inventory_db", "analytics_db"]
                
                for db_name in databases:
                    result = self.pg1_agent.trigger_incremental_backup(db_name)
                    results.append(result)
            
            elif intent == "enable_schedules":
                result = self.pg1_agent.enable_schedules()
                results.append(result)
            
            elif intent == "list_backups":
                # If no specific databases mentioned, list all known databases for this server
                if not databases:
                    # PG1 databases based on setup
                    databases = ["customer_db", "inventory_db", "analytics_db"]
                
                for db_name in databases:
                    result = self.pg1_agent.list_backups(db_name)
                    results.append(result)
        
        except Exception as e:
            logger.error(f"Error in PG1 operations: {e}")
            results.append({"success": False, "error": str(e), "server": "PG1"})
        
        state["pg1_results"] = results
        return state

    def _backup_pg2_node(self, state: GraphState) -> GraphState:
        """Execute operations on PG2 server."""
        results = []
        intent = state.get("intent")
        target_servers = state.get("target_servers", [])
        
        if "PG2" not in target_servers:
            state["pg2_results"] = results
            return state
        
        databases = state.get("databases", [])
        
        try:
            if intent == "restore":
                # Check if we have a specific backup_id or need to use restore plan
                backup_id = state.get("backup_id")
                databases = state.get("databases", [])
                
                if backup_id and databases:
                    # Direct restore with specified backup_id
                    for db_name in databases:
                        result = self.pg2_agent.restore_database(
                            db_name=db_name,
                            backup_id=backup_id
                        )
                        results.append(result)
                else:
                    # Execute restore operations based on plan
                    restore_plan = state.get("restore_plan", {})
                    selections = restore_plan.get("selections", [])
                    
                    for selection in selections:
                        if selection["server"] == "PG2":
                            result = self.pg2_agent.restore_database(
                                db_name=selection["db_name"],
                                backup_id=selection["backup_id"]
                            )
                            results.append(result)
            
            elif intent == "full_backup":
                # If no specific databases mentioned, backup all known databases for this server
                if not databases:
                    databases = ["hr_db", "finance_db", "reporting_db"]
                
                for db_name in databases:
                    result = self.pg2_agent.trigger_full_backup(db_name)
                    results.append(result)
            
            elif intent == "incremental_backup":
                # If no specific databases mentioned, backup all known databases for this server
                if not databases:
                    databases = ["hr_db", "finance_db", "reporting_db"]
                
                for db_name in databases:
                    result = self.pg2_agent.trigger_incremental_backup(db_name)
                    results.append(result)
            
            elif intent == "enable_schedules":
                result = self.pg2_agent.enable_schedules()
                results.append(result)
            
            elif intent == "list_backups":
                # If no specific databases mentioned, list all known databases for this server
                if not databases:
                    # PG2 databases based on setup
                    databases = ["hr_db", "finance_db", "reporting_db"]
                
                for db_name in databases:
                    result = self.pg2_agent.list_backups(db_name)
                    results.append(result)
        
        except Exception as e:
            logger.error(f"Error in PG2 operations: {e}")
            results.append({"success": False, "error": str(e), "server": "PG2"})
        
        state["pg2_results"] = results
        return state

    def _finalize_node(self, state: GraphState) -> GraphState:
        """Finalize the workflow and prepare final output."""
        intent = state.get("intent")
        pg1_results = state.get("pg1_results", [])
        pg2_results = state.get("pg2_results", [])
        restore_plan = state.get("restore_plan")
        
        output_parts = []
        
        # Add restore plan if applicable
        if intent == "restore" and restore_plan:
            if "error" in restore_plan:
                output_parts.append(f"Restore planning failed: {restore_plan['error']}")
            else:
                output_parts.append("Restore Plan:")
                output_parts.append(restore_plan["summary"])
                output_parts.append("")
        
        # Summarize results
        all_results = pg1_results + pg2_results
        successful = [r for r in all_results if r.get("success")]
        failed = [r for r in all_results if not r.get("success")]
        
        output_parts.append(f"Completed {len(successful)} operations successfully")
        if failed:
            output_parts.append(f"{len(failed)} operations failed")
            for result in failed:
                output_parts.append(f"   - {result.get('server', 'Unknown')}: {result.get('error', 'Unknown error')}")
        
        state["final_output"] = "\n".join(output_parts)
        return state

    def _route_from_supervisor(self, state: GraphState) -> str:
        """Route from supervisor based on intent and target servers."""
        intent = state.get("intent")
        target_servers = state.get("target_servers", [])
        backup_id = state.get("backup_id")
        
        # For restore operations, skip backup data collection if specific backup_id is provided
        if intent == "restore":
            if backup_id:
                # Skip backup data collection and go directly to execution
                if "PG1" in target_servers and "PG2" in target_servers:
                    return "both_servers"
                elif "PG1" in target_servers:
                    return "pg1_only"
                elif "PG2" in target_servers:
                    return "pg2_only"
                else:
                    return "end"
            else:
                # No specific backup_id, need to collect backup data for planning
                return "collect_backup_data"
        
        # For other operations, route directly to appropriate servers
        if "PG1" in target_servers and "PG2" in target_servers:
            return "both_servers"
        elif "PG1" in target_servers:
            return "pg1_only"
        elif "PG2" in target_servers:
            return "pg2_only"
        else:
            return "end"

    def _route_from_pg1(self, state: GraphState) -> str:
        """Route from PG1 based on target servers."""
        target_servers = state.get("target_servers", [])
        
        if "PG2" in target_servers:
            return "pg2"
        else:
            return "finalize"

    def execute(self, user_input: str, conversation_history: List[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Execute a user request through the LangGraph workflow.

        Args:
            user_input: User's natural language request
            conversation_history: Previous conversation messages

        Returns:
            Execution result with final output and state
        """
        initial_state = {
            "user_input": user_input,
            "messages": conversation_history or [],
            "intent": None,
            "target_servers": [],
            "databases": [],
            "target_timestamp": None,
            "backup_id": None,
            "backup_data": {},
            "restore_plan": None,
            "user_confirmed": None,
            "pg1_results": [],
            "pg2_results": [],
            "final_output": "",
        }

        try:
            final_state = self.graph.invoke(initial_state)
            
            return {
                "success": True,
                "output": final_state["final_output"],
                "state": final_state
            }
        except Exception as e:
            logger.error(f"Graph execution failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "output": f"Failed to execute request: {e}"
            }


def create_orchestrator_from_env() -> BackupOrchestrator:
    """Create orchestrator using environment variables."""
    return BackupOrchestrator()


if __name__ == "__main__":
    # Test the orchestrator
    orchestrator = create_orchestrator_from_env()
    
    test_requests = [
        "list backups for customer_db",
        "enable incremental every 2 minutes and weekly full backups",
        "restore customer_db to 2025-09-10T10:30:00Z",
        "trigger full backup for all databases"
    ]
    
    for request in test_requests:
        print(f"\nTesting: {request}")
        result = orchestrator.execute(request)
        print(result["output"])
