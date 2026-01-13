import os
import re
from crewai import Agent, Task, Crew, Process

# Disable LLM requirements
os.environ["CREWAI_DISABLE_LLM"] = "true"
os.environ["OPENAI_API_KEY"] = "DUMMY_KEY"

# Cluster to DB mapping
CLUSTER_DATABASES = {
    "pg1": ["db1", "db2"],
    "pg2": ["db3", "db4"]
}

class SupervisorAgent:
    """
    Supervisor agent that routes requests to the appropriate MCP server
    based on cluster or database mentioned in the user prompt.
    """
    
    def __init__(self, mcp_server1, mcp_server2):
        self.mcp_server1 = mcp_server1
        self.mcp_server2 = mcp_server2
        
        self.agent = Agent(
            name="Backup Supervisor",
            role="PostgreSQL Backup Operations Coordinator",
            goal="Route backup/restore requests to the appropriate PostgreSQL cluster",
            backstory="""You are a supervisor overseeing backup operations for multiple 
            PostgreSQL clusters. You analyze user requests and delegate tasks to the 
            appropriate backup agent (PG1 or PG2) based on the cluster or database mentioned.""",
            llm=None,
            verbose=True
        )
    
    def parse_request(self, user_input: str) -> dict:
        """
        Parse user input to determine:
        - Action: backup, restore, list
        - Target: cluster (pg1/pg2) or database (db1/db2/db3/db4)
        - Additional parameters
        
        Returns:
            dict with parsed information
        """
        user_input_lower = user_input.lower()
        
        # Determine action
        action = None
        if "backup" in user_input_lower:
            action = "backup"
        elif "restore" in user_input_lower:
            action = "restore"
        elif "list" in user_input_lower:
            action = "list"
        
        # Determine backup type
        backup_type = "full"
        if "incremental" in user_input_lower or "incr" in user_input_lower:
            backup_type = "incr"
        
        # Determine if recent restore
        recent = "recent" in user_input_lower
        
        # Extract backup name/label
        backup_name = None
        match = re.search(r'\b(\d{8}-\d{6}[FI]?)\b', user_input)
        if match:
            backup_name = match.group(1)
        
        # Determine target cluster(s) and database(s)
        targets = []
        
        # Check for "both clusters"
        if "both" in user_input_lower and "cluster" in user_input_lower:
            targets.append({"cluster": "pg1", "server": self.mcp_server1})
            targets.append({"cluster": "pg2", "server": self.mcp_server2})
        
        # Check for specific clusters
        elif "pg1" in user_input_lower:
            targets.append({"cluster": "pg1", "server": self.mcp_server1})
        elif "pg2" in user_input_lower:
            targets.append({"cluster": "pg2", "server": self.mcp_server2})
        
        # Check for specific databases
        for cluster, dbs in CLUSTER_DATABASES.items():
            for db in dbs:
                if re.search(rf'\b{db}\b', user_input_lower):
                    server = self.mcp_server1 if cluster == "pg1" else self.mcp_server2
                    targets.append({
                        "cluster": cluster,
                        "database": db,
                        "server": server
                    })
        
        return {
            "action": action,
            "backup_type": backup_type,
            "backup_name": backup_name,
            "recent": recent,
            "targets": targets
        }
    
    def execute(self, user_input: str) -> dict:
        """
        Execute the user request by routing to appropriate MCP server(s).
        
        Args:
            user_input: Natural language user request
        
        Returns:
            dict: Results from each MCP server
        """
        parsed = self.parse_request(user_input)
        
        if not parsed["action"]:
            return {"error": "Could not determine action (backup/restore/list)"}
        
        if not parsed["targets"]:
            return {"error": "Could not determine target cluster or database"}
        
        results = {}
        
        for target in parsed["targets"]:
            server = target["server"]
            cluster = target["cluster"]
            db_name = target.get("database")
            
            key = f"{cluster}_{db_name}" if db_name else cluster
            
            try:
                if parsed["action"] == "backup":
                    result = server.execute(
                        action="backup",
                        backup_type=parsed["backup_type"],
                        db_name=db_name
                    )
                
                elif parsed["action"] == "restore":
                    result = server.execute(
                        action="restore",
                        backup_name=parsed["backup_name"],
                        db_name=db_name,
                        recent=parsed["recent"]
                    )
                
                elif parsed["action"] == "list":
                    result = server.execute(action="list")
                
                results[key] = result
            
            except Exception as e:
                results[key] = f"❌ Error: {str(e)}"
        
        return results