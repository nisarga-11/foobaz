import os
from crewai import Agent, Crew, Process
from agents.tools.backup_tool import backup_tool
from agents.tools.restore_tool import restore_tool
from agents.tools.list_tool import list_backups_tool

# Disable LLM requirements
os.environ["CREWAI_DISABLE_LLM"] = "true"
os.environ["OPENAI_API_KEY"] = "DUMMY_KEY"

class MCPServer1:
    def __init__(self):
        self.name = "mcp_server1"
        self.host = "0.0.0.0"
        self.port = 5001
        self.stanza = "pg1_17"
        self.pg_path = "/var/lib/postgresql/17/pg1_17"

        # CrewAI Agent for PG1
        self.agent = Agent(
            name="PG1 Backup Agent",
            role="PostgreSQL Backup and Restore Specialist for PG1",
            goal="Manage backups and restores for PostgreSQL Server 1 (pg1_17)",
            backstory=(
                "You handle full and incremental backups, restore operations, "
                "and provide backup listings using pgBackRest."
            ),
            tools=[backup_tool, restore_tool, list_backups_tool],
            llm=None,
            verbose=False
        )

        self.crew = Crew(
            agents=[self.agent],
            tasks=[],
            process=Process.sequential,
            verbose=False
        )

    def start(self):
        """Start the MCP server (simulated CrewAI server start)."""
        try:
            print(f"🚀 {self.name} starting on port {self.port} for stanza '{self.stanza}'...")
            # In a real setup, you'd bind to socket or register service here
        except Exception as e:
            print(f"❌ Failed to start {self.name}: {e}")

    def execute(self, action: str, **kwargs) -> str:
        """Execute backup, restore, or list operations."""
        try:
            if action == "backup":
                return backup_tool(
                    stanza=self.stanza,
                    pg_path=self.pg_path,
                    backup_type=kwargs.get("backup_type", "full"),
                    db_name=kwargs.get("db_name")
                )

            elif action == "restore":
                return restore_tool(
                    stanza=self.stanza,
                    pg_path=self.pg_path,
                    backup_name=kwargs.get("backup_name"),
                    db_name=kwargs.get("db_name"),
                    recent=kwargs.get("recent", False)
                )

            elif action == "list":
                return list_backups_tool(
                    stanza=self.stanza,
                    pg_path=self.pg_path
                )

            else:
                return f"❌ Unknown action: {action}"

        except Exception as e:
            return f"❌ Error executing {action}: {e}"

# Singleton instance
mcp_server1 = MCPServer1()
