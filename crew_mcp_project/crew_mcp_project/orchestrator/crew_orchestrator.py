from crewai import Crew, Task
from crewai_tools import MCPServerAdapter
from dotenv import load_dotenv
import os

load_dotenv()

SERVER1_HOST = os.getenv("SERVER1_HOST")
SERVER1_MCP_PORT = int(os.getenv("SERVER1_MCP_PORT"))
SERVER2_HOST = os.getenv("SERVER2_HOST")
SERVER2_MCP_PORT = int(os.getenv("SERVER2_MCP_PORT"))

def run_orchestrator():
    mcp_adapter = MCPServerAdapter(
        server_params_list=[
            {"url": f"http://{SERVER1_HOST}:{SERVER1_MCP_PORT}/mcp", "transport": "streamable-http"},
            {"url": f"http://{SERVER2_HOST}:{SERVER2_MCP_PORT}/mcp", "transport": "streamable-http"}
        ]
    )

    agents = mcp_adapter.get_agents()
    agent1, agent2 = agents[0], agents[1]

    task1 = Task(description="Backup DB1", agent=agent1)
    task2 = Task(description="Backup DB2", agent=agent2)

    crew = Crew(agents=[agent1, agent2], tasks=[task1, task2], verbose=True)
    result = crew.kickoff()

    print("Orchestration Result:", result)

if __name__ == "__main__":
    run_orchestrator()
