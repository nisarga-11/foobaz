import os
import threading
import time
import json
from rich import print as rprint
from rich.json import JSON
from rich.console import Console

# Disable LLM requirements
os.environ["CREWAI_DISABLE_LLM"] = "true"
os.environ["OPENAI_API_KEY"] = "DUMMY_KEY"
os.environ["MODEL"] = "none"
os.environ["OPENAI_MODEL_NAME"] = "none"

from servers.mcp_server1 import mcp_server1
from servers.mcp_server2 import mcp_server2
from supervisor import SupervisorAgent

console = Console()

# ----------------------------
# Start MCP servers in background
# ----------------------------
def start_server(server):
    """Start MCP server in background thread"""
    server.start()

print("🚀 Starting MCP servers...")
threading.Thread(target=start_server, args=(mcp_server1,), daemon=True).start()
threading.Thread(target=start_server, args=(mcp_server2,), daemon=True).start()
time.sleep(2)
print("✅ Both MCP servers running with CrewAI tools.\n")

# ----------------------------
# Create supervisor
# ----------------------------
supervisor = SupervisorAgent(mcp_server1, mcp_server2)

# ----------------------------
# Main orchestrator loop
# ----------------------------
def orchestrator():
    """
    Main orchestrator that accepts natural language commands and
    uses the supervisor to route them to appropriate MCP servers.
    """
    
    console.print("\n[bold cyan]🚀 CrewAI MCP Backup Orchestrator[/bold cyan]\n", style="bold")
    
    console.print("Available commands:", style="bold yellow")
    console.print("  • 'backup pg1' - Full backup of PG1 cluster")
    console.print("  • 'backup pg1 incremental' - Incremental backup of PG1")
    console.print("  • 'backup db1' - Backup specific database")
    console.print("  • 'backup both clusters' - Backup both PG1 and PG2")
    console.print("  • 'restore pg1 recent' - Restore latest backup")
    console.print("  • 'restore pg2 from 20251106-143022F' - Restore specific backup")
    console.print("  • 'restore db2 recent' - Restore latest DB backup")
    console.print("  • 'list pg1' - List all backups for PG1")
    console.print("  • 'exit' - Quit\n")
    
    while True:
        try:
            user_input = console.input("[bold green]You:[/bold green] ").strip()
        except KeyboardInterrupt:
            console.print("\n[yellow]👋 Exiting orchestrator...[/yellow]")
            break
        
        if not user_input:
            continue
        
        if user_input.lower() in ["exit", "quit"]:
            console.print("[yellow]👋 Goodbye![/yellow]")
            break
        
        # Execute through supervisor
        console.print("\n[bold cyan]🔍 Analyzing request...[/bold cyan]")
        
        try:
            results = supervisor.execute(user_input)
            
            # Pretty print results
            console.print("\n[bold green]✅ Results:[/bold green]")
            
            if isinstance(results, dict) and "error" in results:
                console.print(f"[red]{results['error']}[/red]")
            else:
                try:
                    rprint(JSON(json.dumps(results, indent=2, default=str)))
                except Exception:
                    rprint(results)
            
            console.print()
        
        except Exception as e:
            console.print(f"[red]❌ Error: {str(e)}[/red]\n")

# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    try:
        orchestrator()
    except KeyboardInterrupt:
        console.print("\n[yellow]👋 Shutting down...[/yellow]")
    except Exception as e:
        console.print(f"[red]❌ Fatal error: {str(e)}[/red]")