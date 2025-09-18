"""Interactive CLI for PostgreSQL backup/restore orchestration."""

import logging
import os
import sys
from typing import Dict, List, Optional, Any

import typer
from dotenv import load_dotenv

from graph import BackupOrchestrator, create_orchestrator_from_env

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

app = typer.Typer(help="PostgreSQL Backup/Restore Orchestration CLI")


class BackupCLI:
    """Interactive CLI for backup/restore operations."""

    def __init__(
        self,
        mcp1_base_url: Optional[str] = None,
        mcp2_base_url: Optional[str] = None,
    ):
        """Initialize the CLI."""
        try:
            # Override URLs if provided
            if mcp1_base_url:
                os.environ["MCP1_BASE_URL"] = mcp1_base_url
            if mcp2_base_url:
                os.environ["MCP2_BASE_URL"] = mcp2_base_url

            self.orchestrator = create_orchestrator_from_env()
            self.conversation_history = []
            
        except Exception as e:
            print(f"Failed to initialize CLI: {e}")
            raise typer.Exit(1)

    def display_banner(self):
        """Display welcome banner."""
        banner = """
PostgreSQL Backup/Restore Orchestrator
Multi-server backup automation using LangChain + LangGraph + Ollama

Servers: PG1 ({}) | PG2 ({})
Model: {}
""".format(
            os.getenv("MCP1_BASE_URL", "Not configured"),
            os.getenv("MCP2_BASE_URL", "Not configured"),
            os.getenv("OLLAMA_MODEL", "Not configured")
        )
        
        print("SP Lakehouse Backup")
        print(banner)

    def display_help(self):
        """Display help information."""
        print("Available Commands:")
        print("=" * 50)
        
        examples = [
            ("list backups for customer_db", "List available backups for a specific database"),
            ("list backups for all databases", "List backups across all databases on both servers"),
            ("trigger full backup for hr_db", "Start a full backup for the specified database"),
            ("trigger incremental backup for customer_db", "Start an incremental backup"),
            ("enable schedules", "Enable automatic backup schedules (2min incremental, weekly full)"),
            ("restore customer_db to latest", "Restore database to the most recent backup"),
            ("restore hr_db to 2025-09-10T10:30Z", "Restore to a specific timestamp"),
            ("restore all databases to 2025-09-10", "Restore all DBs to closest backups around that date"),
            ("health check", "Check health status of both MCP servers"),
            ("help", "Show this help message"),
            ("quit or exit", "Exit the CLI"),
        ]

        for command, description in examples:
            print(f"{command:<35} - {description}")
        
        print("\nTip: You can use natural language! The Supervisor agent will understand your intent.")

    def format_output(self, result: dict) -> None:
        """Format and display execution results."""
        if result["success"]:
            # Display successful output
            print("Execution Result:")
            print("-" * 20)
            print(result["output"])
            
            # Show detailed state information if available
            state = result.get("state", {})
            if state:
                self._display_state_details(state)
                
        else:
            # Display error
            print("Execution Failed:")
            print("-" * 20)
            print(f"Error: {result.get('error', 'Unknown error')}")

    def _display_state_details(self, state: dict) -> None:
        """Display detailed state information."""
        intent = state.get("intent")
        target_servers = state.get("target_servers", [])
        databases = state.get("databases", [])
        
        if intent or target_servers or databases:
            print("\nOperation Details:")
            print("-" * 20)
            
            if intent:
                print(f"Intent: {intent}")
            if target_servers:
                print(f"Target Servers: {', '.join(target_servers)}")
            if databases:
                print(f"Databases: {', '.join(databases)}")

        # Display restore plan if available
        restore_plan = state.get("restore_plan")
        if restore_plan and "summary" in restore_plan:
            print("\nRestore Plan:")
            print("-" * 15)
            print(restore_plan["summary"])

    def _handle_restore_confirmation(self, user_input: str) -> bool:
        """Handle restore operation confirmation."""
        if "restore" in user_input.lower():
            response = input("This will restore database(s). Are you sure you want to proceed? (y/N): ")
            return response.lower() in ['y', 'yes']
        return True

    def execute_command_direct(self, command: str) -> Dict[str, Any]:
        """Execute a command directly without interactive prompts."""
        try:
            # Use the orchestrator to process the command
            initial_state = {
                "user_input": command,
                "conversation_history": []
            }
            
            # Execute the workflow
            final_state = self.orchestrator.graph.invoke(initial_state)
            
            # Format the result using the actual state structure
            pg1_results = final_state.get("pg1_results", [])
            pg2_results = final_state.get("pg2_results", [])
            all_operations = pg1_results + pg2_results
            success_count = sum(1 for op in all_operations if op.get("success", False))
            
            if all_operations:
                return {
                    "success": success_count > 0,
                    "message": f"Completed {success_count}/{len(all_operations)} operations successfully",
                    "operations": all_operations,
                    "intent": final_state.get("intent"),
                    "target_servers": final_state.get("target_servers"),
                    "databases": final_state.get("databases")
                }
            else:
                return {
                    "success": False,
                    "error": "No operations were executed",
                    "intent": final_state.get("intent"),
                    "target_servers": final_state.get("target_servers"),
                    "databases": final_state.get("databases")
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "command": command
            }

    def run_interactive(self):
        """Run interactive REPL."""
        self.display_banner()
        
        print("Ready! Type your backup/restore commands or 'help' for examples.\n")
        
        while True:
            try:
                # Get user input
                user_input = input("backup> ").strip()
                
                # Handle special commands
                if user_input.lower() in ["quit", "exit", "q"]:
                    print("Goodbye!")
                    break
                
                if user_input.lower() in ["help", "h", "?"]:
                    self.display_help()
                    continue
                
                if not user_input:
                    continue

                # Handle restore confirmation
                if not self._handle_restore_confirmation(user_input):
                    print("Restore operation cancelled.")
                    continue

                # Add to conversation history
                self.conversation_history.append({"role": "user", "content": user_input})
                
                # Show processing indicator
                print("Processing request...")
                result = self.orchestrator.execute(user_input, self.conversation_history)
                
                # Display results
                self.format_output(result)
                
                # Add assistant response to history
                if result["success"]:
                    self.conversation_history.append({
                        "role": "assistant", 
                        "content": result["output"]
                    })
                
                print()  # Add spacing
                
            except KeyboardInterrupt:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"Unexpected error: {e}")


@app.command()
def run(
    mcp1_base_url: Optional[str] = typer.Option(
        None, 
        "--mcp1-base-url", 
        help="Override MCP1 base URL"
    ),
    mcp2_base_url: Optional[str] = typer.Option(
        None,
        "--mcp2-base-url", 
        help="Override MCP2 base URL"
    ),
    interactive: bool = typer.Option(
        True,
        "--interactive/--no-interactive",
        help="Run in interactive mode"
    ),
):
    """Run the backup/restore orchestrator."""
    try:
        cli = BackupCLI(mcp1_base_url, mcp2_base_url)
        
        if interactive:
            cli.run_interactive()
        else:
            print("Non-interactive mode not yet implemented. Use --interactive.")
            
    except Exception as e:
        print(f"Failed to start CLI: {e}")
        raise typer.Exit(1)


@app.command()
def test_connection():
    """Test connection to MCP servers and Ollama."""
    print("Testing connections...\n")
    
    # Test Ollama
    try:
        from llm.ollama_helper import test_ollama_connection
        if test_ollama_connection():
            print("Ollama: Connected")
        else:
            print("Ollama: Connection failed")
    except Exception as e:
        print(f"Ollama: Error - {e}")
    
    # Test MCP servers
    for server_name, url_env in [("MCP1", "MCP1_BASE_URL"), ("MCP2", "MCP2_BASE_URL")]:
        url = os.getenv(url_env)
        if not url:
            print(f"{server_name}: No URL configured")
            continue
            
        try:
            if url.startswith("http"):
                # Use HTTP client for running servers
                from mcp_local.mcp_http_client import SyncMCPHTTPClient
                client = SyncMCPHTTPClient(base_url=url)
                health_result = client.health_check()
                print(f"{server_name}: Connected via HTTP ({url})")
            else:
                # Use stdio client (spawn server process)
                from mcp_local.mcp_client import SyncMCPClient
                client = SyncMCPClient(server_name=server_name.replace("MCP", "PG"))
                health_result = client.health_check()
                print(f"{server_name}: Connected via stdio")
            
        except Exception as e:
            print(f"{server_name}: Connection failed - {e}")


@app.command() 
def query(
    command: str = typer.Argument(..., help="Command to execute"),
    mcp1_base_url: Optional[str] = typer.Option(
        None, 
        "--mcp1-base-url", 
        help="Override MCP1 base URL"
    ),
    mcp2_base_url: Optional[str] = typer.Option(
        None,
        "--mcp2-base-url", 
        help="Override MCP2 base URL"
    ),
):
    """Execute a single backup/restore command non-interactively."""
    print(f"Executing command: {command}\n")
    
    try:
        cli = BackupCLI(mcp1_base_url, mcp2_base_url)
        
        # Execute the command directly
        result = cli.execute_command_direct(command)
        
        if result.get("success"):
            print("Command completed successfully")
            print(f"Result: {result.get('message', 'No details available')}")
        else:
            print("Command failed")
            print(f"Error: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"CLI Error: {e}")
        raise typer.Exit(1)

@app.command()
def example():
    """Show example commands."""
    print("Example Commands:\n")
    
    examples = [
        ("Basic Operations", [
            "list backups for customer_db",
            "trigger full backup for hr_db on pg1",
            "enable schedules on both servers"
        ]),
        ("Restore Operations", [
            "restore customer_db to latest backup",
            "restore hr_db to 2025-09-10T10:30:00Z",
            "restore all databases to 2025-09-10"
        ]),
        ("Cross-Server Operations", [
            "list backups for all databases",
            "enable incremental every 2 minutes and weekly full backups",
            "restore everything to the latest safe point"
        ])
    ]
    
    for category, commands in examples:
        print(f"{category}:")
        for cmd in commands:
            print(f"  - {cmd}")
        print()


@app.callback()
def main():
    """PostgreSQL Backup/Restore Orchestration using LangChain + LangGraph + Ollama."""
    pass


if __name__ == "__main__":
    app()


# Direct run support for python -m cli run
def main_entry():
    """Entry point for python -m cli run."""
    if len(sys.argv) == 1:
        # Default to interactive run
        sys.argv.append("run")
    app()


if __name__ == "__main__":
    main_entry()
