"""Interactive CLI for PostgreSQL backup/restore orchestration."""

import logging
import os
import sys
from typing import Dict, List, Optional, Any

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table

from graph import BackupOrchestrator, create_orchestrator_from_env

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

app = typer.Typer(help="PostgreSQL Backup/Restore Orchestration CLI")
console = Console()


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
            console.print(f"❌ Failed to initialize CLI: {e}", style="red")
            raise typer.Exit(1)

    def display_banner(self):
        """Display welcome banner."""
        banner = """
[bold blue]PostgreSQL Backup/Restore Orchestrator[/bold blue]
[dim]Multi-server backup automation using LangChain + LangGraph + Ollama[/dim]

Servers: PG1 ({}) | PG2 ({})
Model: {}
""".format(
            os.getenv("MCP1_BASE_URL", "Not configured"),
            os.getenv("MCP2_BASE_URL", "Not configured"),
            os.getenv("OLLAMA_MODEL", "Not configured")
        )
        
        console.print(Panel(banner, title="🚀 SP Lakehouse Backup", border_style="blue"))

    def display_help(self):
        """Display help information."""
        help_table = Table(title="Available Commands", show_header=True, header_style="bold magenta")
        help_table.add_column("Command", style="cyan", width=25)
        help_table.add_column("Description", style="white")

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
            help_table.add_row(command, description)

        console.print(help_table)
        console.print("\n[dim]💡 Tip: You can use natural language! The Supervisor agent will understand your intent.[/dim]")

    def format_output(self, result: dict) -> None:
        """Format and display execution results."""
        if result["success"]:
            # Display successful output
            console.print(Panel(
                result["output"],
                title="✅ Execution Result",
                border_style="green"
            ))
            
            # Show detailed state information if available
            state = result.get("state", {})
            if state:
                self._display_state_details(state)
                
        else:
            # Display error
            console.print(Panel(
                f"❌ Error: {result.get('error', 'Unknown error')}",
                title="Execution Failed",
                border_style="red"
            ))

    def _display_state_details(self, state: dict) -> None:
        """Display detailed state information."""
        intent = state.get("intent")
        target_servers = state.get("target_servers", [])
        databases = state.get("databases", [])
        
        if intent or target_servers or databases:
            details_table = Table(title="Operation Details", show_header=False)
            details_table.add_column("Property", style="cyan")
            details_table.add_column("Value", style="white")
            
            if intent:
                details_table.add_row("Intent", intent)
            if target_servers:
                details_table.add_row("Target Servers", ", ".join(target_servers))
            if databases:
                details_table.add_row("Databases", ", ".join(databases))
            
            console.print(details_table)

        # Display restore plan if available
        restore_plan = state.get("restore_plan")
        if restore_plan and "summary" in restore_plan:
            console.print(Panel(
                restore_plan["summary"],
                title="📋 Restore Plan",
                border_style="yellow"
            ))

    def _handle_restore_confirmation(self, user_input: str) -> bool:
        """Handle restore operation confirmation."""
        if "restore" in user_input.lower():
            return Confirm.ask("🔄 This will restore database(s). Are you sure you want to proceed?")
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
        
        console.print("[green]🎯 Ready! Type your backup/restore commands or 'help' for examples.[/green]\n")
        
        while True:
            try:
                # Get user input
                user_input = Prompt.ask("[bold cyan]backup>[/bold cyan]").strip()
                
                # Handle special commands
                if user_input.lower() in ["quit", "exit", "q"]:
                    console.print("👋 Goodbye!")
                    break
                
                if user_input.lower() in ["help", "h", "?"]:
                    self.display_help()
                    continue
                
                if not user_input:
                    continue

                # Handle restore confirmation
                if not self._handle_restore_confirmation(user_input):
                    console.print("[yellow]❌ Restore operation cancelled.[/yellow]")
                    continue

                # Add to conversation history
                self.conversation_history.append({"role": "user", "content": user_input})
                
                # Show processing indicator
                with console.status("[bold green]Processing request...") as status:
                    result = self.orchestrator.execute(user_input, self.conversation_history)
                
                # Display results
                self.format_output(result)
                
                # Add assistant response to history
                if result["success"]:
                    self.conversation_history.append({
                        "role": "assistant", 
                        "content": result["output"]
                    })
                
                console.print()  # Add spacing
                
            except KeyboardInterrupt:
                console.print("\n👋 Goodbye!")
                break
            except Exception as e:
                console.print(f"❌ Unexpected error: {e}", style="red")


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
            console.print("Non-interactive mode not yet implemented. Use --interactive.")
            
    except Exception as e:
        console.print(f"❌ Failed to start CLI: {e}", style="red")
        raise typer.Exit(1)


@app.command()
def test_connection():
    """Test connection to MCP servers and Ollama."""
    console.print("🔧 Testing connections...\n")
    
    # Test Ollama
    try:
        from llm.ollama_helper import test_ollama_connection
        if test_ollama_connection():
            console.print("✅ Ollama: Connected", style="green")
        else:
            console.print("❌ Ollama: Connection failed", style="red")
    except Exception as e:
        console.print(f"❌ Ollama: Error - {e}", style="red")
    
    # Test MCP servers
    for server_name, url_env in [("MCP1", "MCP1_BASE_URL"), ("MCP2", "MCP2_BASE_URL")]:
        url = os.getenv(url_env)
        if not url:
            console.print(f"❌ {server_name}: No URL configured", style="red")
            continue
            
        try:
            from mcp_client import SyncMCPClient
            api_key = os.getenv(f"{server_name}_API_KEY")
            
            client = SyncMCPClient(url, api_key)
            health_result = client.health_check()
            console.print(f"✅ {server_name}: Connected ({url})", style="green")
            
        except Exception as e:
            console.print(f"❌ {server_name}: Connection failed - {e}", style="red")


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
    console.print(f"🚀 Executing command: [bold cyan]{command}[/bold cyan]\n")
    
    try:
        cli = BackupCLI(mcp1_base_url, mcp2_base_url)
        
        # Execute the command directly
        result = cli.execute_command_direct(command)
        
        if result.get("success"):
            console.print("✅ Command completed successfully")
            console.print(f"📋 Result: {result.get('message', 'No details available')}")
        else:
            console.print("❌ Command failed")
            console.print(f"🔥 Error: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        console.print(f"❌ CLI Error: {e}", style="red")
        raise typer.Exit(1)

@app.command()
def example():
    """Show example commands."""
    console.print("📚 Example Commands:\n")
    
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
        console.print(f"[bold cyan]{category}:[/bold cyan]")
        for cmd in commands:
            console.print(f"  • {cmd}")
        console.print()


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
