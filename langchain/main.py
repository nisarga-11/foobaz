#!/usr/bin/env python3
"""
Postgres Backup and Restore System with LangGraph Swarm
Terminal-based interface for managing database backups and restores.
"""

import os
import sys
import signal
from typing import Optional
from swarm_system import DatabaseSwarmSystem
from config.settings import settings

class TerminalInterface:
    """Terminal-based interface for the database backup and restore system."""
    
    def __init__(self):
        self.swarm_system = DatabaseSwarmSystem()
        self.running = True
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        print("\nShutting down gracefully...")
        self.running = False
        sys.exit(0)
    
    def print_banner(self):
        """Print the application banner."""
        banner = """
╔══════════════════════════════════════════════════════════════╗
║           Postgres Backup & Restore System                   ║
║                    LangGraph Swarm Edition                   ║
╚══════════════════════════════════════════════════════════════╝

Two agents are available:
- CustomerDB_Agent: Manages customer_db
- EmployeeDB_Agent: Manages employee_db

Type your requests in natural language. Examples:
- "The customer database seems corrupted, can you check?"
- "Create a backup of the employee database"
- "List available backups for both databases"
- "Restore the customer database from the latest backup"
- "Create pgBackRest full backup for both databases"
- "I need to restore using pgBackRest to the latest backup"
- "List all pgBackRest backups available"

Type 'quit' or 'exit' to close the application.
"""
        print(banner)
    
    def print_agent_status(self):
        """Print the current status of both agents."""
        status = self.swarm_system.get_agent_status()
        
        print("\n" + "="*60)
        print("AGENT STATUS")
        print("="*60)
        print(f"Customer Agent: {status['customer_agent']['name']}")
        print(f"  Database: {status['customer_agent']['database']}")
        print(f"  Current Task: {status['customer_agent']['current_task'] or 'Idle'}")
        print()
        print(f"Employee Agent: {status['employee_agent']['name']}")
        print(f"  Database: {status['employee_agent']['database']}")
        print(f"  Current Task: {status['employee_agent']['current_task'] or 'Idle'}")
        print("="*60)
    
    def process_user_input(self, user_input: str) -> None:
        """Process user input through the swarm system."""
        try:
            print(f"\nUser: {user_input}")
            print("-" * 60)
            
            # Process through the swarm system
            result = self.swarm_system.process_user_input(user_input)
            
            # Display conversation history
            if result["conversation_history"]:
                for message in result["conversation_history"]:
                    print(message)
                    print()
            
            # Display final response if different from conversation history
            if result["response"] and result["response"] not in result["conversation_history"]:
                print(result["response"])
            
            print("-" * 60)
            
        except Exception as e:
            print(f"Error processing request: {str(e)}")
            print("Please try again or contact support if the issue persists.")
    
    def run(self):
        """Run the main application loop."""
        self.print_banner()
        self.print_agent_status()
        
        while self.running:
            try:
                # Get user input
                user_input = input("\nEnter your request: ").strip()
                
                # Check for exit commands
                if user_input.lower() in ['quit', 'exit', 'q']:
                    print("Goodbye!")
                    break
                
                # Check for help command
                if user_input.lower() in ['help', 'h', '?']:
                    self.print_banner()
                    continue
                
                # Check for status command
                if user_input.lower() in ['status', 's']:
                    self.print_agent_status()
                    continue
                
                # Skip empty input
                if not user_input:
                    continue
                
                # Process the user input
                self.process_user_input(user_input)
                
            except KeyboardInterrupt:
                print("\nUse 'quit' to exit gracefully.")
            except EOFError:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"Unexpected error: {str(e)}")
                print("Please try again.")

def check_dependencies():
    """Check if required dependencies are available."""
    missing_deps = []
    
    # Check for pg_dump and pg_restore
    try:
        import subprocess
        subprocess.run(["pg_dump", "--version"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        missing_deps.append("pg_dump (PostgreSQL client tools)")
    
    try:
        subprocess.run(["pg_restore", "--version"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        missing_deps.append("pg_restore (PostgreSQL client tools)")
    
    # Check for Ollama
    try:
        import requests
        response = requests.get(f"{settings.OLLAMA_BASE_URL}/api/tags", timeout=5)
        if response.status_code != 200:
            missing_deps.append("Ollama server (running)")
    except Exception:
        missing_deps.append("Ollama server (running)")
    
    if missing_deps:
        print("Missing dependencies:")
        for dep in missing_deps:
            print(f"  - {dep}")
        print("\nPlease install missing dependencies before running the application.")
        return False
    
    return True

def create_backup_directory():
    """Create the backup directory if it doesn't exist."""
    os.makedirs(settings.BACKUP_DIR, exist_ok=True)

def main():
    """Main entry point for the application."""
    print("Initializing Postgres Backup and Restore System...")
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Create backup directory
    create_backup_directory()
    
    # Create and run the terminal interface
    interface = TerminalInterface()
    interface.run()

if __name__ == "__main__":
    main()
