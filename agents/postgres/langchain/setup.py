#!/usr/bin/env python3
"""
Setup script for the Postgres Backup and Restore System
Helps users install dependencies and configure the system.
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

def check_python_version():
    """Check if Python version is compatible."""
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        sys.exit(1)
    print(f"✓ Python {sys.version_info.major}.{sys.version_info.minor} detected")

def install_python_dependencies():
    """Install Python dependencies from requirements.txt."""
    print("\nInstalling Python dependencies...")
    
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], 
                      check=True, capture_output=True, text=True)
        print("✓ Python dependencies installed successfully")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install Python dependencies: {e.stderr}")
        sys.exit(1)

def check_postgres_tools():
    """Check if PostgreSQL tools are available."""
    print("\nChecking PostgreSQL tools...")
    
    tools = ["pg_dump", "pg_restore"]
    missing_tools = []
    
    for tool in tools:
        if shutil.which(tool) is None:
            missing_tools.append(tool)
    
    if missing_tools:
        print(f"❌ Missing PostgreSQL tools: {', '.join(missing_tools)}")
        print("\nTo install PostgreSQL tools:")
        print("  Ubuntu/Debian: sudo apt-get install postgresql-client")
        print("  macOS: brew install postgresql")
        print("  Windows: Download from https://www.postgresql.org/download/windows/")
        return False
    else:
        print("✓ PostgreSQL tools found")
        return True

def check_ollama():
    """Check if Ollama is available and running."""
    print("\nChecking Ollama...")
    
    try:
        # Check if ollama command exists
        if shutil.which("ollama") is None:
            print("❌ Ollama not found")
            print("\nTo install Ollama:")
            print("  Visit: https://ollama.ai/download")
            print("  Or run: curl -fsSL https://ollama.ai/install.sh | sh")
            return False
        
        # Check if Ollama server is running
        result = subprocess.run(["ollama", "list"], 
                              capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print("✓ Ollama is running")
            
            # Check if llama2 model is available
            if "llama2" in result.stdout.lower():
                print("✓ llama2 model is available")
                return True
            else:
                print("⚠️  llama2 model not found")
                print("To install llama2: ollama pull llama2")
                return False
        else:
            print("❌ Ollama server is not running")
            print("Start Ollama with: ollama serve")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Ollama server is not responding")
        print("Start Ollama with: ollama serve")
        return False
    except Exception as e:
        print(f"❌ Error checking Ollama: {e}")
        return False

def create_env_file():
    """Create .env file from template if it doesn't exist."""
    print("\nSetting up environment configuration...")
    
    env_file = Path(".env")
    env_example = Path("env.example")
    
    if env_file.exists():
        print("✓ .env file already exists")
        return
    
    if env_example.exists():
        shutil.copy(env_example, env_file)
        print("✓ Created .env file from template")
        print("⚠️  Please edit .env file with your database credentials")
    else:
        print("❌ env.example file not found")
        sys.exit(1)

def create_backup_directory():
    """Create backup directory."""
    print("\nCreating backup directory...")
    
    backup_dir = Path("./backups")
    backup_dir.mkdir(exist_ok=True)
    print("✓ Backup directory created")

def run_tests():
    """Run system tests."""
    print("\nRunning system tests...")
    
    try:
        result = subprocess.run([sys.executable, "test_system.py"], 
                              capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            print("✓ All tests passed")
            return True
        else:
            print(f"❌ Tests failed: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Tests timed out")
        return False
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return False

def print_next_steps():
    """Print next steps for the user."""
    print("\n" + "="*60)
    print("SETUP COMPLETE!")
    print("="*60)
    print("\nNext steps:")
    print("1. Edit .env file with your database credentials")
    print("2. Ensure PostgreSQL databases are running")
    print("3. Start Ollama server: ollama serve")
    print("4. Run the application: python main.py")
    print("\nExample usage:")
    print("  python main.py")
    print("  > Enter your request: The customer database seems corrupted, can you check?")
    print("\nFor help, run: python main.py --help")

def main():
    """Main setup function."""
    print("="*60)
    print("POSTGRES BACKUP & RESTORE SYSTEM - SETUP")
    print("="*60)
    
    # Check Python version
    check_python_version()
    
    # Install Python dependencies
    install_python_dependencies()
    
    # Check PostgreSQL tools
    postgres_ok = check_postgres_tools()
    
    # Check Ollama
    ollama_ok = check_ollama()
    
    # Create configuration files
    create_env_file()
    create_backup_directory()
    
    # Run tests if dependencies are available
    if postgres_ok and ollama_ok:
        run_tests()
    else:
        print("\n⚠️  Skipping tests due to missing dependencies")
    
    # Print next steps
    print_next_steps()

if __name__ == "__main__":
    main()
