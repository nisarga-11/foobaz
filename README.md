# SP Lakehouse Backup - PostgreSQL Multi-Server Orchestrator

A Python project that uses LangChain + LangGraph with Ollama LLMs to orchestrate PostgreSQL backup/restore operations across two Postgres servers, each fronted by its own MCP server that exposes backup/restore tools over REST.

## 🎯 Features

- **TRUE WAL Incremental Backups**: Real PostgreSQL WAL-based incremental backups with only transaction changes
- **Multi-Server Support**: Manages two PostgreSQL servers (PG1 and PG2) via separate MCP endpoints
- **AI-Powered Orchestration**: Uses Ollama LLMs with LangChain/LangGraph for intelligent request routing
- **Fixed CLI Interface**: Robust JSON parsing with fallback logic for reliable operation
- **Automatic Restore Planning**: Suggests optimal restore points based on timestamp proximity or latest available
- **No Local DB Operations**: All backup/restore operations go through MCP REST APIs only
- **Database-Aware Routing**: Intelligent routing based on database-to-server mapping

## 🏗️ Architecture

```
User ──> LangGraph Supervisor (Ollama + Fixed JSON Parsing)
         │
         ├─(handoff)─> BackupAgentPG1 (LangChain+Ollama) ──> MCP1 (REST) ──> PG Server 1
         └─(handoff)─> BackupAgentPG2 (LangChain+Ollama) ──> MCP2 (REST) ──> PG Server 2
```

### Components:
- **Supervisor Agent**: Interprets user intent and routes to appropriate backup agents
- **Backup Agents**: Execute operations via MCP REST tools for their respective servers
- **TRUE WAL System**: Real PostgreSQL WAL file archiving for incremental backups
- **MCP Servers**: Own backup scheduling (2-minute incremental + weekly full) and on-demand operations

## 📊 Database Structure

### 🏢 Server 1 (PG1) - Business Operations
**MCP Server**: http://localhost:8001  
**Databases**: `customer_db`, `inventory_db`, `analytics_db`

- **customer_db**: Customer management and CRM data (customers, orders, order_items)
- **inventory_db**: Product inventory and warehouse management (products, inventory, movements)
- **analytics_db**: Business intelligence and reporting data (sales_metrics, customer_analytics)

### 🏢 Server 2 (PG2) - HR & Finance  
**MCP Server**: http://localhost:8002  
**Databases**: `hr_db`, `finance_db`, `reporting_db`

- **hr_db**: Human resources management (employees, attendance, performance_reviews)
- **finance_db**: Financial transactions and accounting (accounts, transactions, budget_allocations)
- **reporting_db**: Cross-system reporting and data warehousing (daily_reports, kpi_tracking)

## 🚀 Installation & Setup

### Prerequisites

1. **Ollama**: Local LLM server
2. **PostgreSQL**: Database server(s)
3. **Python 3.11+**: For running the orchestrator

### 1. Install and Setup Ollama

```bash
# Install Ollama (macOS)
brew install ollama

# Start Ollama service
ollama serve

# Pull the required model
ollama pull llama3.1
```

### 2. Setup PostgreSQL Databases

```bash
# Make setup script executable
chmod +x setup_postgres.sh

# Run the database setup
./setup_postgres.sh
```

This creates all databases with sample data on the main PostgreSQL instance.

### 3. Setup the Orchestrator

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -e .

# Configure environment
cp env.example .env
```

### 4. Configure Environment Variables

Edit `.env` file:

```bash
# MCP servers
MCP1_BASE_URL=http://localhost:8001
MCP1_API_KEY=your-api-key-here
MCP2_BASE_URL=http://localhost:8002 
MCP2_API_KEY=your-api-key-here

# Ollama
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=llama3.1

# Optional LLM params
OLLAMA_TEMPERATURE=0.2
OLLAMA_NUM_CTX=8192
```

## 🎮 Usage

### Start the TRUE WAL Backup System

```bash
# Start the MCP servers directly
python true_wal_incremental_backup.py
```

This starts:
- MCP1 server on port 8001 (manages PG1 databases)
- MCP2 server on port 8002 (manages PG2 databases)  
- Automatic WAL incremental backups every 2 minutes
- Weekly full backups on Sundays at 3:00 AM

### Interactive CLI (Fixed)

```bash
# Start interactive CLI with fixed JSON parsing
python -m cli run
```

### Non-Interactive Commands

```bash
# Execute single commands (bypasses any timeout issues)
python -m cli query "list backups for analytics_db"
python -m cli query "create backup for hr_db"
python -m cli query "restore customer_db to latest backup"
```

### Test Connections

```bash
python -m cli test-connection
```

## 📝 Command Examples

### List Backups
```
backup> list backups for customer_db
backup> list backups for hr_db
backup> list backups for all databases  
```

### Create Backups
```
backup> create backup for analytics_db
backup> trigger full backup for hr_db
```

### Restore Operations
```
backup> restore customer_db to latest
backup> restore hr_db to latest backup
backup> restore analytics_db to 2025-09-10T10:30:00Z
```

### Direct API Commands (Alternative)

```bash
# List WAL backups
curl -s http://localhost:8001/invoke -H "Content-Type: application/json" -d '{"tool": "list_backups", "arguments": {"db_name": "analytics_db", "limit": 5}}' | python3 -m json.tool

# Create incremental backup
curl -X POST http://localhost:8001/invoke -H "Content-Type: application/json" -d '{"tool": "trigger_incremental_backup", "arguments": {"db_name": "analytics_db"}}' | python3 -m json.tool

# Check system health
curl -s http://localhost:8001/invoke -H "Content-Type: application/json" -d '{"tool": "health", "arguments": {}}' | python3 -m json.tool
```

## 🔧 TRUE WAL Incremental Backup System

### How It Works

1. **Base Backups**: `pg_basebackup` creates full cluster snapshots
2. **Incremental Backups**: Archives actual PostgreSQL WAL files (only transaction changes)
3. **LSN Tracking**: Precise tracking for point-in-time recovery
4. **Automatic Scheduling**: 2-minute incremental, weekly full backups

### File Structure

```
backups/
├── mcp1/
│   ├── basebackups/                    # pg_basebackup cluster snapshots
│   └── wal_incremental/                # WAL files with ONLY changes
│       └── analytics_db_wal_YYYYMMDD_HHMMSS/
│           ├── *.wal.gz                # Compressed WAL files
│           ├── wal_metadata.json       # LSN tracking
│           └── incremental_summary.txt # What changed
└── mcp2/
    ├── basebackups/                    # pg_basebackup cluster snapshots
    └── wal_incremental/                # WAL files with ONLY changes
```

### Key Benefits

- ✅ **Truly Incremental**: Only captures database changes, not full dumps
- ✅ **Real WAL Files**: Actual PostgreSQL transaction logs
- ✅ **Point-in-Time Recovery**: Exact LSN precision
- ✅ **Compressed Storage**: Efficient backup file sizes
- ✅ **Automated Scheduling**: Hands-off operation

## 🛠️ Fixed CLI Features

### JSON Parsing Improvements

- **Robust JSON Extraction**: Multiple parsing strategies for Ollama responses
- **Fallback Logic**: Graceful degradation when JSON parsing fails
- **Database-Server Mapping**: Intelligent routing based on database names
- **Timeout Handling**: Non-interactive commands bypass timeout issues

### Database-to-Server Mapping

The CLI now correctly maps databases to their servers:

- **PG1 Databases**: `customer_db`, `inventory_db`, `analytics_db` → MCP1
- **PG2 Databases**: `hr_db`, `finance_db`, `reporting_db` → MCP2

## 📋 Current Setup Status

### Actual Configuration
All databases are currently on the main PostgreSQL server (port 5432) with logical separation:

```
📊 PostgreSQL Server (Port 5432)
├── 🏢 PG1 Databases (Business Operations) → MCP1
│   ├── customer_db (3 tables, 21 records)
│   ├── inventory_db (3 tables, 24 records) 
│   └── analytics_db (3 tables, 26 records)
└── 🏢 PG2 Databases (HR & Finance) → MCP2
    ├── hr_db (5 tables, 45 records)
    ├── finance_db (5 tables, 36 records)
    └── reporting_db (3 tables, 30 records)
```

### Sample Data Included
- **6 Databases** with comprehensive business data
- **21 Tables** covering CRM, inventory, analytics, HR, finance, and reporting
- **182+ Records** of realistic sample data for testing

## 🏗️ Project Structure

```
sp-lakehouse-backup/
├── agents/
│   ├── backup_agent_pg1.py      # PG1 backup agent
│   └── backup_agent_pg2.py      # PG2 backup agent
├── supervisor/
│   ├── system_prompt.txt        # Supervisor agent prompt
│   └── restore_planner.py       # Backup selection logic
├── tools/
│   └── mcp_tools.py            # LangChain MCP tool wrappers
├── llm/
│   └── ollama_helper.py        # Ollama LLM helpers (fixed)
├── sql/
│   ├── setup_pg1.sql           # PG1 database schema & data
│   └── setup_pg2.sql           # PG2 database schema & data
├── backups/                    # TRUE WAL backup storage
│   ├── mcp1/                   # PG1 backups
│   └── mcp2/                   # PG2 backups
├── mcp/                        # MCP tools and client
│   ├── mcp_client.py           # MCP REST client
│   ├── mcp_tools.py            # LangChain MCP tools
│   └── server_config.json      # MCP server configuration
├── graph.py                    # LangGraph workflow (fixed JSON parsing)
├── cli.py                      # Interactive CLI (fixed)
├── true_wal_incremental_backup.py  # Main MCP server (TRUE WAL backup system)
├── setup_postgres.sh          # Database setup script
├── pyproject.toml              # Project dependencies
├── env.example                 # Environment template
└── README.md                   # This file
```

## 🚨 Important Notes

### Constraints
- **No Local Backup Code**: All backup/restore operations go through MCP REST APIs
- **DB-Level Operations Only**: Always requires `db_name` parameter
- **TRUE WAL Only**: System uses real PostgreSQL WAL files for incremental backups
- **Read-Only Orchestrator**: Never directly touches PostgreSQL

### CLI Fixes Applied
- ✅ **JSON Parsing**: Robust parsing with multiple fallback strategies
- ✅ **Database Routing**: Correct mapping of databases to servers
- ✅ **Error Handling**: Graceful degradation when parsing fails
- ✅ **Non-Interactive Mode**: Bypass timeout issues with direct commands

## 🎯 Quick Start

1. **Start the system**:
   ```bash
   python true_wal_incremental_backup.py
   ```

2. **Test the CLI**:
   ```bash
   python -m cli query "list backups for analytics_db"
   ```

3. **Check backups**:
   ```bash
   ls -la backups/mcp1/wal_incremental/
   ```

4. **Monitor logs** for TRUE WAL incremental backup activity (every 2 minutes)

## 📞 Support

- **CLI Issues**: Use non-interactive commands (`python -m cli query "..."`)
- **Backup Issues**: Check `backups/` directory for TRUE WAL files
- **Connection Issues**: Verify Ollama and PostgreSQL are running
- **Restore Issues**: Ensure base backups exist before incremental restores

The system provides detailed logging and real WAL-based incremental backups for production-ready PostgreSQL backup orchestration! 🚀