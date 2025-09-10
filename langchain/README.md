# PostgreSQL Backup & Restore System with pgBackRest

A comprehensive PostgreSQL backup and restore system using pgBackRest with intelligent multi-agent coordination and Point-in-Time Recovery (PITR) capabilities.

## 🚀 Features

- **pgBackRest Integration**: Full backup, incremental backup, and Point-in-Time Recovery
- **Multi-Agent System**: CustomerDB_Agent and EmployeeDB_Agent with intelligent handoff
- **Automated Scheduling**: Weekly full backups and incremental backups every 2 minutes
- **Natural Language Interface**: Interact using plain English commands
- **MCP Protocol**: HTTP-based Model Context Protocol for tool communication
- **Intelligent Recommendations**: LLM-powered backup selection and coordination

## 🏗️ Architecture

- **Two Agents**: CustomerDB_Agent and EmployeeDB_Agent
- **pgBackRest**: Industry-standard PostgreSQL backup tool
- **MCP Server**: HTTP-based tool execution server
- **Ollama LLM**: Natural language processing and recommendations
- **Automated Scheduler**: Background backup scheduling

## 📋 Prerequisites

- PostgreSQL 17+ (Postgres.app on macOS)
- pgBackRest 2.56+
- Python 3.8+
- Ollama with a suitable model

## ⚡ Quick Start

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Start the MCP server:**
```bash
python3 http_mcp_server.py &
```

3. **Start the backup scheduler:**
```bash
python3 backup_scheduler.py &
```

4. **Run the main application:**
```bash
python3 main.py
```

## 🎯 Usage Examples

### List Available Backups
```
pgbackrest list backups
```

### Get Recommended Backups
```
pgbackrest recommended backups
```

### Restore to Specific Backup
```
pgbackrest restore to backup 20250907-194143F_20250907-194432I
```

### Point-in-Time Recovery
```
pgbackrest pitr restore to 2025-09-07 15:00:00+05:30
```

### Create Manual Backups
```
pgbackrest full backup
pgbackrest incremental backup
```

## 📁 Project Structure

```
├── main.py                    # Main application interface
├── http_mcp_server.py         # MCP server for tool execution
├── backup_scheduler.py        # Automated backup scheduling
├── pgbackrest_restore.sh      # Restore script
├── pgbackrest_pitr_restore.sh # PITR restore script
├── agents/                    # Multi-agent system
│   ├── base_agent.py         # Base agent class
│   ├── db1_agent.py          # Customer database agent
│   └── db2_agent.py          # Employee database agent
├── mcp_server/tools/         # MCP tool implementations
├── config/                   # Configuration files
├── utils/                    # Utility functions
└── logs/                     # System logs
```

## 🔧 Configuration

### pgBackRest Configuration
Located at: `~/.config/pgbackrest/pgbackrest.conf`

### Environment Variables
Create `.env` file with:
```bash
PGBACKREST_BACKUP_PATH=/Users/aarthiprashanth/backups/pgbackrest_final
MCP_SERVER_URL=http://localhost:8082
```

## 📊 Available Commands

| Command | Description |
|---------|-------------|
| `pgbackrest list backups` | List all available backups |
| `pgbackrest recommended backups` | Get LLM-recommended backups |
| `pgbackrest full backup` | Create a full backup |
| `pgbackrest incremental backup` | Create an incremental backup |
| `pgbackrest restore to backup <ID>` | Restore to specific backup |
| `pgbackrest pitr restore to <timestamp>` | Point-in-Time Recovery |
| `get database info` | Get database information |

## 🛠️ Troubleshooting

### Common Issues

1. **MCP Server Not Running**: Start with `python3 http_mcp_server.py &`
2. **Backup Scheduler Not Running**: Start with `python3 backup_scheduler.py &`
3. **pgBackRest Errors**: Check configuration in `~/.config/pgbackrest/pgbackrest.conf`
4. **PostgreSQL Connection Issues**: Verify PostgreSQL is running and accessible

### Log Files
- System logs: `logs/system_*.log`
- Backup logs: `logs/backup_operations_*.log`
- pgBackRest logs: `logs/pgbackrest_*.log`

## 🔄 Backup Strategy

- **Full Backups**: Weekly (Sundays)
- **Incremental Backups**: Every 2 minutes
- **Retention**: Configurable via pgBackRest settings
- **WAL Archiving**: Continuous for PITR support

## 📈 Monitoring

The system provides comprehensive logging and monitoring:
- Real-time backup status
- Agent coordination logs
- Error tracking and reporting
- Performance metrics

## 🤝 Contributing

This system is designed for PostgreSQL backup and restore operations with intelligent multi-agent coordination. The architecture supports easy extension for additional databases and backup strategies.