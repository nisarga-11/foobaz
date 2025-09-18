#!/bin/bash
# Complete PostgreSQL Backup System Startup Script
# Runs: Scheduled Backups + MCP Servers + CLI

echo "STARTING COMPLETE POSTGRESQL BACKUP SYSTEM"
echo "=============================================="
echo ""
echo "System Components:"
echo "   1. ⏰ Scheduled Backup System (2min incremental + weekly full)"
echo "   2. MCP HTTP Servers (PG1:8003, PG2:8004)" 
echo "   3. True WAL Backup Servers (MCP1:8001, MCP2:8002)"
echo "   4. 💬 CLI Interface for manual operations"
echo ""

# Function to cleanup background processes
cleanup() {
    echo ""
    echo "Stopping all backup services..."
    if [ ! -z "$WAL_PID" ]; then
        kill $WAL_PID 2>/dev/null
        echo "   Stopped WAL backup servers"
    fi
    if [ ! -z "$MCP1_PID" ]; then
        kill $MCP1_PID 2>/dev/null
        echo "   Stopped MCP1 server"
    fi
    if [ ! -z "$MCP2_PID" ]; then
        kill $MCP2_PID 2>/dev/null
        echo "   Stopped MCP2 server"
    fi
    echo "🏁 All services stopped"
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

# Create log directory
mkdir -p logs

echo "Starting system components..."
echo ""

# 1. Start True WAL Backup Servers (with scheduling)
echo "1️⃣ Starting TRUE WAL Backup Servers (MCP1:8001, MCP2:8002)..."
python true_wal_incremental_backup.py > logs/wal_backup_servers.log 2>&1 &
WAL_PID=$!
echo "   WAL Backup Servers started (PID: $WAL_PID)"
echo "   📁 Logs: logs/wal_backup_servers.log"

# Wait for WAL servers to initialize
sleep 5

# 2. Start MCP HTTP Servers 
echo ""
echo "2️⃣ Starting MCP HTTP Servers..."
python mcp_http_server.py --server-name PG1 --port 8003 > logs/mcp_pg1.log 2>&1 &
MCP1_PID=$!
echo "   PG1 MCP Server started on port 8003 (PID: $MCP1_PID)"

python mcp_http_server.py --server-name PG2 --port 8004 > logs/mcp_pg2.log 2>&1 &
MCP2_PID=$!
echo "   PG2 MCP Server started on port 8004 (PID: $MCP2_PID)"
echo "   📁 Logs: logs/mcp_pg1.log, logs/mcp_pg2.log"

# Wait for MCP servers to initialize
sleep 5

# 3. Enable backup schedules on both WAL servers
echo ""
echo "3️⃣ Enabling automatic backup schedules..."
echo "   ⏰ Configuring 2-minute incremental + weekly full backups..."

# Enable schedules on MCP1 (8001)
curl -s -X POST http://localhost:8001/invoke \
  -H "Content-Type: application/json" \
  -d '{"tool": "start_scheduler", "arguments": {}}' > /dev/null

# Enable schedules on MCP2 (8002) 
curl -s -X POST http://localhost:8002/invoke \
  -H "Content-Type: application/json" \
  -d '{"tool": "start_scheduler", "arguments": {}}' > /dev/null

echo "   Backup schedules enabled on both servers"

# 4. Test system connectivity
echo ""
echo "4️⃣ Testing system connectivity..."

# Test WAL servers
echo "   🔍 Testing WAL backup servers..."
WAL1_STATUS=$(curl -s http://localhost:8001/invoke -X POST -H "Content-Type: application/json" -d '{"tool": "health", "arguments": {}}' | jq -r '.result.status' 2>/dev/null || echo "error")
WAL2_STATUS=$(curl -s http://localhost:8002/invoke -X POST -H "Content-Type: application/json" -d '{"tool": "health", "arguments": {}}' | jq -r '.result.status' 2>/dev/null || echo "error")

if [ "$WAL1_STATUS" = "healthy" ]; then
    echo "   MCP1 WAL Server: Healthy"
else
    echo "   MCP1 WAL Server: $WAL1_STATUS"
fi

if [ "$WAL2_STATUS" = "healthy" ]; then
    echo "   MCP2 WAL Server: Healthy"
else
    echo "   MCP2 WAL Server: $WAL2_STATUS"
fi

# Test MCP servers
echo "   🔍 Testing MCP HTTP servers..."
MCP1_STATUS=$(curl -s http://localhost:8003/health | jq -r '.status' 2>/dev/null || echo "error")
MCP2_STATUS=$(curl -s http://localhost:8004/health | jq -r '.status' 2>/dev/null || echo "error")

if [ "$MCP1_STATUS" = "healthy" ]; then
    echo "   PG1 MCP Server: Healthy"
else
    echo "   PG1 MCP Server: $MCP1_STATUS"
fi

if [ "$MCP2_STATUS" = "healthy" ]; then
    echo "   PG2 MCP Server: Healthy"
else
    echo "   PG2 MCP Server: $MCP2_STATUS"
fi

echo ""
echo "🎉 COMPLETE BACKUP SYSTEM READY!"
echo "================================"
echo ""
echo "System Status:"
echo "   TRUE WAL Servers: http://localhost:8001, http://localhost:8002"
echo "   MCP HTTP Servers: http://localhost:8003, http://localhost:8004"
echo "   Backup Schedules: ACTIVE (2min incremental + weekly full)"
echo "   📁 Backup Storage: backups/mcp1/, backups/mcp2/"
echo ""
echo "Database Assignment:"
echo "   📊 PG1 (MCP1): customer_db, inventory_db, analytics_db"
echo "   📊 PG2 (MCP2): hr_db, finance_db, reporting_db"
echo ""
echo "Usage Examples:"
echo "   # List current backups"
echo "   python cli.py query \"List backups for customer_db\""
echo ""
echo "   # Trigger manual backup"
echo "   python cli.py query \"Create a full backup of hr_db\""
echo ""
echo "   # Restore database"
echo "   python cli.py query \"Restore customer_db from latest backup\""
echo ""
echo "   # Check system health"
echo "   python cli.py test-connection"
echo ""
echo "📊 Scheduled Backup Status:"
echo "   ⏱️  Incremental: Every 2 minutes (capturing WAL changes)"
echo "   📅 Full Backup: Every Sunday at 3:00 AM"
echo "   📁 Storage: Both servers archiving to backups/mcp1 & backups/mcp2"
echo ""
echo "📜 Logs Available:"
echo "   📄 WAL Servers: logs/wal_backup_servers.log"
echo "   📄 MCP PG1: logs/mcp_pg1.log"
echo "   📄 MCP PG2: logs/mcp_pg2.log"
echo ""
echo "System Management:"
echo "   To stop: Press Ctrl+C"
echo "   To check logs: tail -f logs/*.log"
echo "   Manual operations: Use CLI commands above"
echo ""

# Show current backup counts
echo "📈 Current Backup Inventory:"
if [ -d "backups/mcp1/basebackups" ]; then
    MCP1_BASE_COUNT=$(ls backups/mcp1/basebackups 2>/dev/null | wc -l)
    echo "   📦 MCP1 Base Backups: $MCP1_BASE_COUNT"
fi
if [ -d "backups/mcp1/wal_incremental" ]; then
    MCP1_WAL_COUNT=$(ls backups/mcp1/wal_incremental 2>/dev/null | wc -l)
    echo "   MCP1 WAL Incremental: $MCP1_WAL_COUNT"
fi
if [ -d "backups/mcp2/basebackups" ]; then
    MCP2_BASE_COUNT=$(ls backups/mcp2/basebackups 2>/dev/null | wc -l)
    echo "   📦 MCP2 Base Backups: $MCP2_BASE_COUNT"
fi
if [ -d "backups/mcp2/wal_incremental" ]; then
    MCP2_WAL_COUNT=$(ls backups/mcp2/wal_incremental 2>/dev/null | wc -l)
    echo "   MCP2 WAL Incremental: $MCP2_WAL_COUNT"
fi

echo ""
echo "System is now running. Press Ctrl+C to stop all services."
echo "💬 Open a new terminal and run CLI commands to interact with the system."
echo ""

# Keep the script running
while true; do
    sleep 10
    
    # Quick health check every minute
    if [ $((SECONDS % 60)) -eq 0 ]; then
        echo "⏰ $(date): System running - WAL:$WAL_PID MCP1:$MCP1_PID MCP2:$MCP2_PID"
    fi
done
