#!/bin/bash

# Start All Lakehouse Services
# 1. PostgreSQL Orchestrator (port 8002)
# 2. Ceph S3 Server (port 8000)  
# 3. Lakehouse Orchestrator (port 8001)

set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo "================================================"
echo "Starting All Lakehouse Services"
echo "================================================"

# Check conda environment
if [[ "$CONDA_DEFAULT_ENV" != "py312" ]]; then
    echo -e "${RED}Please activate py312 environment first:${NC}"
    echo "  conda activate py312"
    exit 1
fi

# Directories
POSTGRES_DIR="/root/sp-lakehouse-backup/project"
CEPH_DIR="/root/sp-lakehouse-backup/crew_mcp_project/crew_mcp_project/ceph"

# Kill existing processes
echo -e "${BLUE}Stopping any existing services...${NC}"
lsof -ti:8000 | xargs kill -9 2>/dev/null || true
lsof -ti:8001 | xargs kill -9 2>/dev/null || true
lsof -ti:8002 | xargs kill -9 2>/dev/null || true
sleep 2

# 1. Start PostgreSQL Orchestrator (port 8002)
echo ""
echo -e "${GREEN}[1/3] Starting PostgreSQL Orchestrator on port 8002...${NC}"
cd "$POSTGRES_DIR"

if [ ! -f "orchestrator.py" ]; then
    echo -e "${RED}orchestrator.py not found in $POSTGRES_DIR${NC}"
    exit 1
fi

# Check if it uses uvicorn or direct python
if grep -q "uvicorn.run" orchestrator.py; then
    # Modify port in-place temporarily or run with custom port
    nohup python orchestrator.py > /tmp/postgres_orchestrator.log 2>&1 &
else
    # Assume it's a FastAPI app with 'app' variable
    nohup uvicorn orchestrator:app --host 0.0.0.0 --port 8002 > /tmp/postgres_orchestrator.log 2>&1 &
fi

POSTGRES_PID=$!
echo -e "${GREEN}PostgreSQL Orchestrator started (PID: $POSTGRES_PID)${NC}"

# Wait for it to be ready
echo -n "Waiting for PostgreSQL orchestrator"
for i in {1..30}; do
    if curl -s http://localhost:8002/ > /dev/null 2>&1 || \
       curl -s http://localhost:8002/health > /dev/null 2>&1 || \
       curl -s http://localhost:8002/servers > /dev/null 2>&1; then
        echo -e " ${GREEN}Ready!${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e " ${RED}Failed!${NC}"
        echo "Check log: tail -20 /tmp/postgres_orchestrator.log"
        tail -20 /tmp/postgres_orchestrator.log
        exit 1
    fi
    echo -n "."
    sleep 1
done

# 2. Start Ceph S3 Server (port 8000)
echo ""
echo -e "${GREEN}[2/3] Starting Ceph S3 Server on port 8000...${NC}"
cd "$CEPH_DIR"

if [ ! -f "main.py" ]; then
    echo -e "${RED}main.py not found in $CEPH_DIR${NC}"
    exit 1
fi

nohup uvicorn main:app --reload --host 0.0.0.0 --port 8000 > /tmp/ceph_server.log 2>&1 &
CEPH_PID=$!
echo -e "${GREEN}Ceph S3 Server started (PID: $CEPH_PID)${NC}"

# Wait for it to be ready
echo -n "Waiting for Ceph S3 server"
for i in {1..30}; do
    if curl -s http://localhost:8000/ > /dev/null 2>&1; then
        echo -e " ${GREEN}Ready!${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e " ${RED}Failed!${NC}"
        echo "Check log: tail -20 /tmp/ceph_server.log"
        tail -20 /tmp/ceph_server.log
        kill $POSTGRES_PID 2>/dev/null
        exit 1
    fi
    echo -n "."
    sleep 1
done

# 3. Start Lakehouse Orchestrator (port 8001)
echo ""
echo -e "${GREEN}[3/3] Starting Lakehouse Orchestrator on port 8001...${NC}"
cd "$POSTGRES_DIR"

if [ ! -f "lakehouse_orchestrator.py" ]; then
    echo -e "${RED}lakehouse_orchestrator.py not found in $POSTGRES_DIR${NC}"
    kill $POSTGRES_PID $CEPH_PID 2>/dev/null
    exit 1
fi

nohup python lakehouse_orchestrator.py > /tmp/lakehouse_orchestrator.log 2>&1 &
LAKEHOUSE_PID=$!
echo -e "${GREEN}Lakehouse Orchestrator started (PID: $LAKEHOUSE_PID)${NC}"

# Wait for it to be ready
echo -n "Waiting for Lakehouse orchestrator"
for i in {1..30}; do
    if curl -s http://localhost:8001/ > /dev/null 2>&1; then
        echo -e " ${GREEN}Ready!${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e " ${RED}Failed!${NC}"
        echo "Check log: tail -20 /tmp/lakehouse_orchestrator.log"
        tail -20 /tmp/lakehouse_orchestrator.log
        kill $POSTGRES_PID $CEPH_PID 2>/dev/null
        exit 1
    fi
    echo -n "."
    sleep 1
done

# Save PIDs
echo "$POSTGRES_PID" > /tmp/postgres_orchestrator.pid
echo "$CEPH_PID" > /tmp/ceph_server.pid
echo "$LAKEHOUSE_PID" > /tmp/lakehouse_orchestrator.pid

# Display status
echo ""
echo "================================================"
echo -e "${GREEN}All Services Started Successfully!${NC}"
echo "================================================"
echo ""
echo "Service Status:"
echo "  1. PostgreSQL Orchestrator (8002): PID $POSTGRES_PID"
echo "  2. Ceph S3 Server (8000):          PID $CEPH_PID"
echo "  3. Lakehouse Orchestrator (8001):  PID $LAKEHOUSE_PID"
echo ""
echo "Endpoints:"
echo "  PostgreSQL: http://localhost:8002/"
echo "  Ceph S3:    http://localhost:8000/"
echo "  Lakehouse:  http://localhost:8001/"
echo ""
echo "Test Commands:"
echo "  # Health check all services"
echo "  curl http://localhost:8001/lakehouse/health"
echo ""
echo "  # Backup users lakehouse (PostgreSQL + S3)"
echo "  curl -X POST http://localhost:8001/lakehouse/backup \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"lakehouse_name\": \"users_lakehouse\"}'"
echo ""
echo "Log Files:"
echo "  PostgreSQL: tail -f /tmp/postgres_orchestrator.log"
echo "  Ceph S3:    tail -f /tmp/ceph_server.log"
echo "  Lakehouse:  tail -f /tmp/lakehouse_orchestrator.log"
echo ""
echo "To stop all services:"
echo "  kill $POSTGRES_PID $CEPH_PID $LAKEHOUSE_PID"
echo "================================================"