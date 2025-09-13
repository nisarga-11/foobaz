#!/bin/bash

# PostgreSQL Multi-Server Setup Script
# This script sets up two PostgreSQL servers with sample databases

set -e

echo "🚀 Setting up PostgreSQL Multi-Server Environment"
echo "=================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default configurations
PG1_PORT=${PG1_PORT:-5432}
PG2_PORT=${PG2_PORT:-5433}
POSTGRES_USER=${POSTGRES_USER:-postgres}

echo -e "${BLUE}Configuration:${NC}"
echo "PG1 Server: localhost:$PG1_PORT (Business Operations)"
echo "PG2 Server: localhost:$PG2_PORT (HR & Finance)"
echo "PostgreSQL User: $POSTGRES_USER"
echo ""

# Function to check if PostgreSQL is running
check_postgres() {
    local port=$1
    local server_name=$2
    
    if pg_isready -h localhost -p $port -U $POSTGRES_USER >/dev/null 2>&1; then
        echo -e "${GREEN}✅ $server_name (port $port) is running${NC}"
        return 0
    else
        echo -e "${RED}❌ $server_name (port $port) is not running${NC}"
        return 1
    fi
}

# Function to check if server is in recovery mode
check_recovery_mode() {
    local port=$1
    local user=$2
    
    local read_only=$(psql -h localhost -p $port -U $user -d postgres -t -c "SHOW transaction_read_only;" 2>/dev/null | tr -d ' ')
    if [[ "$read_only" == "on" ]]; then
        return 0  # In recovery mode
    else
        return 1  # Not in recovery mode
    fi
}

# Function to run SQL file
run_sql_file() {
    local sql_file=$1
    local port=$2
    local server_name=$3
    
    echo -e "${BLUE}Setting up $server_name databases...${NC}"
    
    # Check if server is in recovery mode
    if check_recovery_mode $port $POSTGRES_USER; then
        echo -e "${YELLOW}⚠️  $server_name is in recovery mode (read-only)${NC}"
        echo "Cannot create databases on a standby server."
        echo "Please run setup on the primary server for this instance."
        return 1
    fi
    
    if psql -h localhost -p $port -U $POSTGRES_USER -f "$sql_file" >/dev/null 2>&1; then
        echo -e "${GREEN}✅ $server_name setup completed${NC}"
        return 0
    else
        echo -e "${RED}❌ Failed to setup $server_name${NC}"
        echo "Trying with current user..."
        
        # Try with current user if postgres user fails
        if psql -h localhost -p $port -U $USER -d postgres -f "$sql_file" >/dev/null 2>&1; then
            echo -e "${GREEN}✅ $server_name setup completed (using $USER)${NC}"
            return 0
        else
            echo -e "${RED}❌ Failed to setup $server_name with both postgres and $USER${NC}"
            echo "Please check the SQL file: $sql_file"
            return 1
        fi
    fi
}

# Check if SQL files exist
if [[ ! -f "sql/setup_pg1.sql" ]] || [[ ! -f "sql/setup_pg2.sql" ]]; then
    echo -e "${RED}❌ SQL setup files not found!${NC}"
    echo "Please make sure sql/setup_pg1.sql and sql/setup_pg2.sql exist"
    exit 1
fi

# Check PostgreSQL servers
echo -e "${BLUE}Checking PostgreSQL servers...${NC}"

PG1_RUNNING=false
PG2_RUNNING=false

if check_postgres $PG1_PORT "PG1 Server"; then
    PG1_RUNNING=true
fi

if check_postgres $PG2_PORT "PG2 Server"; then
    PG2_RUNNING=true
fi

echo ""

# Setup PG1 if running
if $PG1_RUNNING; then
    run_sql_file "sql/setup_pg1.sql" $PG1_PORT "PG1"
    echo -e "${GREEN}PG1 Databases created:${NC}"
    echo "  • customer_db (CRM and orders)"
    echo "  • inventory_db (Products and stock)"
    echo "  • analytics_db (Business intelligence)"
    echo ""
else
    echo -e "${YELLOW}⚠️  Skipping PG1 setup (server not running)${NC}"
fi

# Setup PG2 if running
PG2_SETUP_SUCCESS=false
if $PG2_RUNNING; then
    if run_sql_file "sql/setup_pg2.sql" $PG2_PORT "PG2"; then
        PG2_SETUP_SUCCESS=true
        echo -e "${GREEN}PG2 Databases created:${NC}"
        echo "  • hr_db (Employee management)"
        echo "  • finance_db (Accounting and budgets)"
        echo "  • reporting_db (Executive dashboards)"
        echo ""
    else
        echo -e "${YELLOW}⚠️  PG2 setup failed (check if it's a standby/replica server)${NC}"
        echo ""
    fi
else
    echo -e "${YELLOW}⚠️  Skipping PG2 setup (server not running)${NC}"
fi

# Summary
echo -e "${BLUE}Setup Summary:${NC}"
echo "=============="

if $PG1_RUNNING && $PG2_RUNNING; then
    echo -e "${GREEN}✅ Both PostgreSQL servers are configured and ready!${NC}"
    echo ""
    echo -e "${BLUE}Next Steps:${NC}"
    echo "1. Start MCP servers:"
    echo "   • MCP1 on port 8001 for PG1 (localhost:$PG1_PORT)"
    echo "   • MCP2 on port 8002 for PG2 (localhost:$PG2_PORT)"
    echo ""
    echo "2. Configure environment:"
    echo "   cp env.example .env"
    echo "   # Edit .env with your MCP server URLs and API keys"
    echo ""
    echo "3. Start the backup orchestrator:"
    echo "   source venv/bin/activate"
    echo "   python -m cli run"
    echo ""
    echo -e "${GREEN}🎯 Ready for backup/restore operations across both servers!${NC}"
    
elif $PG1_RUNNING; then
    echo -e "${YELLOW}⚠️  Only PG1 server is configured${NC}"
    echo "To setup PG2, start PostgreSQL on port $PG2_PORT and run this script again"
    
elif $PG2_RUNNING; then
    echo -e "${YELLOW}⚠️  Only PG2 server is configured${NC}"
    echo "To setup PG1, start PostgreSQL on port $PG1_PORT and run this script again"
    
else
    echo -e "${RED}❌ No PostgreSQL servers are running${NC}"
    echo ""
    echo -e "${BLUE}To start PostgreSQL servers:${NC}"
    echo ""
    echo -e "${YELLOW}Option 1: Single PostgreSQL instance (PG1 only):${NC}"
    echo "  brew services start postgresql"
    echo "  # This will run on default port $PG1_PORT"
    echo ""
    echo -e "${YELLOW}Option 2: Multiple PostgreSQL instances:${NC}"
    echo "  # Create second data directory"
    echo "  initdb -D /usr/local/var/postgres2"
    echo "  # Start PG1 on port $PG1_PORT (default)"
    echo "  pg_ctl -D /usr/local/var/postgres -l /usr/local/var/log/postgres.log start"
    echo "  # Start PG2 on port $PG2_PORT"
    echo "  pg_ctl -D /usr/local/var/postgres2 -o \"-p $PG2_PORT\" -l /usr/local/var/log/postgres2.log start"
    echo ""
    echo "Then run this script again to setup the databases."
fi

echo ""
echo -e "${BLUE}Database Structure Overview:${NC}"
echo "PG1 (Business Operations) - 6 databases total:"
echo "  📊 customer_db: customers, orders, order_items (3 tables)"
echo "  📦 inventory_db: products, inventory, inventory_movements (3 tables)"
echo "  📈 analytics_db: sales_metrics, customer_analytics, product_analytics (3 tables)"
echo ""
echo "PG2 (HR & Finance) - 6 databases total:"
echo "  👥 hr_db: employees, attendance, performance_reviews, departments, leave_requests (5 tables)"
echo "  💰 finance_db: accounts, transactions, budget_allocations, invoices, expenses (5 tables)"
echo "  📋 reporting_db: daily_reports, kpi_tracking, executive_dashboard (3 tables)"
echo ""
echo -e "${GREEN}Total: 21 tables across 6 databases on 2 servers${NC}"
