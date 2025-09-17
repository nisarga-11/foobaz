#!/bin/bash
echo "🐧 LINUX SETUP FIX FOR SP-LAKEHOUSE-BACKUP"
echo "=========================================="
echo ""
echo "This script fixes common Linux issues with the backup system"
echo ""

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

echo "🔍 STEP 1: Checking PostgreSQL Setup"
echo "===================================="

# Check if PostgreSQL is installed
if command_exists psql; then
    echo "✅ PostgreSQL client installed"
    PSQL_VERSION=$(psql --version | head -n1)
    echo "   Version: $PSQL_VERSION"
else
    echo "❌ PostgreSQL client not found!"
    echo "   Install with: sudo yum install postgresql postgresql-contrib"
    echo "   Or: sudo apt install postgresql postgresql-contrib"
    exit 1
fi

# Check PostgreSQL service
if systemctl is-active --quiet postgresql; then
    echo "✅ PostgreSQL service is running"
else
    echo "⚠️  PostgreSQL service not running"
    echo "   Start with: sudo systemctl start postgresql"
    echo "   Enable: sudo systemctl enable postgresql"
fi

echo ""
echo "🔐 STEP 2: PostgreSQL Authentication Fix"
echo "========================================"

echo "The system is asking for passwords because of authentication setup."
echo "Here are the fixes:"
echo ""

echo "📝 Option A: Set PGPASSWORD environment variable"
echo "   export PGPASSWORD='your_postgres_password'"
echo "   Or add to ~/.bashrc: echo 'export PGPASSWORD=\"your_password\"' >> ~/.bashrc"
echo ""

echo "📝 Option B: Create .pgpass file (Recommended)"
echo "   Create file: ~/.pgpass"
echo "   Content: localhost:5432:*:postgres:your_password"
echo "   Permissions: chmod 600 ~/.pgpass"
echo ""

echo "📝 Option C: Configure peer authentication"
echo "   Edit /var/lib/pgsql/data/pg_hba.conf or /etc/postgresql/*/main/pg_hba.conf"
echo "   Change: local all postgres md5"
echo "   To:     local all postgres peer"
echo "   Then: sudo systemctl restart postgresql"
echo ""

echo "🌐 STEP 3: Network/Firewall Check"
echo "================================="

# Check if ports are available
for port in 8001 8002 8003 8004; do
    if ss -tulpn | grep -q ":$port "; then
        echo "⚠️  Port $port is already in use"
        echo "   Process using port $port:"
        ss -tulpn | grep ":$port "
    else
        echo "✅ Port $port is available"
    fi
done

echo ""
echo "🔥 STEP 4: Firewall Configuration"
echo "================================="

if command_exists firewall-cmd; then
    echo "🔧 FirewallD detected - adding rules..."
    echo "   sudo firewall-cmd --permanent --add-port=8001-8004/tcp"
    echo "   sudo firewall-cmd --reload"
elif command_exists ufw; then
    echo "🔧 UFW detected - adding rules..."
    echo "   sudo ufw allow 8001:8004/tcp"
elif command_exists iptables; then
    echo "🔧 iptables detected - adding rules..."
    echo "   sudo iptables -A INPUT -p tcp --dport 8001:8004 -j ACCEPT"
else
    echo "✅ No common firewall detected - should be OK"
fi

echo ""
echo "🔧 STEP 5: Quick PostgreSQL Test"
echo "================================"

echo "Testing PostgreSQL connection..."
if sudo -u postgres psql -c "SELECT version();" >/dev/null 2>&1; then
    echo "✅ PostgreSQL connection works with sudo"
    echo "   Use: sudo -u postgres psql for manual access"
elif psql -U postgres -h localhost -c "SELECT version();" >/dev/null 2>&1; then
    echo "✅ PostgreSQL connection works with password"
else
    echo "❌ PostgreSQL connection failed"
    echo "   Check PostgreSQL status: sudo systemctl status postgresql"
    echo "   Check logs: sudo journalctl -u postgresql"
fi

echo ""
echo "🚀 STEP 6: Environment Setup"
echo "============================"

echo "Create/update your .env file:"
cat << 'EOF'
# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_actual_password_here

# MCP Server URLs
MCP1_BASE_URL=http://localhost:8003
MCP2_BASE_URL=http://localhost:8004

# Ollama Configuration  
OLLAMA_HOST=localhost
OLLAMA_PORT=11434
EOF

echo ""
echo "💡 STEP 7: Simplified Testing"
echo "============================="

echo "Instead of the full system, test components individually:"
echo ""
echo "1️⃣ Test PostgreSQL setup:"
echo "   ./setup_postgres.sh"
echo ""
echo "2️⃣ Test recommendation tool without full system:"
echo "   python -c \"from mcp_local.mcp_tools import create_mcp_toolset; print('Tools work!')\""
echo ""
echo "3️⃣ Test Ollama (if installed):"
echo "   curl -X POST http://localhost:11434/api/generate -d '{\"model\":\"llama2\",\"prompt\":\"test\"}'"
echo ""
echo "4️⃣ Test individual MCP server:"
echo "   python mcp_local/postgres_backup_server.py --server-name PG1"
echo ""

echo "🎯 RECOMMENDED LINUX WORKFLOW"
echo "============================="
echo ""
echo "1. Fix PostgreSQL authentication (choose Option A, B, or C above)"
echo "2. Set up .env file with your actual password"
echo "3. Test individual components before running full system"
echo "4. Use recommendation tool via CLI instead of full system"
echo ""
echo "Quick test command:"
echo "   python cli.py test-connection"
echo ""
