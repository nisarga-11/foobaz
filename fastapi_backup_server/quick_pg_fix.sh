#!/bin/bash
# Quick diagnostic and fix for PostgreSQL backup issue

echo "=========================================="
echo "PostgreSQL Backup Diagnostic"
echo "=========================================="

# Check 1: Is PostgreSQL installed?
echo -e "\n[1] Checking PostgreSQL installation..."
if command -v psql &> /dev/null; then
    echo "✓ psql found: $(which psql)"
    psql --version
else
    echo "✗ psql NOT found - PostgreSQL client not installed"
    echo "Fix: yum install postgresql postgresql-contrib"
    exit 1
fi

if command -v pg_dump &> /dev/null; then
    echo "✓ pg_dump found: $(which pg_dump)"
else
    echo "✗ pg_dump NOT found"
    exit 1
fi

# Check 2: Is PostgreSQL running?
echo -e "\n[2] Checking PostgreSQL service..."
if systemctl is-active --quiet postgresql; then
    echo "✓ PostgreSQL service is running"
elif systemctl is-active --quiet postgresql-*.service; then
    echo "✓ PostgreSQL service is running"
else
    echo "✗ PostgreSQL service is NOT running"
    echo "Fix: systemctl start postgresql"
    exit 1
fi

# Check 3: Can we connect?
echo -e "\n[3] Testing connection to PostgreSQL..."
if psql -U postgres -h localhost -p 5432 -c "SELECT 1;" 2>&1 | grep -q "connection to server"; then
    echo "✗ Cannot connect - authentication issue"
    echo ""
    echo "COMMON REASONS:"
    echo "  1. Wrong password"
    echo "  2. pg_hba.conf doesn't allow connection"
    echo "  3. PostgreSQL not listening on localhost:5432"
    echo ""
    echo "TRY THESE FIXES:"
    echo ""
    echo "Fix 1: Set password environment variable"
    echo "  export PGPASSWORD='postgres'"
    echo "  OR"
    echo "  export PGPASSWORD='your_actual_password'"
    echo ""
    echo "Fix 2: Create .pgpass file"
    echo "  echo 'localhost:5432:*:postgres:postgres' > ~/.pgpass"
    echo "  chmod 600 ~/.pgpass"
    echo ""
    echo "Fix 3: Check pg_hba.conf (requires root/postgres user)"
    echo "  Find pg_hba.conf: sudo find /var /etc -name pg_hba.conf 2>/dev/null"
    echo "  Add this line: host all postgres 127.0.0.1/32 trust"
    echo "  Then: systemctl restart postgresql"
    
    # Try with password prompt suppressed
    echo -e "\n[3a] Trying connection WITH password..."
    PGPASSWORD='postgres' psql -U postgres -h localhost -p 5432 -c "SELECT 1;" 2>&1 | head -5
elif psql -U postgres -h localhost -p 5432 -c "SELECT 1;" &> /dev/null; then
    echo "✓ Connection successful!"
else
    echo "? Connection test unclear, trying with PGPASSWORD..."
    PGPASSWORD='postgres' psql -U postgres -h localhost -p 5432 -c "SELECT 1;" 2>&1
fi

# Check 4: Does users_db exist?
echo -e "\n[4] Checking if users_db exists..."
if PGPASSWORD='postgres' psql -U postgres -h localhost -p 5432 -l 2>/dev/null | grep -q users_db; then
    echo "✓ users_db exists"
elif psql -U postgres -h localhost -p 5432 -l 2>/dev/null | grep -q users_db; then
    echo "✓ users_db exists"
else
    echo "✗ users_db does NOT exist"
    echo ""
    echo "LIST OF DATABASES:"
    PGPASSWORD='postgres' psql -U postgres -h localhost -p 5432 -l 2>/dev/null || psql -U postgres -h localhost -p 5432 -l 2>/dev/null
    echo ""
    echo "Fix: Create the database"
    echo "  PGPASSWORD='postgres' psql -U postgres -h localhost -c \"CREATE DATABASE users_db;\""
fi

# Check 5: Test pg_dump
echo -e "\n[5] Testing pg_dump on users_db..."
PGPASSWORD='postgres' pg_dump -U postgres -h localhost -p 5432 users_db 2>&1 | head -10
EXIT_CODE=${PIPESTATUS[0]}

if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ pg_dump works!"
else
    echo "✗ pg_dump failed with exit code: $EXIT_CODE"
    echo ""
    echo "Full error:"
    PGPASSWORD='postgres' pg_dump -U postgres -h localhost -p 5432 users_db 2>&1
fi

echo -e "\n=========================================="
echo "Diagnostic Complete"
echo "=========================================="