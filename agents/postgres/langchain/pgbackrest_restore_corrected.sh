#!/bin/bash
# Corrected pgBackRest restore helper script
# Usage: ./pgbackrest_restore_corrected.sh <backup_set_id> [stanza]
# Where backup_set_id is something like 20250908-125120F_20250908-130131I
# Stanza defaults to customer_demo if not provided

set -euo pipefail

# Configuration values (matching your actual pgbackrest.conf)
CUSTOMER_PGDATA="/Users/aarthiprashanth/postgres/pg-customer"
EMPLOYEE_PGDATA="/Users/aarthiprashanth/postgres/pg-employee"
REPO="/Users/aarthiprashanth/pgbackrest-repo"
CUSTOMER_STANZA="customer_demo"
EMPLOYEE_STANZA="employee_demo"
CONFIG_FILE="/Users/aarthiprashanth/pgbackrest.conf"

# Ensure a backup set was provided
if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <backup_set_id> [stanza]"
  echo "Available stanzas: customer_demo, employee_demo"
  echo "Example: $0 20250908-125120F_20250908-130131I customer_demo"
  exit 1
fi

BACKUP_SET="$1"
STANZA="${2:-customer_demo}"

# Set PGDATA based on stanza
if [ "$STANZA" = "customer_demo" ]; then
  PGDATA="$CUSTOMER_PGDATA"
elif [ "$STANZA" = "employee_demo" ]; then
  PGDATA="$EMPLOYEE_PGDATA"
else
  echo "Error: Invalid stanza '$STANZA'. Must be 'customer_demo' or 'employee_demo'"
  exit 1
fi

echo "=== pgBackRest Restore ==="
echo "Backup set: $BACKUP_SET"
echo "Stanza: $STANZA"
echo "PostgreSQL data directory: $PGDATA"
echo "Backup repository: $REPO"
echo ""

# Stop PostgreSQL
echo "1. Stopping PostgreSQL at $PGDATA..."
pg_ctl stop -D "$PGDATA" || {
  echo "Failed to stop PostgreSQL. Check if the cluster is running and if pg_ctl is in PATH." >&2
  exit 1
}

# Perform the restore
echo "2. Restoring backup set $BACKUP_SET..."
PGBACKREST_PG1_PATH="$PGDATA" \
PGBACKREST_REPO1_PATH="$REPO" \
pgbackrest \
  --config="$CONFIG_FILE" \
  --stanza="$STANZA" \
  --pg1-path="$PGDATA" \
  --repo1-path="$REPO" \
  restore \
  --set="$BACKUP_SET" \
  --delta || {
  echo "Restore failed. See pgBackRest logs for details." >&2
  exit 1
}

# Start PostgreSQL
echo "3. Starting PostgreSQL..."
pg_ctl start -D "$PGDATA" || {
  echo "Failed to start PostgreSQL. Check logs for details." >&2
  exit 1
}

echo ""
echo "=== Restore Complete ==="
echo "PostgreSQL has been restored from backup set: $BACKUP_SET"
echo ""
echo "To verify the restore worked:"
echo "1. Check PostgreSQL logs for recovery messages"
echo "2. Connect and verify your data:"
if [ "$STANZA" = "customer_demo" ]; then
  echo "   psql -h localhost -p 5432 -d customer_db -c \"SELECT COUNT(*) FROM customers;\""
else
  echo "   psql -h localhost -p 5434 -d employee_db -c \"SELECT COUNT(*) FROM employees;\""
fi
