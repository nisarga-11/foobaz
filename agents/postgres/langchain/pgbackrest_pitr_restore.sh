#!/bin/bash
# pgBackRest Point-in-Time Recovery (PITR) restore script
# Usage: ./pgbackrest_pitr_restore.sh <target_time>
# Example: ./pgbackrest_pitr_restore.sh "2025-09-07 15:20:00+05:30"

set -euo pipefail

# Configuration values
PGDATA="/Users/aarthiprashanth/Library/Application Support/Postgres/var-17"
REPO="/Users/aarthiprashanth/backups/pgbackrest_final"
STANZA="demo"

# Ensure a target time was provided
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <target_time>"
  echo "Example: $0 \"2025-09-07 15:20:00+05:30\""
  echo ""
  echo "Available backup times:"
  echo "  - 2025-09-07 14:08:50+05:30 (full backup)"
  echo "  - 2025-09-07 19:19:19+05:30 (latest incremental)"
  echo ""
  echo "WAL coverage: 00000001000000010000009C to 00000011000000020000004C"
  exit 1
fi

TARGET_TIME="$1"

echo "=== pgBackRest Point-in-Time Recovery ==="
echo "Target time: $TARGET_TIME"
echo "PostgreSQL data directory: $PGDATA"
echo "Backup repository: $REPO"
echo "Stanza: $STANZA"
echo ""

# Stop PostgreSQL cleanly
echo "1. Stopping PostgreSQL..."
pg_ctl stop -D "$PGDATA" || {
  echo "Failed to stop PostgreSQL. Check if the cluster is running and if pg_ctl is in PATH." >&2
  exit 1
}

# Wait for postmaster.pid to be removed
echo "   Waiting for PostgreSQL to fully stop..."
sleep 2

# Perform PITR restore
echo "2. Performing Point-in-Time Recovery restore..."
echo "   Target: $TARGET_TIME"
echo "   This will restore the database to the exact state at that time"

PGBACKREST_PG1_PATH="$PGDATA" \
PGBACKREST_REPO1_PATH="$REPO" \
pgbackrest \
  --stanza="$STANZA" \
  --pg1-path="$PGDATA" \
  --repo1-path="$REPO" \
  restore \
  --type=time \
  --target="$TARGET_TIME" \
  --target-timeline=current \
  --target-action=promote \
  --delta || {
  echo "PITR restore failed. See pgBackRest logs for details." >&2
  exit 1
}

# Verify recovery.signal was created
if [ -f "$PGDATA/recovery.signal" ]; then
  echo "   ✓ recovery.signal created - PostgreSQL will enter recovery mode"
else
  echo "   ⚠ recovery.signal not found - this may indicate an issue"
fi

# Check postgresql.auto.conf for restore_command
if grep -q "restore_command.*pgbackrest.*archive-get" "$PGDATA/postgresql.auto.conf"; then
  echo "   ✓ restore_command configured correctly"
else
  echo "   ⚠ restore_command may not be configured properly"
fi

# Start PostgreSQL
echo "3. Starting PostgreSQL..."
pg_ctl start -D "$PGDATA" || {
  echo "Failed to start PostgreSQL. Check logs for details." >&2
  exit 1
}

echo ""
echo "=== PITR Restore Complete ==="
echo "PostgreSQL has been restored to: $TARGET_TIME"
echo ""
echo "To verify the restore worked:"
echo "1. Check PostgreSQL logs for recovery messages:"
echo "   tail -f $PGDATA/log/postgresql-*.log"
echo ""
echo "2. Connect and verify your data:"
echo "   psql -d customer_db -c \"SELECT COUNT(*) FROM customers;\""
echo ""
echo "3. Look for recovery completion messages in the logs"
echo "   (should show 'consistent recovery state reached' and 'database system is ready')"
