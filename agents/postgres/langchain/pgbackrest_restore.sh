#!/bin/bash
# Simple pgBackRest restore helper script
# Usage: ./pgbackrest_restore.sh <backup_set_id>
# Where backup_set_id is something like 20250907-144732F_20250907-145745I

set -euo pipefail

# Configuration values (update these if your paths change)
PGDATA="/Users/aarthiprashanth/Library/Application Support/Postgres/var-17"
REPO="/Users/aarthiprashanth/backups/pgbackrest_final"
STANZA="demo"

# Ensure a backup set was provided
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <backup_set_id>"
  exit 1
fi
BACKUP_SET="$1"

# Stop PostgreSQL
echo "Stopping PostgreSQL at $PGDATA..."
pg_ctl stop -D "$PGDATA" || {
  echo "Failed to stop PostgreSQL. Check if the cluster is running and if pg_ctl is in PATH." >&2
  exit 1
}

# Perform the restore
echo "Restoring backup set $BACKUP_SET..."
PGBACKREST_PG1_PATH="$PGDATA" \
PGBACKREST_REPO1_PATH="$REPO" \
pgbackrest --stanza="$STANZA" --delta --db-path="$PGDATA" restore --set="$BACKUP_SET" || {
  echo "Restore failed. See pgBackRest logs for details." >&2
  exit 1
}

# Fix postgresql.auto.conf to include correct repo path
echo "Fixing PostgreSQL configuration..."
AUTO_CONF="$PGDATA/postgresql.auto.conf"
if [ -f "$AUTO_CONF" ]; then
  # Fix restore_command to include repo1-path
  sed -i '' 's|restore_command = '\''pgbackrest --pg1-path="/Users/aarthiprashanth/Library/Application Support/Postgres/var-17" --stanza=demo archive-get %f "%p"'\''|restore_command = '\''pgbackrest --pg1-path="/Users/aarthiprashanth/Library/Application Support/Postgres/var-17" --repo1-path=/Users/aarthiprashanth/backups/pgbackrest_final --stanza=demo archive-get %f "%p"'\''|g' "$AUTO_CONF"
  echo "Fixed restore_command in postgresql.auto.conf"
fi

# Start PostgreSQL
echo "Starting PostgreSQL..."
pg_ctl start -D "$PGDATA" || {
  echo "Failed to start PostgreSQL. Check logs for details." >&2
  exit 1
}

echo "Restore of $BACKUP_SET completed successfully."
