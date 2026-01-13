#!/bin/bash
# Automated restore script

LOG_DIR=~/crew_mcp_project/crew_mcp_project/logs/restore_logs
mkdir -p $LOG_DIR
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
LOG_FILE=$LOG_DIR/restore_$TIMESTAMP.log

STANZAS=("db1_stanza" "db2_stanza")

for STANZA in "${STANZAS[@]}"; do
    echo "Restoring latest backup for stanza: $STANZA" | tee -a $LOG_FILE
    sudo -u postgres pgbackrest --stanza=$STANZA restore --type=default 2>&1 | tee -a $LOG_FILE
done

echo "All restores completed successfully" | tee -a $LOG_FILE
