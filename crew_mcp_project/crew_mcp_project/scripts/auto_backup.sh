#!/bin/bash
LOG_DIR=~/crew_mcp_project/crew_mcp_project/logs/backup_logs
mkdir -p $LOG_DIR
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
LOG_FILE=$LOG_DIR/backup_$TIMESTAMP.log

STANZAS=("db1_stanza" "db2_stanza")

for STANZA in "${STANZAS[@]}"; do
    echo "Checking stanza: $STANZA" | tee -a $LOG_FILE
    sudo -u postgres pgbackrest --stanza=$STANZA check 2>&1 | tee -a $LOG_FILE

    echo "Running backup for stanza: $STANZA" | tee -a $LOG_FILE
    sudo -u postgres pgbackrest --stanza=$STANZA backup 2>&1 | tee -a $LOG_FILE
done

echo "All backups completed successfully" | tee -a $LOG_FILE
