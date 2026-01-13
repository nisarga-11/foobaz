#!/bin/bash

CLUSTER1_DIR="/var/lib/pgsql/17/data"
CLUSTER2_DIR="/var/lib/pgsql/17/data_db1"

echo "Checking PostgreSQL cluster 1 (port 5433)..."
sudo -u postgres /usr/pgsql-17/bin/pg_ctl status -D $CLUSTER1_DIR || \
sudo -u postgres /usr/pgsql-17/bin/pg_ctl start -D $CLUSTER1_DIR -l $CLUSTER1_DIR/log/server.log

echo "Checking PostgreSQL cluster 2 (port 5434)..."
sudo -u postgres /usr/pgsql-17/bin/pg_ctl status -D $CLUSTER2_DIR || \
sudo -u postgres /usr/pgsql-17/bin/pg_ctl start -D $CLUSTER2_DIR -l $CLUSTER2_DIR/log/server.log

echo "To connect to cluster1:"
echo "  sudo -u postgres psql -p 5433 -d postgres"

echo "To connect to cluster2:"
echo "  sudo -u postgres psql -p 5434 -d postgres"
