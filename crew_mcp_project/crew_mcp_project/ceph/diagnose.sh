#!/bin/bash

echo "=== Rclone Diagnostics ==="
echo ""

echo "1. Rclone Version:"
rclone version
echo ""

echo "2. Available Remotes:"
rclone listremotes
echo ""

echo "3. Testing remote config:"
rclone config show remote
echo ""

echo "4. Listing root of remote:"
rclone lsf remote: --max-depth 1
echo ""

echo "5. Testing src-slog-bkt1:"
rclone lsd remote:src-slog-bkt1 2>&1
echo ""

echo "6. Testing dest-slog-bkt1:"
rclone lsd remote:dest-slog-bkt1 2>&1
echo ""

echo "7. Active RC jobs:"
curl -s -u admin:admin123 -X POST http://9.11.52.248:5572/job/list | python3 -m json.tool