#!/bin/bash
# setup_sp_upload.sh

echo "=== IBM Storage Protect C Uploader Setup ==="

# Install dependencies
sudo apt-get update
sudo apt-get install -y libcurl4-openssl-dev build-essential

# Compile
gcc -o sp_upload sp_upload.c -lcurl

if [ $? -eq 0 ]; then
    echo "✓ Compilation successful!"
    
    # Create config file
    cat > sp_config.sh << 'EOF'
#!/bin/bash
# IBM Storage Protect Configuration

export SP_SERVER_URL="http://spserver.company.local:1580"
export SP_NODE_ID="APPLEBEES"
export SP_PASSWORD="admin_2345_admin"
export SP_BACKUP_DIR="/sp_backups/ceph_downloads"
EOF
    
    chmod +x sp_config.sh
    echo "✓ Created sp_config.sh - Edit with your credentials"
    echo ""
    echo "Next steps:"
    echo "1. Edit sp_config.sh with your Storage Protect details"
    echo "2. Run: source sp_config.sh"
    echo "3. Run: ./sp_upload downloads"
else
    echo "✗ Compilation failed"
    exit 1
fi