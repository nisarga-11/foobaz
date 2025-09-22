#!/bin/bash

echo "🔧 PostgreSQL Authentication Fix Script"
echo "========================================"
echo ""
echo "This script will help you configure PostgreSQL to work without password prompts"
echo "for the backup system."
echo ""

# Function to check if running as root
check_root() {
    if [[ $EUID -eq 0 ]]; then
        echo "⚠️  Running as root. This is OK for system configuration."
        return 0
    else
        echo "ℹ️  Running as regular user. Some commands may need sudo."
        return 1
    fi
}

# Function to find PostgreSQL data directory
find_pg_data_dir() {
    echo "🔍 Finding PostgreSQL data directory..."
    
    # Common locations
    local possible_dirs=(
        "/var/lib/postgresql/data"
        "/var/lib/pgsql/data"
        "/usr/local/var/postgres"
        "/opt/homebrew/var/postgres"
        "/var/lib/postgresql/*/main"
        "/etc/postgresql/*/main"
    )
    
    for dir in "${possible_dirs[@]}"; do
        if [[ -d "$dir" ]]; then
            echo "   Found: $dir"
            echo "$dir"
            return 0
        fi
    done
    
    # Try to get from postgres process
    local pg_data=$(ps aux | grep postgres | grep -o '\-D [^ ]*' | head -1 | cut -d' ' -f2)
    if [[ -n "$pg_data" && -d "$pg_data" ]]; then
        echo "   Found from process: $pg_data"
        echo "$pg_data"
        return 0
    fi
    
    echo "   ❌ Could not find PostgreSQL data directory"
    return 1
}

# Function to backup pg_hba.conf
backup_pg_hba() {
    local pg_data_dir="$1"
    local pg_hba="$pg_data_dir/pg_hba.conf"
    
    if [[ -f "$pg_hba" ]]; then
        local backup_file="${pg_hba}.backup.$(date +%Y%m%d_%H%M%S)"
        echo "📋 Backing up pg_hba.conf to: $backup_file"
        
        if cp "$pg_hba" "$backup_file"; then
            echo "   ✅ Backup created successfully"
            return 0
        else
            echo "   ❌ Failed to create backup"
            return 1
        fi
    else
        echo "   ❌ pg_hba.conf not found at: $pg_hba"
        return 1
    fi
}

# Function to fix pg_hba.conf
fix_pg_hba() {
    local pg_data_dir="$1"
    local pg_hba="$pg_data_dir/pg_hba.conf"
    
    echo "🔧 Fixing pg_hba.conf authentication..."
    
    # Create a temporary file with the fixed configuration
    local temp_file=$(mktemp)
    
    # Process the file line by line
    while IFS= read -r line; do
        # Skip comments and empty lines
        if [[ "$line" =~ ^[[:space:]]*# ]] || [[ -z "${line// }" ]]; then
            echo "$line" >> "$temp_file"
            continue
        fi
        
        # Fix local connections for postgres user
        if [[ "$line" =~ local[[:space:]]+all[[:space:]]+postgres[[:space:]]+ident ]]; then
            echo "   🔄 Changing: $line"
            echo "   ➡️  To: local   all             postgres                                peer" >> "$temp_file"
        elif [[ "$line" =~ local[[:space:]]+all[[:space:]]+postgres[[:space:]]+md5 ]]; then
            echo "   ✅ Already configured: $line"
            echo "$line" >> "$temp_file"
        elif [[ "$line" =~ local[[:space:]]+all[[:space:]]+postgres[[:space:]]+peer ]]; then
            echo "   ✅ Already configured: $line"
            echo "$line" >> "$temp_file"
        else
            echo "$line" >> "$temp_file"
        fi
    done < "$pg_hba"
    
    # Replace the original file
    if cp "$temp_file" "$pg_hba"; then
        echo "   ✅ pg_hba.conf updated successfully"
        rm "$temp_file"
        return 0
    else
        echo "   ❌ Failed to update pg_hba.conf"
        rm "$temp_file"
        return 1
    fi
}

# Function to restart PostgreSQL
restart_postgres() {
    echo "🔄 Restarting PostgreSQL service..."
    
    # Try different service management systems
    if command -v systemctl >/dev/null 2>&1; then
        echo "   Using systemctl..."
        if systemctl restart postgresql; then
            echo "   ✅ PostgreSQL restarted successfully"
            return 0
        else
            echo "   ❌ Failed to restart PostgreSQL with systemctl"
        fi
    fi
    
    if command -v service >/dev/null 2>&1; then
        echo "   Using service command..."
        if service postgresql restart; then
            echo "   ✅ PostgreSQL restarted successfully"
            return 0
        else
            echo "   ❌ Failed to restart PostgreSQL with service"
        fi
    fi
    
    echo "   ⚠️  Please restart PostgreSQL manually:"
    echo "      sudo systemctl restart postgresql"
    echo "      or"
    echo "      sudo service postgresql restart"
    return 1
}

# Function to test connection
test_connection() {
    echo "🧪 Testing PostgreSQL connection..."
    
    # Test with postgres user
    if psql -U postgres -h localhost -c "SELECT version();" >/dev/null 2>&1; then
        echo "   ✅ Connection successful with postgres user"
        return 0
    else
        echo "   ❌ Connection failed with postgres user"
        echo "   💡 Try running as postgres user: sudo -u postgres psql"
        return 1
    fi
}

# Main execution
main() {
    echo "Starting PostgreSQL authentication fix..."
    echo ""
    
    # Find PostgreSQL data directory
    local pg_data_dir
    if ! pg_data_dir=$(find_pg_data_dir); then
        echo "❌ Cannot proceed without PostgreSQL data directory"
        echo ""
        echo "💡 Manual steps:"
        echo "   1. Find your PostgreSQL data directory"
        echo "   2. Edit pg_hba.conf"
        echo "   3. Change 'ident' to 'peer' for postgres user"
        echo "   4. Restart PostgreSQL"
        exit 1
    fi
    
    echo ""
    
    # Backup pg_hba.conf
    if ! backup_pg_hba "$pg_data_dir"; then
        echo "❌ Cannot proceed without backing up pg_hba.conf"
        exit 1
    fi
    
    echo ""
    
    # Fix pg_hba.conf
    if ! fix_pg_hba "$pg_data_dir"; then
        echo "❌ Failed to fix pg_hba.conf"
        exit 1
    fi
    
    echo ""
    
    # Restart PostgreSQL
    restart_postgres
    
    echo ""
    
    # Test connection
    test_connection
    
    echo ""
    echo "🎉 PostgreSQL authentication fix completed!"
    echo ""
    echo "📝 Summary of changes:"
    echo "   • Removed -W flag from pg_basebackup commands"
    echo "   • Configured pg_hba.conf for peer authentication"
    echo "   • Restarted PostgreSQL service"
    echo ""
    echo "🚀 You can now run backup commands without password prompts!"
}

# Run main function
main "$@"
