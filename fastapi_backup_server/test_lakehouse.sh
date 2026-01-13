#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}🧪 Testing Lakehouse Orchestrator System${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Base URL
BASE_URL="http://localhost:8002"

# Function to print test results
print_test() {
    echo -e "${YELLOW}➤ Test: $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ Success${NC}"
    echo ""
}

print_error() {
    echo -e "${RED}✗ Failed${NC}"
    echo ""
}

# Test 1: Health Check
print_test "Health Check"
response=$(curl -s "$BASE_URL/health")
if echo "$response" | grep -q "healthy\|degraded"; then
    echo "$response" | jq '.'
    print_success
else
    echo "$response"
    print_error
fi

# Test 2: List Targets
print_test "List Available Targets"
curl -s "$BASE_URL/targets" | jq '.'
print_success

# Test 3: List Databases
print_test "List Databases"
curl -s "$BASE_URL/databases" | jq '.'
print_success

# Test 4: List Servers
print_test "List Servers"
curl -s "$BASE_URL/servers" | jq '.'
print_success

# Test 5: Full Backup - Single Database
print_test "Full Backup of users_db"
curl -s -X POST "$BASE_URL/backup" \
  -H "Content-Type: application/json" \
  -d '{
    "target": "users_db",
    "backup_type": "full"
  }' | jq '.'
print_success

# Test 6: Lakehouse Backup
print_test "Lakehouse Full Backup (PostgreSQL + Ceph)"
curl -s -X POST "$BASE_URL/backup" \
  -H "Content-Type: application/json" \
  -d '{
    "target": "lakehouse",
    "backup_type": "full"
  }' | jq '.'
print_success

# Test 7: Base Backup
print_test "Base Backup (Physical)"
curl -s -X POST "$BASE_URL/backup" \
  -H "Content-Type: application/json" \
  -d '{
    "target": "lakehouse",
    "backup_type": "base"
  }' | jq '.'
print_success

# Test 8: Incremental Backup
print_test "Incremental Backup (WAL)"
curl -s -X POST "$BASE_URL/backup" \
  -H "Content-Type: application/json" \
  -d '{
    "target": "lakehouse",
    "backup_type": "incremental"
  }' | jq '.'
print_success

# Test 9: Status - Single Database
print_test "Check users_db Status"
curl -s "$BASE_URL/status/users_db" | jq '.'
print_success

# Test 10: Status - Lakehouse
print_test "Check Lakehouse Status"
curl -s "$BASE_URL/status/lakehouse" | jq '.'
print_success

# Test 11: Status - Server Level
print_test "Check Server PG1 Status"
curl -s "$BASE_URL/status/PG1" | jq '.'
print_success

# Test 12: Ceph Status
print_test "Check Ceph Status"
curl -s "$BASE_URL/ceph/status" | jq '.'
print_success

# Test 13: Ceph Files
print_test "List Ceph Files"
curl -s "$BASE_URL/ceph/files" | jq '.'
print_success

echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}✓ All Tests Completed!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${YELLOW}📊 Next Steps:${NC}"
echo "1. Check backup files in: ./backups/"
echo "2. View interactive docs: http://localhost:8002/docs"
echo "3. Test restore operations"
echo "4. Configure S3 for Ceph log parsing"
echo ""