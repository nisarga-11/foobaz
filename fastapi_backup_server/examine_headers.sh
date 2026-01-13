#!/bin/bash
# Extract structure definitions from TSM headers

HEADER_DIR="/opt/tivoli/tsm/client/api/bin64/sample"

echo "========================================"
echo "Examining dsmApiVersionEx structure"
echo "========================================"
grep -A 10 "typedef struct" "$HEADER_DIR/dsmapitd.h" | grep -A 10 "dsmApiVersionEx"

echo ""
echo "========================================"
echo "Examining dsmInitExIn_t structure"
echo "========================================"
grep -A 30 "typedef struct" "$HEADER_DIR/dsmapitd.h" | grep -A 30 "dsmInitExIn_t"

echo ""
echo "========================================"
echo "Examining dsmInitExOut_t structure"
echo "========================================"
grep -A 20 "typedef struct" "$HEADER_DIR/dsmapitd.h" | grep -A 20 "dsmInitExOut_t"

echo ""
echo "========================================"
echo "Looking for DSM_MAX constants"
echo "========================================"
grep "#define DSM_MAX" "$HEADER_DIR/dsmapitd.h"

echo ""
echo "========================================"
echo "Looking for stVersion usage examples"
echo "========================================"
grep -i "stVersion" "$HEADER_DIR/dsmapitd.h" | head -20