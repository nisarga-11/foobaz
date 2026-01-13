#!/usr/bin/env python3
"""
S3 Log Parser & Local Mirror using Rclone RC API
Works with: /operations/list, /operations/copyfile, /operations/deletefile
PUT => Download object to local folder
DELETE => Remove local file only (NO remote deletion)
"""

import sys
import os
import requests
from datetime import datetime, timezone
import json

# ==========================================================
# CONFIGURATION
# ==========================================================
RC_URL = "http://9.11.52.248:5572"
USER = "admin"
PASS = "admin123"

REMOTE_SRC_BUCKET = "remote:src-slog-bkt1"
REMOTE_DEST_BUCKET = "remote:dest-slog-bkt1"

BASE_DIR = "/root/sp-lakehouse-backup/crew_mcp_project/crew_mcp_project/ceph"
LOG_DIR = os.path.join(BASE_DIR, "logs")
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
LOG_FILE = os.path.join(LOG_DIR, "latest-log.txt")

# Create directories
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ==========================================================
# ARGUMENT PARSING
# ==========================================================
def print_usage():
    print("\nUsage: python timebasedparser_final.py <start> <end> [operation]")
    print("\nExamples:")
    print("  python timebasedparser_final.py '01/Jan/2024:00:00:00' '31/Jan/2024:23:59:59'")
    print("  python timebasedparser_final.py '01/Jan/2024:00:00:00' '31/Jan/2024:23:59:59' PUT")
    print("  python timebasedparser_final.py '01/Jan/2024:00:00:00' '31/Jan/2024:23:59:59' DELETE")
    print("\nOperation filters: ALL (default), PUT, DELETE, GET")
    sys.exit(1)

if len(sys.argv) < 3:
    print_usage()

start_str = sys.argv[1]
end_str = sys.argv[2]
operation_filter = sys.argv[3].upper() if len(sys.argv) == 4 else "ALL"

# Validate operation filter
valid_ops = ["ALL", "PUT", "DELETE", "GET"]
if operation_filter not in valid_ops:
    print(f"❌ Invalid operation filter: {operation_filter}")
    print(f"   Valid options: {', '.join(valid_ops)}")
    sys.exit(1)

# Parse dates
time_fmt = "%d/%b/%Y:%H:%M:%S"
try:
    start_dt = datetime.strptime(start_str, time_fmt).replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end_str, time_fmt).replace(tzinfo=timezone.utc)
except ValueError as e:
    print(f"❌ Date parsing error: {e}")
    print(f"   Expected format: DD/MMM/YYYY:HH:MM:SS")
    print(f"   Example: 01/Jan/2024:00:00:00")
    sys.exit(1)

if start_dt > end_dt:
    print("❌ Start time must be before end time")
    sys.exit(1)

# ==========================================================
# HELPER FUNCTIONS
# ==========================================================
def test_connection():
    """Test rclone RC server connectivity"""
    try:
        resp = requests.post(
            f"{RC_URL}/rc/noop",
            auth=(USER, PASS),
            timeout=5
        )
        if resp.status_code == 200:
            return True
        return False
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def parse_time_str(s):
    """Parse time string from log"""
    s = s.lstrip("[")
    return datetime.strptime(s, "%d/%b/%Y:%H:%M:%S").replace(tzinfo=timezone.utc)

# ==========================================================
# HEADER PRINT
# ==========================================================
print("\n" + "🟦" * 60)
print("   S3 LOG PARSER & LOCAL MIRROR (USING RCLONE RC API)")
print("🟦" * 60)
print(f"⏰ Time Range: {start_str} → {end_str}")
print(f"📄 Log File: {LOG_FILE}")
print(f"📁 Downloads: {DOWNLOAD_DIR}")
print(f"⚙️  Operation Filter: {operation_filter}")
print(f"🔗 RC URL: {RC_URL} (user={USER})")

# Test connection
print(f"\n🔌 Testing connection to Rclone RC server...")
if not test_connection():
    print("❌ Cannot connect to Rclone RC server")
    print(f"   Make sure rclone is running at {RC_URL}")
    print(f"   Start with: rclone rcd --rc-addr=9.11.52.248:5572 --rc-user=admin --rc-pass=admin123 --rc-web-gui")
    sys.exit(1)
print("✅ Connected successfully\n")

# ==========================================================
# STEP 0: LIST LOG FILES WITH /operations/list
# ==========================================================
print("="*70)
print(" STEP 0: LISTING LOG FILES")
print("="*70)

try:
    resp = requests.post(
        f"{RC_URL}/operations/list",
        json={"fs": REMOTE_DEST_BUCKET, "remote": ""},
        auth=(USER, PASS),
        timeout=30
    )
    resp.raise_for_status()
except requests.exceptions.RequestException as e:
    print(f"❌ Failed to list files: {e}")
    sys.exit(1)

all_logs = resp.json().get("list", [])
print(f"📋 Found {len(all_logs)} total files in {REMOTE_DEST_BUCKET}")

matched = []

for item in all_logs:
    name = item["Name"]
    
    # Filter only log files
    if not name.startswith("src-slog-bkt1-"):
        continue

    # Parse timestamp from filename
    # Expected format: src-slog-bkt1-YYYY-MM-DD-HH-MM-SS
    parts = name.split("-")
    if len(parts) < 9:
        continue

    try:
        dt = datetime(
            int(parts[3]), int(parts[4]), int(parts[5]),
            int(parts[6]), int(parts[7]), int(parts[8]),
            tzinfo=timezone.utc,
        )
        if start_dt <= dt <= end_dt:
            matched.append((name, dt))
            print(f"✅ Matched: {name} ({dt.strftime('%Y-%m-%d %H:%M:%S')})")
    except (ValueError, IndexError):
        continue

if not matched:
    print(f"⚠️  No logs found in range {start_str} to {end_str}")
    print(f"   Available logs should match pattern: src-slog-bkt1-YYYY-MM-DD-HH-MM-SS")
    sys.exit(0)

print(f"\n📊 Total matched logs: {len(matched)}")

# ==========================================================
# STEP 1: DOWNLOAD MATCHED LOGS
# ==========================================================
print("\n" + "="*70)
print(" STEP 1: DOWNLOADING MATCHED LOGS")
print("="*70)

combined = ""
download_success = 0
download_failed = 0

for name, dt in matched:
    local_path = os.path.join(LOG_DIR, name)

    print(f"⬇️  Downloading {name} -> {local_path}")

    payload = {
        "srcFs": REMOTE_DEST_BUCKET,
        "srcRemote": name,
        "dstFs": LOG_DIR,
        "dstRemote": name
    }

    try:
        r = requests.post(
            f"{RC_URL}/operations/copyfile",
            json=payload,
            auth=(USER, PASS),
            timeout=60
        )

        if r.status_code != 200:
            print(f"   ❌ Failed: {r.status_code} - {r.text}")
            download_failed += 1
            continue

        download_success += 1
        
        # Read downloaded file
        if os.path.exists(local_path):
            with open(local_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
                combined += content
                if not content.endswith("\n"):
                    combined += "\n"
            print(f"   ✅ Downloaded ({len(content)} bytes)")
        else:
            print(f"   ⚠️  File downloaded but not found locally")
            
    except Exception as e:
        print(f"   ❌ Exception: {e}")
        download_failed += 1

print(f"\n📊 Download Summary: {download_success} succeeded, {download_failed} failed")

# Save combined log
with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write(combined)

print(f"✅ Combined logs saved: {LOG_FILE} ({len(combined)} bytes)")

# ==========================================================
# STEP 2: PARSE LOGS
# ==========================================================
print("\n" + "="*70)
print(" STEP 2: PARSING LOG FILE")
print("="*70)

ops = []
parse_errors = 0

with open(LOG_FILE, encoding="utf-8", errors="ignore") as f:
    for line_num, line in enumerate(f, 1):
        parts = line.split()
        if len(parts) < 6:
            continue

        try:
            dt = parse_time_str(parts[2])
        except (ValueError, IndexError):
            parse_errors += 1
            continue

        # Filter by time range
        if not (start_dt <= dt <= end_dt):
            continue

        obj = parts[4]
        opfield = parts[5].upper()

        # Determine operation type
        if "PUT" in opfield:
            op = "PUT"
        elif "DELETE" in opfield:
            op = "DELETE"
        elif "GET" in opfield:
            op = "GET"
        else:
            continue

        # Filter by operation type
        if operation_filter != "ALL" and operation_filter != op:
            continue

        ops.append((dt, obj, op))

ops.sort()

print(f"✅ Found {len(ops)} operations (parsed with {parse_errors} errors)")
print(f"\nOperations breakdown:")

put_ops = sum(1 for _, _, op in ops if op == "PUT")
del_ops = sum(1 for _, _, op in ops if op == "DELETE")
get_ops = sum(1 for _, _, op in ops if op == "GET")

print(f"   • PUT: {put_ops}")
print(f"   • DELETE: {del_ops}")
print(f"   • GET: {get_ops}")

if len(ops) > 0:
    print(f"\n📋 Operations list:")
    for i, (dt, obj, op) in enumerate(ops[:10], 1):  # Show first 10
        print(f"   {i}. [{dt.strftime('%Y-%m-%d %H:%M:%S')}] {obj} → {op}")
    
    if len(ops) > 10:
        print(f"   ... and {len(ops) - 10} more operations")

# ==========================================================
# STEP 3: EXECUTE OPERATIONS
# ==========================================================
print("\n" + "="*70)
print(" STEP 3: EXECUTING OPERATIONS")
print("="*70)

put_count = 0
del_count = 0
put_failed = 0
del_failed = 0

for dt, obj, op in ops:
    t = dt.strftime("%d/%b/%Y:%H:%M:%S")

    if op == "PUT":
        print(f"⬇️  [{t}] PUT → Downloading {obj}")

        payload = {
            "srcFs": REMOTE_SRC_BUCKET,
            "srcRemote": obj,
            "dstFs": DOWNLOAD_DIR,
            "dstRemote": obj
        }

        try:
            r = requests.post(
                f"{RC_URL}/operations/copyfile",
                json=payload,
                auth=(USER, PASS),
                timeout=120
            )

            if r.status_code != 200:
                print(f"   ❌ PUT failed: {r.status_code} - {r.text}")
                put_failed += 1
            else:
                put_count += 1
                print(f"   ✅ Downloaded to {os.path.join(DOWNLOAD_DIR, obj)}")
        except Exception as e:
            print(f"   ❌ Exception: {e}")
            put_failed += 1

    elif op == "DELETE":
        print(f"🗑️  [{t}] DELETE → Removing local {obj}")

        local_path = os.path.join(DOWNLOAD_DIR, obj)
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
                del_count += 1
                print(f"   ✅ Local deleted: {obj}")
            else:
                print(f"   ⚠️  Local object not found: {obj}")
                del_count += 1
        except Exception as e:
            print(f"   ❌ Delete failed: {e}")
            del_failed += 1

# ==========================================================
# FINAL SUMMARY
# ==========================================================
print("\n" + "="*70)
print(" EXECUTION SUMMARY")
print("="*70)
print(f"✅ PUT operations: {put_count} succeeded, {put_failed} failed")
print(f"🗑️  DELETE operations: {del_count} succeeded, {del_failed} failed")
print(f"📄 Log file: {LOG_FILE}")
print(f"📁 Downloads: {DOWNLOAD_DIR}")
print(f"🌐 Web GUI: {RC_URL}")
print("="*70)

if put_failed > 0 or del_failed > 0:
    print("⚠️  Some operations failed. Check the logs above for details.")
else:
    print("✅ All operations completed successfully!")

print()