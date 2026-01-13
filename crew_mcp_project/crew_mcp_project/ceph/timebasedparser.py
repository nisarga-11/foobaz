# ===========================
# timebasedparser.py (MIRROR S3 OPERATIONS)
# ===========================
import sys
import os
import boto3
from datetime import datetime, timezone

# ---- S3 connection ----
AWS_ACCESS_KEY = "abc"
AWS_SECRET_KEY = "abc"
REGION = "us-east-1"
ENDPOINT_URL = "http://fenrir-vm158.storage.tucson.ibm.com:8080"
LOG_BUCKET = "dest-slog-bkt1"   # ← Logs are stored here
SRC_BUCKET = "src-slog-bkt1"    # ← Data objects are stored here

# ---- Paths ----
BASE_DIR = "/root/sp-lakehouse-backup/crew_mcp_project/crew_mcp_project/ceph"
LOG_FILE = os.path.join(BASE_DIR, "logs", "latest-log.txt")
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(os.path.join(BASE_DIR, "logs"), exist_ok=True)

# ---- Input args ----
if len(sys.argv) < 3 or len(sys.argv) > 4:
    print("Usage: python timebasedparser.py <start_time> <end_time> [operation]")
    print("Example: python timebasedparser.py 06/Nov/2025:04:00:00 06/Nov/2025:05:00:00 PUT")
    print("Supported operations: PUT, GET, DELETE (default = ALL)")
    sys.exit(1)

start_str, end_str = sys.argv[1], sys.argv[2]
operation_filter = sys.argv[3].upper() if len(sys.argv) == 4 else "ALL"

time_fmt = "%d/%b/%Y:%H:%M:%S"
start_dt = datetime.strptime(start_str, time_fmt).replace(tzinfo=timezone.utc)
end_dt = datetime.strptime(end_str, time_fmt).replace(tzinfo=timezone.utc)

print("\n" + "🪟" * 30)
print("  S3 LOG PARSER & OPERATION MIRROR (PUT/GET/DELETE)")
print("🪟" * 30)
print(f"\n⏰ Time Range: {start_str} → {end_str}")
print(f"📁 Download Directory: {DOWNLOAD_DIR}")
print(f"📄 Log File: {LOG_FILE}")
print(f"⚙️  Operation Filter: {operation_filter}")

# --------------------------------------------------
# STEP 0: Download all log files in time range from S3
# --------------------------------------------------
print("\n" + "="*70)
print("  STEP 0: DOWNLOADING ALL LOGS IN TIME RANGE FROM S3")
print("="*70)

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION,
    endpoint_url=ENDPOINT_URL
)

# List all objects in LOG bucket
try:
    response = s3.list_objects_v2(Bucket=LOG_BUCKET)
except Exception as e:
    print(f"❌ Failed to list objects in {LOG_BUCKET}: {e}")
    sys.exit(1)

if 'Contents' not in response:
    print(f"⚠️ No logs found in bucket {LOG_BUCKET}.")
    sys.exit(0)

# Parse log filenames: src-slog-bkt1-2025-11-10-06-46-01-XAEG0JCAOA209XT9
matched_logs = []
for obj in response['Contents']:
    key = obj['Key']
    
    # Skip non-log files
    if not key.startswith('src-slog-bkt1-'):
        continue
    
    # Extract timestamp: src-slog-bkt1-YYYY-MM-DD-HH-MM-SS-...
    parts = key.split('-')
    if len(parts) < 9:
        continue
    
    try:
        year, month, day = parts[3], parts[4], parts[5]
        hour, minute, sec = parts[6], parts[7], parts[8]
        
        log_dt = datetime(
            int(year), int(month), int(day),
            int(hour), int(minute), int(sec),
            tzinfo=timezone.utc
        )
        
        if start_dt <= log_dt <= end_dt:
            matched_logs.append(key)
            print(f"✅ Matched: {key}")
    except Exception as e:
        print(f"⚠️ Could not parse timestamp from {key}: {e}")
        continue

if not matched_logs:
    print(f"⚠️ No logs found between {start_str} and {end_str}")
    sys.exit(0)

# Download and combine all matched logs
print(f"\n📥 Downloading {len(matched_logs)} log file(s)...")

all_content = []
for log_key in matched_logs:
    try:
        response = s3.get_object(Bucket=LOG_BUCKET, Key=log_key)
        content = response['Body'].read().decode('utf-8')
        all_content.append(content)
        print(f"   ✓ Downloaded: {log_key}")
    except Exception as e:
        print(f"   ✗ Failed: {log_key} - {e}")

# Write combined logs to latest-log.txt
with open(LOG_FILE, 'w') as f:
    f.write('\n'.join(all_content))

print(f"✅ Combined logs saved to: {LOG_FILE}")

# --------------------------------------------------
# STEP 1: (REMOVED) — Cleaning old files in downloads directory
# --------------------------------------------------
"""
print("\n" + "="*70)
print("  STEP 1: CLEANING OLD FILES IN DOWNLOADS DIRECTORY")
print("="*70)

for file_name in os.listdir(DOWNLOAD_DIR):
    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    try:
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f"🧹 Deleted old file: {file_name}")
    except Exception as e:
        print(f"⚠️ Could not delete {file_name}: {e}")
"""
print("\n⏭️ Skipping STEP 1 (No cleanup — keeping existing files intact).")

# --------------------------------------------------
# STEP 2: Parse log file and build chronological operations
# --------------------------------------------------
print("\n" + "="*70)
print("  STEP 2: PARSING LOG FILE & BUILDING OPERATION TIMELINE")
print("="*70)

if not os.path.exists(LOG_FILE):
    print("❌ Log file missing after download.")
    sys.exit(1)

def parse_time(time_str):
    """Parses time like '[10/Nov/2025:05:14:10'"""
    time_str = time_str.lstrip("[")
    return datetime.strptime(time_str, "%d/%b/%Y:%H:%M:%S").replace(tzinfo=timezone.utc)

# Store all operations with timestamps
operations = []  # [(timestamp, object_name, operation), ...]

with open(LOG_FILE, "r", encoding="utf-8") as f:
    for line in f:
        parts = line.split()
        
        if len(parts) < 6:
            continue

        # Extract timestamp from parts[2]: [10/Nov/2025:05:14:10
        raw_time = parts[2].lstrip("[")
        try:
            log_time = parse_time(raw_time)
        except Exception:
            continue

        if not (start_dt <= log_time <= end_dt):
            continue

        # Object name is at parts[4], operation is at parts[5]
        obj_name = parts[4]
        action_field = parts[5]

        operation = ""
        if "PUT" in action_field.upper():
            operation = "PUT"
        elif "GET" in action_field.upper():
            operation = "GET"
        elif "DELETE" in action_field.upper():
            operation = "DELETE"
        else:
            continue

        if operation_filter != "ALL" and operation != operation_filter:
            continue

        operations.append((log_time, obj_name, operation))

if not operations:
    print(f"⚠️ No matching {operation_filter} operations found between {start_str} and {end_str}")
    sys.exit(0)

# Sort operations chronologically
operations.sort(key=lambda x: x[0])

print(f"✅ Found {len(operations)} operation(s) (chronologically sorted):")
for log_time, obj, op in operations:
    time_str = log_time.strftime("%d/%b/%Y:%H:%M:%S")
    print(f"   • [{time_str}] {obj} → {op}")

# --------------------------------------------------
# STEP 3: Execute operations in chronological order
# --------------------------------------------------
print("\n" + "="*70)
print("  STEP 3: MIRRORING OPERATIONS LOCALLY")
print("="*70)

downloaded_files = set()  # Track what we've downloaded

for log_time, obj_name, operation in operations:
    local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(obj_name))
    time_str = log_time.strftime("%d/%b/%Y:%H:%M:%S")
    
    if operation == "PUT":
        try:
            s3.download_file(SRC_BUCKET, obj_name, local_path)
            downloaded_files.add(obj_name)
            print(f"⬇️  [{time_str}] PUT: Downloaded {obj_name} → {local_path}")
        except s3.exceptions.NoSuchKey:
            print(f"⚠️  [{time_str}] PUT: Object {obj_name} no longer exists in S3")
        except Exception as e:
            print(f"❌ [{time_str}] PUT: Failed to download {obj_name}: {e}")
    
    elif operation == "DELETE":
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
                downloaded_files.discard(obj_name)
                print(f"🗑️  [{time_str}] DELETE: Removed {obj_name} from downloads")
            except Exception as e:
                print(f"❌ [{time_str}] DELETE: Failed to remove {obj_name}: {e}")
        else:
            print(f"⚠️  [{time_str}] DELETE: {obj_name} not found in downloads (already deleted or never downloaded)")
    
    elif operation == "GET":
        if obj_name not in downloaded_files:
            try:
                s3.download_file(SRC_BUCKET, obj_name, local_path)
                downloaded_files.add(obj_name)
                print(f"⬇️  [{time_str}] GET: Downloaded {obj_name} → {local_path}")
            except s3.exceptions.NoSuchKey:
                print(f"⚠️  [{time_str}] GET: Object {obj_name} no longer exists in S3")
            except Exception as e:
                print(f"❌ [{time_str}] GET: Failed to download {obj_name}: {e}")
        else:
            print(f"ℹ️  [{time_str}] GET: {obj_name} already in downloads")

print("\n✅ Operation mirroring completed successfully.")
print(f"\n📊 Summary:")
print(f"   • Logs saved to: {LOG_FILE}")
print(f"   • Final state in: {DOWNLOAD_DIR}")
print(f"   • Files remaining: {len(downloaded_files)}")
if downloaded_files:
    print(f"   • Remaining files:")
    for f in downloaded_files:
        print(f"     - {f}")
