# ===========================
# timebasedparser_rclone.py (Corrected Final Version)
# ===========================
import sys
import os
import subprocess
from datetime import datetime, timezone

# ---- Rclone Remote Config ----
REMOTE_NAME = "remote"
LOG_BUCKET = "dest-slog-bkt1"
SRC_BUCKET = "src-slog-bkt1"
DEST_BUCKET = "dest-data-bkt1"     # optional remote delete target

# ---- Paths ----
BASE_DIR = "/root/sp-lakehouse-backup/crew_mcp_project/crew_mcp_project/ceph"
LOG_FILE = os.path.join(BASE_DIR, "logs", "latest-log.txt")
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(os.path.join(BASE_DIR, "logs"), exist_ok=True)

# ---- Input args ----
if len(sys.argv) < 3 or len(sys.argv) > 4:
    print("Usage: python timebasedparser_rclone.py <start_time> <end_time> [operation]")
    print("Example: python timebasedparser_rclone.py 10/Nov/2025:09:38:00 10/Nov/2025:09:41:00 PUT")
    sys.exit(1)

start_str, end_str = sys.argv[1], sys.argv[2]
operation_filter = sys.argv[3].upper() if len(sys.argv) == 4 else "ALL"

time_fmt = "%d/%b/%Y:%H:%M:%S"
start_dt = datetime.strptime(start_str, time_fmt).replace(tzinfo=timezone.utc)
end_dt = datetime.strptime(end_str, time_fmt).replace(tzinfo=timezone.utc)

print("\n" + "🪟" * 30)
print("  S3 LOG PARSER & OPERATION MIRROR (USING RCLONE)")
print("🪟" * 30)
print(f"\n⏰ Time Range: {start_str} → {end_str}")
print(f"📁 Download Directory: {DOWNLOAD_DIR}")
print(f"📄 Log File: {LOG_FILE}")
print(f"⚙️  Operation Filter: {operation_filter}")

# --------------------------------------------------
# STEP 0: Download All Log Files in Time Range
# --------------------------------------------------
print("\n" + "="*70)
print("  STEP 0: DOWNLOADING ALL LOGS IN TIME RANGE")
print("="*70)

try:
    result = subprocess.run(
        ["rclone", "lsf", f"{REMOTE_NAME}:{LOG_BUCKET}"],
        capture_output=True, text=True, check=True
    )
    all_keys = [line.strip() for line in result.stdout.splitlines() if line.strip()]
except subprocess.CalledProcessError as e:
    print(f"❌ Failed to list logs: {e.stderr}")
    sys.exit(1)

matched_logs = []
for key in all_keys:
    if not key.startswith("src-slog-bkt1-"):
        continue
    parts = key.split("-")
    if len(parts) < 9:
        continue
    try:
        year, month, day = parts[3], parts[4], parts[5]
        hour, minute, sec = parts[6], parts[7], parts[8]
        log_dt = datetime(int(year), int(month), int(day),
                          int(hour), int(minute), int(sec),
                          tzinfo=timezone.utc)
        if start_dt <= log_dt <= end_dt:
            matched_logs.append(key)
            print(f"✅ Matched: {key}")
    except Exception:
        continue

if not matched_logs:
    print(f"⚠️ No logs found between {start_str} and {end_str}")
    sys.exit(0)

all_content = []
for key in matched_logs:
    dest_path = os.path.join(BASE_DIR, "logs", os.path.basename(key))
    try:
        subprocess.run([
            "rclone", "copyto",
            f"{REMOTE_NAME}:{LOG_BUCKET}/{key}",
            dest_path
        ], check=True)
        print(f"   ✓ Downloaded: {key}")
        with open(dest_path, "r", encoding="utf-8") as f:
            all_content.append(f.read())
    except Exception as e:
        print(f"   ✗ Failed to download {key}: {e}")

with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write("\n".join(all_content))

print(f"✅ Combined logs saved to: {LOG_FILE}")

# --------------------------------------------------
# STEP 2: Parse Log File
# --------------------------------------------------
print("\n" + "="*70)
print("  STEP 2: PARSING LOG FILE & BUILDING OPERATION TIMELINE")
print("="*70)

def parse_time(time_str):
    time_str = time_str.lstrip("[")
    return datetime.strptime(time_str, "%d/%b/%Y:%H:%M:%S").replace(tzinfo=timezone.utc)

operations = []

with open(LOG_FILE, "r", encoding="utf-8") as f:
    for line in f:
        parts = line.split()
        if len(parts) < 6:
            continue

        try:
            log_time = parse_time(parts[2])
        except Exception:
            continue

        if not (start_dt <= log_time <= end_dt):
            continue

        obj_name = parts[4]
        action_field = parts[5].upper()

        if "PUT" in action_field:
            op = "PUT"
        elif "GET" in action_field:
            op = "GET"
        elif "DELETE" in action_field:
            op = "DELETE"
        else:
            continue

        if operation_filter != "ALL" and op != operation_filter:
            continue

        operations.append((log_time, obj_name, op))

if not operations:
    print(f"⚠️ No matching operations found.")
    sys.exit(0)

operations.sort(key=lambda x: x[0])

print(f"✅ Found {len(operations)} operation(s):")
for log_time, obj, op in operations:
    print(f"   • [{log_time.strftime('%d/%b/%Y:%H:%M:%S')}] {obj} → {op}")

# --------------------------------------------------
# STEP 3: Mirror Operations (Corrected Logic)
# --------------------------------------------------
print("\n" + "="*70)
print("  STEP 3: EXECUTING OPERATIONS")
print("="*70)

for log_time, obj_name, op in operations:
    time_str = log_time.strftime("%d/%b/%Y:%H:%M:%S")
    local_path = os.path.join(DOWNLOAD_DIR, os.path.basename(obj_name))

    if op == "PUT":
        print(f"⬇️  [{time_str}] PUT: Downloading {obj_name}...")
        subprocess.run([
            "rclone", "copyto",
            f"{REMOTE_NAME}:{SRC_BUCKET}/{obj_name}",
            local_path
        ])

    elif op == "GET":
        print(f"📥 [{time_str}] GET: Fetching {obj_name}...")
        subprocess.run([
            "rclone", "copyto",
            f"{REMOTE_NAME}:{SRC_BUCKET}/{obj_name}",
            local_path
        ])

    elif op == "DELETE":
        print(f"🗑️  [{time_str}] DELETE: Removing {obj_name}...")

        # ---- Step 1: delete locally ----
        if os.path.exists(local_path):
            os.remove(local_path)
            print(f"   ✓ Local delete successful: {local_path}")
        else:
            print(f"   ▪ Local file not present, skipping local delete.")

        # ---- Step 2: delete on remote only if present ----
        remote_obj = f"{REMOTE_NAME}:{DEST_BUCKET}/{obj_name}"
        check = subprocess.run(
            ["rclone", "lsf", f"{REMOTE_NAME}:{DEST_BUCKET}", "--include", obj_name],
            capture_output=True, text=True
        )

        if obj_name in check.stdout:
            subprocess.run(["rclone", "deletefile", remote_obj])
            print(f"   ✓ Remote delete successful: {remote_obj}")
        else:
            print(f"   ▪ Remote object not found, skipping remote delete.")

print("\n✅ Operation mirroring completed successfully.")
print(f"📄 Logs: {LOG_FILE}")
print(f"📁 Downloads: {DOWNLOAD_DIR}")
