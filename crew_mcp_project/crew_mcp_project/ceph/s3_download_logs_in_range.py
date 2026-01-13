# ===============================
# s3_download_logs_in_range.py
# ===============================
import boto3
import os
import sys
from datetime import datetime, timezone

# ---- S3 connection details ----
AWS_ACCESS_KEY = "abc"
AWS_SECRET_KEY = "abc"
REGION = "us-east-1"
ENDPOINT_URL = "http://fenrir-vm158.storage.tucson.ibm.com:8080"
BUCKET_NAME = "dest-slog-bkt1"

# ---- Output ----
LOG_DIR = "/root/sp-lakehouse-backup/crew_mcp_project/crew_mcp_project/ceph/logs"
os.makedirs(LOG_DIR, exist_ok=True)

if len(sys.argv) != 3:
    print("Usage: python s3_download_logs_in_range.py <start_time> <end_time>")
    print("Example: python s3_download_logs_in_range.py 06/Nov/2025:04:00:00 06/Nov/2025:08:00:00")
    sys.exit(1)

time_fmt = "%d/%b/%Y:%H:%M:%S"
start_dt = datetime.strptime(sys.argv[1], time_fmt).replace(tzinfo=timezone.utc)
end_dt = datetime.strptime(sys.argv[2], time_fmt).replace(tzinfo=timezone.utc)

print("="*70)
print("  DOWNLOADING ALL LOG OBJECTS BETWEEN GIVEN TIME RANGE")
print("="*70)
print(f"⏰ Time Window: {start_dt} → {end_dt}")

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION,
    endpoint_url=ENDPOINT_URL
)

try:
    objects = s3.list_objects_v2(Bucket=BUCKET_NAME)
    if "Contents" not in objects:
        raise Exception("No objects found in bucket.")

    matching_objs = [
        obj for obj in objects["Contents"]
        if start_dt <= obj["LastModified"].astimezone(timezone.utc) <= end_dt
    ]

    if not matching_objs:
        print("⚠️ No logs found in that range.")
        sys.exit(0)

    print(f"✅ Found {len(matching_objs)} log file(s) in range.")

    combined_log_path = os.path.join(LOG_DIR, "latest-log.txt")
    with open(combined_log_path, "w", encoding="utf-8") as combined:
        for obj in sorted(matching_objs, key=lambda x: x["LastModified"]):
            key = obj["Key"]
            local_path = os.path.join(LOG_DIR, key)
            print(f"⬇️  Downloading: {key}")
            s3.download_file(BUCKET_NAME, key, local_path)
            with open(local_path, "r", encoding="utf-8") as f:
                combined.write(f.read())
                combined.write("\n")
    print(f"📄 Combined log saved to: {combined_log_path}")

except Exception as e:
    print(f"[ERROR] Failed to download logs: {e}")
