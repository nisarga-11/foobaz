import subprocess
import os

# Change these if your bucket names differ
SRC_BUCKET = "src-slog-bkt1"
DEST_BUCKET = "dest-slog-bkt1"
REMOTE = "remote"  # the Rclone remote you configured

def run_rclone_command(command):
    """Run any rclone shell command and return output."""
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print(result.stdout)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print("❌ Error executing command:", e.stderr)
        return None


def upload_file(local_path, bucket=DEST_BUCKET):
    """Uploads a file to Ceph via Rclone."""
    if not os.path.exists(local_path):
        print(f"⚠️ File not found: {local_path}")
        return
    command = f"rclone copy {local_path} {REMOTE}:{bucket} --progress"
    print(f"⬆️ Uploading {local_path} to {bucket} ...")
    run_rclone_command(command)


def delete_file(filename, bucket=DEST_BUCKET):
    """Deletes a file from Ceph via Rclone."""
    command = f"rclone delete {REMOTE}:{bucket}/{filename}"
    print(f"🗑️ Deleting {filename} from {bucket} ...")
    run_rclone_command(command)


def download_file(filename, dest_dir="/root/downloads", bucket=SRC_BUCKET):
    """Downloads a file from Ceph via Rclone."""
    os.makedirs(dest_dir, exist_ok=True)
    command = f"rclone copy {REMOTE}:{bucket}/{filename} {dest_dir} --progress"
    print(f"⬇️ Downloading {filename} to {dest_dir} ...")
    run_rclone_command(command)


if __name__ == "__main__":
    # Simple manual test
    test_file = "/root/test.txt"
    upload_file(test_file)
    download_file("test.txt")
    delete_file("test.txt")
