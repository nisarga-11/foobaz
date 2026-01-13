import requests
import json
from requests.auth import HTTPBasicAuth
import subprocess
from urllib.parse import urlencode

# ---------------------------
# RCLONE RC CONFIG
# ---------------------------
RC_HOST = "http://localhost:5572"
RC_USER = "admin"
RC_PASS = "admin123"

session = requests.Session()
session.auth = (RC_USER, RC_PASS)


# ---------------------------
# BASE RC CALL
# ---------------------------
def rc_call(cmd, params=None):
    if params is None:
        params = {}

    url = f"{RC_HOST}/{cmd}"

    try:
        r = session.post(url, json=params)
        r.raise_for_status()
        result = r.json() if r.text else {}
        return {'success': True, 'status_code': r.status_code, 'data': result}
    except Exception as e:
        print(f"RC ERROR: {e}")
        try:
            return {'success': False, 'status_code': r.status_code, 'data': r.json()}
        except:
            return {'success': False, 'status_code': r.status_code if 'r' in locals() else None, 'data': None}


# ---------------------------
# UPLOAD FILE - Using URL parameters + file upload
# ---------------------------
def upload_file(remote, remote_path, local_file):
    """Upload file by passing parameters in URL"""
    
    # Build URL with parameters
    params = {
        'fs': remote,
        'remote': remote_path,
        '_config': '{}'
    }
    url = f"{RC_HOST}/operations/uploadfile?{urlencode(params)}"
    
    print(f"\nUploading local file {local_file} → {remote}{remote_path}")
    print(f"URL: {url}")
    
    # Send only the file in multipart
    with open(local_file, "rb") as f:
        files = {'file': (local_file.split('/')[-1], f, 'application/octet-stream')}
        
        r = requests.post(
            url,
            auth=HTTPBasicAuth(RC_USER, RC_PASS),
            files=files
        )
    
    print(f"Upload response: {r.text} (status: {r.status_code})")
    
    try:
        r.raise_for_status()
        result = r.json() if r.text else {}
        return {'success': True, 'status_code': r.status_code, 'data': result}
    except Exception as e:
        print(f"Upload error: {e}")
        try:
            return {'success': False, 'status_code': r.status_code, 'data': r.json()}
        except:
            return {'success': False, 'status_code': r.status_code, 'data': None}


# ---------------------------
# DELETE FILE
# ---------------------------
def delete_file(remote, remote_path):
    print(f"\nDeleting: {remote}{remote_path}")
    return rc_call("operations/deletefile", {
        "fs": remote,
        "remote": remote_path
    })


# ---------------------------
# MOVE FILE
# ---------------------------
def move_file(src_remote, src_path, dst_remote, dst_path, async_job=False):
    print(f"\nMoving {src_path} → {dst_path}")
    return rc_call("operations/movefile", {
        "srcFs": src_remote,
        "srcRemote": src_path,
        "dstFs": dst_remote,
        "dstRemote": dst_path,
        "_async": async_job
    })


# ---------------------------
# COPY FILE
# ---------------------------
def copy_file(src_remote, src_path, dst_remote, dst_path, async_job=False):
    print(f"\nCopying {src_path} → {dst_path}")
    return rc_call("operations/copyfile", {
        "srcFs": src_remote,
        "srcRemote": src_path,
        "dstFs": dst_remote,
        "dstRemote": dst_path,
        "_async": async_job
    })


# ---------------------------
# LIST DIRECTORY
# ---------------------------
def list_dir(remote, remote_path=""):
    print(f"\nListing: {remote}{remote_path}")
    return rc_call("operations/list", {
        "fs": remote,
        "remote": remote_path
    })


# ---------------------------
# STREAM LOGS (optional)
# ---------------------------
def stream_logs():
    url = f"{RC_HOST}/core/command"

    payload = {
        "command": "log",
        "returnType": "STREAM_ONLY_STDOUT"
    }

    print("\n--- STREAMING LOGS (CTRL+C to exit) ---\n")

    with session.post(url, json=payload, stream=True) as r:
        for line in r.iter_lines():
            if line:
                print("LOG:", line.decode())


# ---------------------------
# MAIN PROGRAM
# ---------------------------
if __name__ == "__main__":

    # Connection test
    print("\n=== TESTING RC CONNECTION ===")
    print(rc_call("rc/noop", {"ping": "ok"}))

    # List sources
    print("\n=== LISTING DIRECTORY remote:src-slog-bkt1 ===")
    listing = list_dir("remote:", "src-slog-bkt1")
    if listing and listing.get('success'):
        items = listing['data'].get('list', [])
        print(f"✓ Found {len(items)} items (status: {listing['status_code']})")
        # Show first few items
        for item in items[:3]:
            print(f"  - {item['Name']} ({item['Size']} bytes)")
    else:
        print("✗ Listing failed")

    # Copy file async
    print("\n=== COPYING FILE (ASYNC) ===")
    copy_job = copy_file(
        "remote:",
        "src-slog-bkt1/ttt1.txt",
        "remote:",
        "dest-slog-bkt1/ttt1_copied.txt",
        async_job=True
    )
    if copy_job and copy_job.get('success'):
        print(f"✓ Copy job started (jobid: {copy_job['data'].get('jobid')}, status: {copy_job['status_code']})")
    else:
        print("✗ Copy failed")

    # Upload file with URL parameters
    print("\n=== UPLOAD FILE ===")
    result = upload_file(
        remote="remote:",
        remote_path="dest-slog-bkt1/uploaded_test.txt",
        local_file="/etc/hosts"
    )
    
    # Fixed success check
    if result and result.get('success'):
        print("✓ Upload successful!")
    else:
        print("✗ Upload failed")
        if result:
            print(f"  Status: {result.get('status_code')}")
            print(f"  Data: {result.get('data')}")

    # Verify upload by listing destination
    print("\n=== VERIFYING UPLOAD ===")
    dest_listing = list_dir("remote:", "dest-slog-bkt1")
    if dest_listing and dest_listing.get('success'):
        items = dest_listing['data'].get('list', [])
        uploaded_files = [item['Name'] for item in items if 'uploaded' in item['Name']]
        if uploaded_files:
            print(f"✓ Found uploaded files: {uploaded_files}")
        else:
            print("✗ No uploaded files found")
    else:
        print("✗ Verification failed")

    # Delete file (will fail if file doesn't exist - that's OK)
    print("\n=== DELETE FILE ===")
    delete_result = delete_file("remote:", "dest-slog-bkt1/test-delete.txt")
    if delete_result:
        if delete_result.get('success'):
            print(f"✓ File deleted successfully (status: {delete_result['status_code']})")
        else:
            print(f"✗ Delete failed (status: {delete_result['status_code']})")
            if delete_result['status_code'] == 404:
                print("  (File doesn't exist - this is expected)")

    # Move file async
    print("\n=== MOVE FILE ===")
    move_result = move_file(
        "remote:",
        "src-slog-bkt1/ttt2.txt",
        "remote:",
        "dest-slog-bkt1/ttt2_moved.txt",
        async_job=True
    )
    if move_result and move_result.get('success'):
        print(f"✓ Move job started (jobid: {move_result['data'].get('jobid')}, status: {move_result['status_code']})")
    else:
        print("✗ Move failed")

    print("\n=== ALL OPERATIONS COMPLETED ===")