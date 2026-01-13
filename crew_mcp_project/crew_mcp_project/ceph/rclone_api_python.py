import requests
from requests.auth import HTTPBasicAuth
import json
from typing import Dict, List, Optional
from pathlib import Path


class RcloneAPI:
    """
    Python client for rclone RC API
    
    Usage:
        api = RcloneAPI(base_url="http://localhost:5572", user="admin", password="admin123")
        
        # List files
        files = api.list_files("remote:src-slog-bkt1")
        
        # Upload file
        api.upload_file("/tmp", "test.txt", "remote:dest-slog-bkt1/uploads", "test.txt")
        
        # Download file
        content = api.download_file("remote:dest-slog-bkt1/ceph", "test.txt")
        
        # Sync buckets
        api.sync("remote:src-slog-bkt1", "remote:dest-slog-bkt1")
    """
    
    def __init__(self, base_url: str = "http://localhost:5572", 
                 user: str = "admin", password: str = "admin123"):
        self.base_url = base_url.rstrip('/')
        self.auth = HTTPBasicAuth(user, password)
        self.headers = {"Content-Type": "applicat;'ion/json"}
    
    def _post(self, endpoint: str, data: Dict) -> Dict:
        """Make POST request to rclone API"""
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.post(url, json=data, auth=self.auth, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"error": str(e), "endpoint": endpoint}
    
    def _get(self, path: str) -> bytes:
        """Download file via direct HTTP access"""
        url = f"{self.base_url}/[{path}]"
        try:
            response = requests.get(url, auth=self.auth)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            raise Exception(f"Download failed: {e}")
    
    # ==================== LIST OPERATIONS ====================
    
    def list_remotes(self) -> List[str]:
        """List all configured remotes"""
        result = self._post("config/listremotes", {})
        return result.get("remotes", [])
    
    def list_files(self, remote_path: str, recursive: bool = False) -> List[Dict]:
        """
        List files in a remote path
        
        Args:
            remote_path: e.g., "remote:bucket" or "remote:bucket/folder"
            recursive: If True, list all files recursively
        """
        data = {
            "fs": remote_path,
            "remote": ""
        }
        if recursive:
            data["opt"] = {"recurse": True}
        
        result = self._post("operations/list", data)
        return result.get("list", [])
    
    def get_file_info(self, remote_path: str, filename: str) -> Dict:
        """Get metadata for a specific file"""
        data = {
            "fs": remote_path,
            "remote": filename
        }
        result = self._post("operations/stat", data)
        return result.get("item", {})
    
    def get_size(self, remote_path: str) -> Dict:
        """Get total size and file count of a remote path"""
        data = {"fs": remote_path}
        return self._post("operations/size", data)
    
    # ==================== UPLOAD/DOWNLOAD ====================
    
    def upload_file(self, src_dir: str, src_file: str, 
                   dst_path: str, dst_file: str) -> Dict:
        """
        Upload a local file to remote
        
        Args:
            src_dir: Local directory path (e.g., "/tmp")
            src_file: Local filename (e.g., "test.txt")
            dst_path: Remote path (e.g., "remote:bucket/folder")
            dst_file: Remote filename (e.g., "test.txt")
        """
        data = {
            "srcFs": src_dir,
            "srcRemote": src_file,
            "dstFs": dst_path,
            "dstRemote": dst_file
        }
        return self._post("operations/copyfile", data)
    
    def download_file(self, remote_path: str, filename: str, 
                     save_to: Optional[str] = None) -> bytes:
        """
        Download file from remote
        
        Args:
            remote_path: Remote path (e.g., "remote:bucket/folder")
            filename: Filename to download
            save_to: Optional local path to save file
            
        Returns:
            File content as bytes
        """
        full_path = f"{remote_path}/{filename}"
        content = self._get(full_path)
        
        if save_to:
            Path(save_to).write_bytes(content)
        
        return content
    
    # ==================== COPY/MOVE/SYNC ====================
    
    def copy(self, src_path: str, dst_path: str, async_job: bool = False, 
             flags: Optional[Dict] = None) -> Dict:
        """
        Copy files from source to destination (doesn't delete from source)
        
        Args:
            src_path: Source remote path
            dst_path: Destination remote path
            async_job: Run as background job
            flags: Additional rclone flags as dict
                   Example: {"transfers": 10, "bwlimit": "10M", "fast-list": True}
        """
        data = {
            "srcFs": src_path,
            "dstFs": dst_path
        }
        if async_job:
            data["_async"] = True
        
        # Add flags to request
        if flags:
            data["_config"] = flags
        
        return self._post("sync/copy", data)
    
    def sync(self, src_path: str, dst_path: str, async_job: bool = False) -> Dict:
        """
        Sync source to destination (makes destination identical to source)
        Deletes files in destination that aren't in source
        
        Args:
            src_path: Source remote path
            dst_path: Destination remote path
            async_job: Run as background job
        """
        data = {
            "srcFs": src_path,
            "dstFs": dst_path
        }
        if async_job:
            data["_async"] = True
        
        return self._post("sync/sync", data)
    
    def move_file(self, src_path: str, src_file: str,
                 dst_path: str, dst_file: str) -> Dict:
        """Move a file from source to destination"""
        data = {
            "srcFs": src_path,
            "srcRemote": src_file,
            "dstFs": dst_path,
            "dstRemote": dst_file
        }
        return self._post("operations/movefile", data)
    
    # ==================== DELETE OPERATIONS ====================
    
    def delete_file(self, remote_path: str, filename: str) -> Dict:
        """Delete a specific file"""
        data = {
            "fs": remote_path,
            "remote": filename
        }
        return self._post("operations/deletefile", data)
    
    def purge(self, remote_path: str) -> Dict:
        """Delete a directory and all its contents"""
        data = {"fs": remote_path}
        return self._post("operations/purge", data)
    
    # ==================== JOB MANAGEMENT ====================
    
    def list_jobs(self) -> Dict:
        """List all running and completed jobs"""
        return self._post("job/list", {})
    
    def get_job_status(self, job_id: int) -> Dict:
        """Get status of a specific job"""
        data = {"jobid": job_id}
        return self._post("job/status", data)
    
    def stop_job(self, job_id: int) -> Dict:
        """Stop a running job"""
        data = {"jobid": job_id}
        return self._post("job/stop", data)
    
    # ==================== STATS & MONITORING ====================
    
    def get_stats(self) -> Dict:
        """Get current transfer statistics"""
        return self._post("core/stats", {})
    
    def reset_stats(self) -> Dict:
        """Reset transfer statistics"""
        return self._post("core/stats-reset", {})
    
    def get_bandwidth_stats(self) -> Dict:
        """Get bandwidth statistics"""
        return self._post("core/bwlimit", {})


# ==================== EXAMPLE USAGE ====================

if __name__ == "__main__":
    # Initialize API client
    api = RcloneAPI()
    
    print("=== Available Remotes ===")
    remotes = api.list_remotes()
    print(remotes)
    
    print("\n=== List Files in src-slog-bkt1 ===")
    files = api.list_files("remote:src-slog-bkt1")
    for file in files:
        print(f"  {file['Name']} - {file['Size']} bytes")
    
    print("\n=== Get Bucket Size ===")
    size_info = api.get_size("remote:src-slog-bkt1")
    print(f"  Total: {size_info.get('bytes', 0)} bytes, {size_info.get('count', 0)} files")
    
    print("\n=== Upload a Test File ===")
    # Create a test file
    with open("/tmp/api_test.txt", "w") as f:
        f.write("Hello from Python API!")
    
    result = api.upload_file("/tmp", "api_test.txt", 
                            "remote:dest-slog-bkt1/python", "api_test.txt")
    print(f"  Upload result: {result}")
    
    print("\n=== Download the File Back ===")
    content = api.download_file("remote:dest-slog-bkt1/python", "api_test.txt")
    print(f"  Downloaded content: {content.decode()}")
    
    print("\n=== Get Transfer Stats ===")
    stats = api.get_stats()
    print(f"  Transfers: {stats.get('transfers', 0)}")
    print(f"  Total bytes: {stats.get('totalBytes', 0)}")
    print(f"  Errors: {stats.get('errors', 0)}")
    
    print("\n=== Copy Between Buckets (Async) ===")
    copy_result = api.copy("remote:src-slog-bkt1", 
                          "remote:dest-slog-bkt1/backup", 
                          async_job=True)
    print(f"  Job started: {copy_result}")
    
    if 'jobid' in copy_result:
        print("\n=== Check Job Status ===")
        import time
        time.sleep(2)
        job_status = api.get_job_status(copy_result['jobid'])
        print(f"  Job status: {job_status}")