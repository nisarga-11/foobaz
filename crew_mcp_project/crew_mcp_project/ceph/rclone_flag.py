"""
Complete Guide to Using Rclone Flags in Your Project
"""

from rclone_api import RcloneAPI
import requests
from requests.auth import HTTPBasicAuth


# ==================== METHOD 1: API Level Flags ====================

def example_api_flags():
    """Pass flags per API call"""
    api = RcloneAPI()
    
    # Example 1: Fast sync with parallel transfers
    flags = {
        "transfers": 20,          # 20 parallel transfers
        "checkers": 20,           # 20 parallel checkers
        "fast-list": True,        # Use recursive listing
        "buffer-size": "256M"     # 256MB buffer per transfer
    }
    
    result = api.sync(
        "remote:src-slog-bkt1",
        "remote:dest-slog-bkt1",
        flags=flags
    )
    print(f"Sync result: {result}")
    
    # Example 2: Bandwidth limited copy
    flags = {
        "bwlimit": "10M",         # Limit to 10 MB/s
        "tpslimit": 100,          # 100 transactions per second
        "transfers": 4
    }
    
    result = api.copy(
        "remote:src-slog-bkt1",
        "remote:backup",
        flags=flags
    )
    
    # Example 3: Filtered sync (only certain files)
    flags = {
        "include": "*.txt",       # Only .txt files
        "exclude": "*.tmp",       # Exclude .tmp files
        "min-size": "1M",         # Only files > 1MB
        "max-age": "24h"          # Files modified in last 24h
    }
    
    result = api.sync(
        "remote:src-slog-bkt1",
        "remote:filtered-backup",
        flags=flags
    )


# ==================== METHOD 2: Direct API Calls with Flags ====================

def direct_api_with_flags():
    """Use curl/requests directly with flags"""
    
    base_url = "http://localhost:5572"
    auth = HTTPBasicAuth("admin", "admin123")
    
    # Sync with custom flags
    data = {
        "srcFs": "remote:src-slog-bkt1",
        "dstFs": "remote:dest-slog-bkt1",
        "_config": {
            "transfers": 10,
            "checkers": 10,
            "fast-list": True,
            "no-traverse": True,
            "buffer-size": "128M",
            "use-server-modtime": True
        }
    }
    
    response = requests.post(
        f"{base_url}/sync/sync",
        json=data,
        auth=auth,
        headers={"Content-Type": "application/json"}
    )
    
    print(response.json())


# ==================== METHOD 3: Set Global Options ====================

def set_global_options():
    """Set options that apply to all operations"""
    api = RcloneAPI()
    
    # Set global bandwidth limit
    result = api._post("core/bwlimit", {
        "rate": "10M"  # 10 MB/s for all operations
    })
    print(f"Bandwidth limit set: {result}")
    
    # Get current bandwidth limit
    current = api._post("core/bwlimit", {})
    print(f"Current limit: {current}")


# ==================== COMMON FLAG COMBINATIONS ====================

class RcloneFlagPresets:
    """Predefined flag combinations for common scenarios"""
    
    @staticmethod
    def fast_sync():
        """Optimized for speed"""
        return {
            "transfers": 32,
            "checkers": 32,
            "fast-list": True,
            "buffer-size": "256M",
            "multi-thread-streams": 4,
            "use-server-modtime": True
        }
    
    @staticmethod
    def slow_careful():
        """Optimized for reliability and low resource usage"""
        return {
            "transfers": 2,
            "checkers": 4,
            "retries": 10,
            "low-level-retries": 20,
            "timeout": "10m",
            "buffer-size": "16M"
        }
    
    @staticmethod
    def bandwidth_limited(limit="5M"):
        """Limit bandwidth usage"""
        return {
            "bwlimit": limit,
            "tpslimit": 50,
            "transfers": 4,
            "buffer-size": "32M"
        }
    
    @staticmethod
    def large_files():
        """Optimized for large file transfers"""
        return {
            "transfers": 4,
            "checkers": 8,
            "buffer-size": "512M",
            "multi-thread-streams": 8,
            "use-mmap": True
        }
    
    @staticmethod
    def many_small_files():
        """Optimized for many small files"""
        return {
            "transfers": 64,
            "checkers": 64,
            "fast-list": True,
            "buffer-size": "16M",
            "no-traverse": True
        }
    
    @staticmethod
    def incremental_backup():
        """For incremental backups"""
        return {
            "update": True,           # Skip files newer on dest
            "use-server-modtime": True,
            "checksum": True,         # Verify with checksums
            "no-update-modtime": True
        }
    
    @staticmethod
    def filtered_sync(extensions=None, min_size=None, max_age=None):
        """Sync with filters"""
        flags = {}
        
        if extensions:
            # Example: [".txt", ".pdf"]
            flags["include"] = f"*{{{','.join(extensions)}}}"
        
        if min_size:
            flags["min-size"] = min_size  # e.g., "10M"
        
        if max_age:
            flags["max-age"] = max_age    # e.g., "24h"
        
        return flags


# ==================== PRACTICAL EXAMPLES ====================

def example_fast_backup():
    """Fast backup with monitoring"""
    api = RcloneAPI()
    
    print("Starting fast backup...")
    flags = RcloneFlagPresets.fast_sync()
    
    result = api.sync(
        "remote:src-slog-bkt1",
        "remote:fast-backup",
        async_job=True,
        flags=flags
    )
    
    if 'jobid' in result:
        job_id = result['jobid']
        print(f"Job started: {job_id}")
        
        # Monitor progress
        import time
        while True:
            status = api.get_job_status(job_id)
            if status.get('finished', False):
                print("Backup complete!")
                break
            
            stats = api.get_stats()
            print(f"Progress: {stats.get('bytes', 0)} bytes, "
                  f"{stats.get('transfers', 0)} files")
            time.sleep(5)


def example_filtered_backup():
    """Backup only specific file types from today"""
    api = RcloneAPI()
    
    flags = RcloneFlagPresets.filtered_sync(
        extensions=[".txt", ".pdf", ".doc"],
        max_age="24h",
        min_size="1K"
    )
    
    print("Backing up documents from last 24 hours...")
    result = api.copy(
        "remote:src-slog-bkt1",
        "remote:daily-docs",
        flags=flags
    )
    
    stats = api.get_stats()
    print(f"Backed up {stats.get('transfers', 0)} files")


def example_bandwidth_limited_sync():
    """Sync during business hours with bandwidth limit"""
    api = RcloneAPI()
    
    flags = RcloneFlagPresets.bandwidth_limited("2M")  # 2 MB/s
    
    print("Starting bandwidth-limited sync...")
    result = api.sync(
        "remote:src-slog-bkt1",
        "remote:dest-slog-bkt1",
        async_job=True,
        flags=flags
    )
    
    print(f"Sync started with 2MB/s limit")


def example_large_file_transfer():
    """Transfer large video files efficiently"""
    api = RcloneAPI()
    
    flags = RcloneFlagPresets.large_files()
    flags["include"] = "*.{mp4,mkv,avi}"  # Only video files
    
    result = api.copy(
        "remote:video-source",
        "remote:video-backup",
        flags=flags
    )


# ==================== CURL EXAMPLES WITH FLAGS ====================

def curl_examples():
    """Equivalent curl commands with flags"""
    
    print("""
    # Fast sync with flags
    curl -u admin:admin123 \\
      -H "Content-Type: application/json" \\
      -d '{
        "srcFs":"remote:src-slog-bkt1",
        "dstFs":"remote:dest-slog-bkt1",
        "_config":{
          "transfers":20,
          "checkers":20,
          "fast-list":true,
          "buffer-size":"256M"
        }
      }' \\
      "http://localhost:5572/sync/sync"
    
    # Bandwidth limited copy
    curl -u admin:admin123 \\
      -H "Content-Type: application/json" \\
      -d '{
        "srcFs":"remote:src-slog-bkt1",
        "dstFs":"remote:backup",
        "_config":{
          "bwlimit":"5M",
          "tpslimit":100
        }
      }' \\
      "http://localhost:5572/sync/copy"
    
    # Filtered sync (only .txt files)
    curl -u admin:admin123 \\
      -H "Content-Type: application/json" \\
      -d '{
        "srcFs":"remote:src-slog-bkt1",
        "dstFs":"remote:filtered",
        "_config":{
          "include":"*.txt",
          "exclude":"*.tmp",
          "max-age":"24h"
        }
      }' \\
      "http://localhost:5572/sync/sync"
    """)


# ==================== ALL AVAILABLE FLAGS ====================

ALL_RCLONE_FLAGS = {
    # Performance
    "transfers": "Number of file transfers to run in parallel (default 4)",
    "checkers": "Number of checkers to run in parallel (default 8)",
    "buffer-size": "In memory buffer size per transfer (default 16M)",
    "multi-thread-streams": "Max number of streams for multi-thread downloads (default 4)",
    
    # Bandwidth
    "bwlimit": "Bandwidth limit in KiB/s or use suffix B|K|M|G (default 0 = off)",
    "tpslimit": "Limit HTTP transactions per second (0 = unlimited)",
    
    # Filtering
    "include": "Include files matching pattern",
    "exclude": "Exclude files matching pattern",
    "min-size": "Only transfer files bigger than this",
    "max-size": "Only transfer files smaller than this",
    "min-age": "Only transfer files older than this",
    "max-age": "Only transfer files younger than this",
    
    # Reliability
    "retries": "Retry operations this many times if they fail (default 3)",
    "low-level-retries": "Number of low level retries to do (default 10)",
    "timeout": "IO idle timeout (default 5m0s)",
    "contimeout": "Connect timeout (default 1m0s)",
    
    # Sync Options
    "update": "Skip files that are newer on the destination",
    "use-server-modtime": "Use server modified time instead of object metadata",
    "no-update-modtime": "Don't update destination mod-time if files identical",
    "checksum": "Skip based on checksum & size, not mod-time & size",
    "size-only": "Skip based on size only, not mod-time or checksum",
    
    # Advanced
    "fast-list": "Use recursive list if available. Uses more memory but fewer transactions",
    "no-traverse": "Don't traverse destination file system on copy",
    "no-check-certificate": "Do not verify SSL certificates",
    "use-mmap": "Use mmap allocator (faster for large files)",
    "cache-dir": "Directory rclone will use for caching",
    
    # Logging
    "log-level": "Log level: DEBUG|INFO|NOTICE|ERROR (default NOTICE)",
    "log-file": "Log everything to this file",
    "stats": "Interval between printing stats (0 to disable)",
    
    # S3 Specific
    "s3-upload-concurrency": "Concurrency for multipart uploads (default 4)",
    "s3-chunk-size": "Chunk size to use for uploading (default 5M)",
    "s3-upload-cutoff": "Cutoff for switching to chunked upload (default 200M)",
}


# ==================== USAGE ====================

if __name__ == "__main__":
    api = RcloneAPI()
    
    print("=== Example 1: Fast Sync ===")
    flags = RcloneFlagPresets.fast_sync()
    print(f"Flags: {flags}")
    
    print("\n=== Example 2: Bandwidth Limited ===")
    flags = RcloneFlagPresets.bandwidth_limited("5M")
    print(f"Flags: {flags}")
    
    print("\n=== Example 3: Filtered Backup ===")
    flags = RcloneFlagPresets.filtered_sync(
        extensions=[".txt", ".pdf"],
        max_age="7d"
    )
    print(f"Flags: {flags}")
    
    print("\n=== Running actual sync with flags ===")
    result = api.sync(
        "remote:src-slog-bkt1",
        "remote:test-dest",
        flags=RcloneFlagPresets.fast_sync()
    )
    print(f"Result: {result}")