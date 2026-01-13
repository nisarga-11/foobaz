"""
S3 Log Parser Module for FastAPI (CORRECTED)
Handles downloading logs from S3 and mirroring operations
FIXED: Better file tracking and error handling
"""
import boto3
from datetime import datetime, timezone
from typing import List, Tuple, Set
from dataclasses import dataclass
import os

@dataclass
class OperationSummary:
    """Summary of parsing and mirroring operations"""
    logs_downloaded: int
    total_operations: int
    put_operations: int
    get_operations: int
    delete_operations: int
    files_remaining: int
    remaining_file_list: List[str]
    log_file_path: str
    download_dir: str

class S3LogParser:
    """
    Parser for S3 logs that mirrors PUT/GET/DELETE operations locally
    """
    
    def __init__(self):
        # S3 Configuration
        self.aws_access_key = "abc"
        self.aws_secret_key = "abc"
        self.region = "us-east-1"
        self.endpoint_url = "http://fenrir-vm158.storage.tucson.ibm.com:8080"
        self.log_bucket = "dest-slog-bkt1"
        self.src_bucket = "src-slog-bkt1"
        
        # Paths
        self.base_dir = "/root/sp-lakehouse-backup/crew_mcp_project/crew_mcp_project/ceph"
        self.log_file = os.path.join(self.base_dir, "logs", "latest-log.txt")
        self.download_dir = os.path.join(self.base_dir, "downloads")
        
        # Ensure directories exist
        os.makedirs(self.download_dir, exist_ok=True)
        os.makedirs(os.path.join(self.base_dir, "logs"), exist_ok=True)
        
        # Initialize S3 client
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name=self.region,
            endpoint_url=self.endpoint_url
        )
        
        self.time_fmt = "%d/%b/%Y:%H:%M:%S"
    
    def _parse_log_timestamp(self, log_filename: str) -> datetime:
        """
        Parse timestamp from log filename
        Format: src-slog-bkt1-YYYY-MM-DD-HH-MM-SS-...
        """
        parts = log_filename.split('-')
        
        if len(parts) < 9:
            raise ValueError(f"Invalid log filename format: {log_filename}")
        
        year, month, day = parts[3], parts[4], parts[5]
        hour, minute, sec = parts[6], parts[7], parts[8]
        
        return datetime(
            int(year), int(month), int(day),
            int(hour), int(minute), int(sec),
            tzinfo=timezone.utc
        )
    
    def _download_logs_in_range(
        self, 
        start_dt: datetime, 
        end_dt: datetime
    ) -> List[str]:
        """
        Download all log files within the specified time range from S3
        Returns list of downloaded log keys
        """
        print(f"\n📥 Downloading logs from S3 bucket: {self.log_bucket}")
        
        # List all objects in LOG bucket
        try:
            response = self.s3.list_objects_v2(Bucket=self.log_bucket)
        except Exception as e:
            raise Exception(f"Failed to list objects in {self.log_bucket}: {e}")
        
        if 'Contents' not in response:
            raise Exception(f"No logs found in bucket {self.log_bucket}")
        
        # Filter logs by timestamp
        matched_logs = []
        for obj in response['Contents']:
            key = obj['Key']
            
            # Skip non-log files
            if not key.startswith('src-slog-bkt1-'):
                continue
            
            try:
                log_dt = self._parse_log_timestamp(key)
                
                if start_dt <= log_dt <= end_dt:
                    matched_logs.append(key)
                    print(f"   ✅ Matched: {key}")
            except Exception as e:
                print(f"   ⚠️  Could not parse timestamp from {key}: {e}")
                continue
        
        if not matched_logs:
            raise Exception(f"No logs found between {start_dt} and {end_dt}")
        
        # Download and combine all matched logs
        print(f"\n📥 Downloading {len(matched_logs)} log file(s)...")
        
        all_content = []
        for log_key in matched_logs:
            try:
                response = self.s3.get_object(Bucket=self.log_bucket, Key=log_key)
                content = response['Body'].read().decode('utf-8')
                all_content.append(content)
                print(f"   ✓ Downloaded: {log_key}")
            except Exception as e:
                print(f"   ✗ Failed: {log_key} - {e}")
        
        # Write combined logs to file
        with open(self.log_file, 'w') as f:
            f.write('\n'.join(all_content))
        
        print(f"✅ Combined logs saved to: {self.log_file}")
        
        return matched_logs
    
    def _parse_log_line_time(self, time_str: str) -> datetime:
        """Parse time from log line like '[10/Nov/2025:05:14:10'"""
        time_str = time_str.lstrip("[")
        return datetime.strptime(time_str, self.time_fmt).replace(tzinfo=timezone.utc)
    
    def _parse_operations(
        self, 
        start_dt: datetime, 
        end_dt: datetime,
        operation_filter: str
    ) -> List[Tuple[datetime, str, str]]:
        """
        Parse log file and extract operations
        Returns list of (timestamp, object_name, operation) tuples
        """
        print(f"\n📋 Parsing log file: {self.log_file}")
        
        if not os.path.exists(self.log_file):
            raise Exception(f"Log file not found: {self.log_file}")
        
        operations = []
        
        with open(self.log_file, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                parts = line.split()
                
                if len(parts) < 6:
                    continue
                
                # Extract timestamp from parts[2]: [10/Nov/2025:05:14:10
                raw_time = parts[2].lstrip("[")
                try:
                    log_time = self._parse_log_line_time(raw_time)
                except Exception as e:
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
        
        # Sort operations chronologically
        operations.sort(key=lambda x: x[0])
        
        print(f"✅ Found {len(operations)} operation(s) (chronologically sorted)")
        for log_time, obj, op in operations[:10]:  # Show first 10
            time_str = log_time.strftime(self.time_fmt)
            print(f"   • [{time_str}] {obj} → {op}")
        if len(operations) > 10:
            print(f"   ... and {len(operations) - 10} more")
        
        return operations
    
    def _get_local_filename(self, obj_name: str) -> str:
        """
        Get the local filename for an S3 object.
        Uses basename to handle both simple names and paths.
        """
        return os.path.basename(obj_name)
    
    def _mirror_operations(
        self, 
        operations: List[Tuple[datetime, str, str]]
    ) -> Tuple[Set[str], int, int, int]:
        """
        Mirror operations locally (PUT=download, DELETE=remove, GET=download if needed)
        Returns (downloaded_files_set, put_count, get_count, delete_count)
        
        CORRECTED: Better tracking of local files
        """
        print(f"\n🔄 Mirroring {len(operations)} operation(s) locally...")
        
        # Track by local filename (basename)
        downloaded_files = set()  # Set of local filenames that currently exist
        put_count = 0
        get_count = 0
        delete_count = 0
        
        for log_time, obj_name, operation in operations:
            local_filename = self._get_local_filename(obj_name)
            local_path = os.path.join(self.download_dir, local_filename)
            time_str = log_time.strftime(self.time_fmt)
            
            if operation == "PUT":
                try:
                    # Download from S3
                    self.s3.download_file(self.src_bucket, obj_name, local_path)
                    downloaded_files.add(local_filename)
                    put_count += 1
                    print(f"   ⬇️  [{time_str}] PUT: {obj_name} → {local_filename}")
                except Exception as e:
                    if "NoSuchKey" in str(e) or "404" in str(e):
                        print(f"   ⚠️  [{time_str}] PUT: {obj_name} not found in S3")
                    else:
                        print(f"   ❌ [{time_str}] PUT: Failed - {obj_name}: {e}")
            
            elif operation == "DELETE":
                if os.path.exists(local_path):
                    try:
                        os.remove(local_path)
                        downloaded_files.discard(local_filename)
                        delete_count += 1
                        print(f"   🗑️  [{time_str}] DELETE: Removed {local_filename}")
                    except Exception as e:
                        print(f"   ❌ [{time_str}] DELETE: Failed - {local_filename}: {e}")
                else:
                    print(f"   ℹ️  [{time_str}] DELETE: {local_filename} not in downloads")
            
            elif operation == "GET":
                # Only download if not already present
                if local_filename not in downloaded_files:
                    try:
                        self.s3.download_file(self.src_bucket, obj_name, local_path)
                        downloaded_files.add(local_filename)
                        get_count += 1
                        print(f"   ⬇️  [{time_str}] GET: {obj_name} → {local_filename}")
                    except Exception as e:
                        if "NoSuchKey" in str(e) or "404" in str(e):
                            print(f"   ⚠️  [{time_str}] GET: {obj_name} not found in S3")
                        else:
                            print(f"   ❌ [{time_str}] GET: Failed - {obj_name}: {e}")
                else:
                    print(f"   ℹ️  [{time_str}] GET: {local_filename} already exists")
        
        return downloaded_files, put_count, get_count, delete_count
    
    def _get_actual_files_in_directory(self) -> List[str]:
        """
        Get actual list of files currently in downloads directory.
        This is the source of truth for what's available.
        """
        if not os.path.exists(self.download_dir):
            return []
        
        files = []
        for item in os.listdir(self.download_dir):
            item_path = os.path.join(self.download_dir, item)
            if os.path.isfile(item_path):
                files.append(item)
        
        return sorted(files)
    
    def parse_and_mirror(
        self, 
        start_time_str: str, 
        end_time_str: str,
        operation_filter: str = "ALL"
    ) -> OperationSummary:
        """
        Main method to parse logs and mirror operations
        
        Args:
            start_time_str: Start time in format DD/MMM/YYYY:HH:MM:SS
            end_time_str: End time in format DD/MMM/YYYY:HH:MM:SS
            operation_filter: Filter by operation (PUT, GET, DELETE, or ALL)
        
        Returns:
            OperationSummary with results
        """
        print("\n" + "="*70)
        print("  S3 LOG PARSER & OPERATION MIRROR")
        print("="*70)
        print(f"\n⏰ Time Range: {start_time_str} → {end_time_str}")
        print(f"📁 Download Directory: {self.download_dir}")
        print(f"⚙️  Operation Filter: {operation_filter}")
        
        # Parse time strings
        try:
            start_dt = datetime.strptime(start_time_str, self.time_fmt).replace(tzinfo=timezone.utc)
            end_dt = datetime.strptime(end_time_str, self.time_fmt).replace(tzinfo=timezone.utc)
        except Exception as e:
            raise ValueError(f"Invalid time format: {e}")
        
        # Step 1: Download logs from S3
        print("\n" + "="*70)
        print("  STEP 1: DOWNLOADING LOGS FROM S3")
        print("="*70)
        
        try:
            matched_logs = self._download_logs_in_range(start_dt, end_dt)
        except Exception as e:
            print(f"❌ Error downloading logs: {e}")
            raise
        
        # Step 2: Parse operations from logs
        print("\n" + "="*70)
        print("  STEP 2: PARSING LOG FILE")
        print("="*70)
        
        try:
            operations = self._parse_operations(start_dt, end_dt, operation_filter)
        except Exception as e:
            print(f"❌ Error parsing operations: {e}")
            raise
        
        if not operations:
            print(f"\n⚠️  No matching operations found")
            actual_files = self._get_actual_files_in_directory()
            
            return OperationSummary(
                logs_downloaded=len(matched_logs),
                total_operations=0,
                put_operations=0,
                get_operations=0,
                delete_operations=0,
                files_remaining=len(actual_files),
                remaining_file_list=actual_files,
                log_file_path=self.log_file,
                download_dir=self.download_dir
            )
        
        # Step 3: Mirror operations locally
        print("\n" + "="*70)
        print("  STEP 3: MIRRORING OPERATIONS")
        print("="*70)
        
        try:
            downloaded_files, put_count, get_count, delete_count = self._mirror_operations(operations)
        except Exception as e:
            print(f"❌ Error mirroring operations: {e}")
            raise
        
        # Step 4: Verify actual files in directory
        actual_files = self._get_actual_files_in_directory()
        
        # Create summary
        print("\n✅ Operation mirroring completed successfully.")
        print(f"\n📊 Summary:")
        print(f"   • Logs downloaded: {len(matched_logs)}")
        print(f"   • Total operations: {len(operations)}")
        print(f"   • PUT operations: {put_count}")
        print(f"   • GET operations: {get_count}")
        print(f"   • DELETE operations: {delete_count}")
        print(f"   • Files in directory: {len(actual_files)}")
        
        if actual_files:
            print(f"\n📁 Files in downloads directory:")
            for f in actual_files:
                file_path = os.path.join(self.download_dir, f)
                size = os.path.getsize(file_path)
                size_display = f"{size / 1024:.1f} KB" if size < 1024*1024 else f"{size / (1024*1024):.2f} MB"
                print(f"   • {f} ({size_display})")
        
        return OperationSummary(
            logs_downloaded=len(matched_logs),
            total_operations=len(operations),
            put_operations=put_count,
            get_operations=get_count,
            delete_operations=delete_count,
            files_remaining=len(actual_files),
            remaining_file_list=actual_files,
            log_file_path=self.log_file,
            download_dir=self.download_dir
        )