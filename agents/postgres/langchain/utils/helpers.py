import os
import re
import glob
from datetime import datetime, timedelta
from typing import List, Tuple, Optional

def parse_timestamp_from_filename(filename: str) -> Optional[datetime]:
    """Extract timestamp from backup filename."""
    # Pattern: database_name_YYYYMMDDTHHMMSS.tar
    pattern = r'_(\d{8}T\d{6})\.tar$'
    match = re.search(pattern, filename)
    if match:
        timestamp_str = match.group(1)
        try:
            return datetime.strptime(timestamp_str, '%Y%m%dT%H%M%S')
        except ValueError:
            return None
    return None

def find_backup_files(database_name: str, backup_dir: str) -> List[str]:
    """Find all backup files for a specific database."""
    pattern = os.path.join(backup_dir, f"{database_name}_*.tar")
    backup_files = glob.glob(pattern)
    return sorted(backup_files)

def find_matching_timestamps(db1_files: List[str], db2_files: List[str]) -> List[Tuple[str, str]]:
    """Find backup files with matching timestamps across databases."""
    matching_pairs = []
    
    for db1_file in db1_files:
        db1_timestamp = parse_timestamp_from_filename(db1_file)
        if not db1_timestamp:
            continue
            
        for db2_file in db2_files:
            db2_timestamp = parse_timestamp_from_filename(db2_file)
            if not db2_timestamp:
                continue
                
            if db1_timestamp == db2_timestamp:
                matching_pairs.append((db1_file, db2_file))
    
    return matching_pairs

def find_closest_timestamps(db1_files: List[str], db2_files: List[str]) -> Tuple[str, str]:
    """Find the closest matching timestamps between two databases."""
    if not db1_files or not db2_files:
        return None, None
    
    min_diff = float('inf')
    best_pair = (db1_files[0], db2_files[0])
    
    for db1_file in db1_files:
        db1_timestamp = parse_timestamp_from_filename(db1_file)
        if not db1_timestamp:
            continue
            
        for db2_file in db2_files:
            db2_timestamp = parse_timestamp_from_filename(db2_file)
            if not db2_timestamp:
                continue
                
            time_diff = abs((db1_timestamp - db2_timestamp).total_seconds())
            if time_diff < min_diff:
                min_diff = time_diff
                best_pair = (db1_file, db2_file)
    
    return best_pair

def format_timestamp_display(timestamp: datetime) -> str:
    """Format timestamp for display."""
    return timestamp.strftime('%Y-%m-%d %H:%M:%S')

def get_backup_file_info(backup_files: List[str]) -> List[dict]:
    """Get detailed information about backup files."""
    file_info = []
    for file_path in backup_files:
        filename = os.path.basename(file_path)
        timestamp = parse_timestamp_from_filename(filename)
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
        
        file_info.append({
            'filename': filename,
            'file_path': file_path,
            'timestamp': timestamp,
            'size_mb': round(file_size / (1024 * 1024), 2),
            'formatted_timestamp': format_timestamp_display(timestamp) if timestamp else 'Unknown'
        })
    
    return sorted(file_info, key=lambda x: x['timestamp'] if x['timestamp'] else datetime.min, reverse=True)

def clean_old_backups(backup_dir: str, retention_days: int) -> List[str]:
    """Remove backup files older than retention_days."""
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    removed_files = []
    
    for file_path in glob.glob(os.path.join(backup_dir, "*.tar")):
        timestamp = parse_timestamp_from_filename(os.path.basename(file_path))
        if timestamp and timestamp < cutoff_date:
            try:
                os.remove(file_path)
                removed_files.append(file_path)
            except OSError:
                pass
    
    return removed_files

def validate_database_name(db_name: str) -> bool:
    """Validate database name format."""
    # Simple validation - alphanumeric and underscores only
    return bool(re.match(r'^[a-zA-Z0-9_]+$', db_name))

def extract_database_name_from_filename(filename: str) -> Optional[str]:
    """Extract database name from backup filename."""
    # Pattern: database_name_YYYYMMDDTHHMMSS.tar
    pattern = r'^([a-zA-Z0-9_]+)_\d{8}T\d{6}\.tar$'
    match = re.match(pattern, filename)
    return match.group(1) if match else None

def find_matching_pgbackrest_timestamps(customer_backups: List[dict], employee_backups: List[dict]) -> List[dict]:
    """Find pgBackRest backups with matching timestamps between customer and employee servers."""
    matching_pairs = []
    
    for customer_backup in customer_backups:
        customer_timestamp = customer_backup.get("timestamp")
        if not customer_timestamp:
            continue
            
        for employee_backup in employee_backups:
            employee_timestamp = employee_backup.get("timestamp")
            if not employee_timestamp:
                continue
                
            # Parse timestamps (assuming they're in ISO format or similar)
            try:
                if isinstance(customer_timestamp, str):
                    customer_dt = datetime.fromisoformat(customer_timestamp.replace('Z', '+00:00'))
                else:
                    customer_dt = customer_timestamp
                    
                if isinstance(employee_timestamp, str):
                    employee_dt = datetime.fromisoformat(employee_timestamp.replace('Z', '+00:00'))
                else:
                    employee_dt = employee_timestamp
                
                # Check if timestamps match (within 2 minutes tolerance for pgBackRest)
                time_diff = abs((customer_dt - employee_dt).total_seconds())
                if time_diff <= 120:  # 2 minutes tolerance
                    matching_pairs.append({
                        "customer_backup": customer_backup,
                        "employee_backup": employee_backup,
                        "timestamp_diff_seconds": time_diff,
                        "customer_timestamp": customer_timestamp,
                        "employee_timestamp": employee_timestamp
                    })
                    break  # Found a match for this customer backup, move to next
            except Exception as e:
                # Skip if timestamp parsing fails
                continue
    
    # Sort by timestamp (most recent first)
    matching_pairs.sort(key=lambda x: x["customer_timestamp"], reverse=True)
    
    return matching_pairs

def find_closest_pgbackrest_timestamps(customer_backups: List[dict], employee_backups: List[dict]) -> Optional[dict]:
    """Find the closest pgBackRest backup timestamps between customer and employee servers."""
    closest_pair = None
    min_time_diff = float('inf')
    
    for customer_backup in customer_backups:
        customer_timestamp = customer_backup.get("timestamp")
        if not customer_timestamp:
            continue
            
        for employee_backup in employee_backups:
            employee_timestamp = employee_backup.get("timestamp")
            if not employee_timestamp:
                continue
                
            try:
                if isinstance(customer_timestamp, str):
                    customer_dt = datetime.fromisoformat(customer_timestamp.replace('Z', '+00:00'))
                else:
                    customer_dt = customer_timestamp
                    
                if isinstance(employee_timestamp, str):
                    employee_dt = datetime.fromisoformat(employee_timestamp.replace('Z', '+00:00'))
                else:
                    employee_dt = employee_timestamp
                
                time_diff = abs((customer_dt - employee_dt).total_seconds())
                if time_diff < min_time_diff:
                    min_time_diff = time_diff
                    closest_pair = {
                        "customer_backup": customer_backup,
                        "employee_backup": employee_backup,
                        "timestamp_diff_seconds": time_diff,
                        "customer_timestamp": customer_timestamp,
                        "employee_timestamp": employee_timestamp
                    }
            except Exception as e:
                # Skip if timestamp parsing fails
                continue
    
    return closest_pair
