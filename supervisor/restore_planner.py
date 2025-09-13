"""Pure Python helper for backup selection and restore planning."""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

from dateutil import parser as date_parser
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class BackupInfo(BaseModel):
    """Information about a backup."""

    backup_id: str
    completed_at_iso: str
    size_bytes: Optional[int] = None
    backup_type: Optional[str] = None  # "full" or "incremental"
    
    @property
    def completed_at(self) -> datetime:
        """Parse completed_at_iso to datetime object."""
        return date_parser.parse(self.completed_at_iso)


class RestoreSelection(BaseModel):
    """A selected backup for restore."""

    server: str
    db_name: str
    backup_id: str
    completed_at_iso: str
    reason: str  # "latest" or "closest to target"
    time_delta_seconds: Optional[float] = None  # Only for closest selections


class RestorePlan(BaseModel):
    """Complete restore plan across multiple databases."""

    target_timestamp: Optional[str] = None
    selections: List[RestoreSelection]
    
    def display_summary(self) -> str:
        """Generate a human-readable summary of the restore plan."""
        if not self.selections:
            return "No databases selected for restore."
        
        summary = []
        if self.target_timestamp:
            summary.append(f"Restore plan for target time: {self.target_timestamp}")
        else:
            summary.append("Restore plan using latest backups:")
        
        summary.append("")
        
        for selection in self.selections:
            line = f"• {selection.db_name} on {selection.server}: {selection.backup_id}"
            line += f" (completed: {selection.completed_at_iso})"
            
            if selection.time_delta_seconds is not None:
                delta_minutes = abs(selection.time_delta_seconds) / 60
                if delta_minutes < 60:
                    line += f" - {delta_minutes:.1f} minutes from target"
                else:
                    delta_hours = delta_minutes / 60
                    line += f" - {delta_hours:.1f} hours from target"
            else:
                line += f" - {selection.reason}"
            
            summary.append(line)
        
        return "\n".join(summary)


def parse_target_timestamp(target_timestamp: str) -> datetime:
    """
    Parse various timestamp formats to datetime object.
    
    Args:
        target_timestamp: Timestamp string in various formats
        
    Returns:
        Parsed datetime object
        
    Raises:
        ValueError: If timestamp cannot be parsed
    """
    try:
        return date_parser.parse(target_timestamp)
    except Exception as e:
        raise ValueError(f"Unable to parse timestamp '{target_timestamp}': {e}")


def find_closest_backup(
    backups: List[BackupInfo],
    target_time: datetime
) -> Tuple[BackupInfo, float]:
    """
    Find the backup closest to the target time.
    
    Args:
        backups: List of available backups
        target_time: Target timestamp to match
        
    Returns:
        Tuple of (closest_backup, time_delta_seconds)
        
    Raises:
        ValueError: If no backups available
    """
    if not backups:
        raise ValueError("No backups available")
    
    closest_backup = None
    smallest_delta = float('inf')
    
    for backup in backups:
        try:
            backup_time = backup.completed_at
            delta = abs((target_time - backup_time).total_seconds())
            
            if delta < smallest_delta:
                smallest_delta = delta
                closest_backup = backup
                
        except Exception as e:
            logger.warning(f"Failed to parse backup time for {backup.backup_id}: {e}")
            continue
    
    if closest_backup is None:
        raise ValueError("No valid backup timestamps found")
    
    return closest_backup, smallest_delta


def find_latest_backup(backups: List[BackupInfo]) -> BackupInfo:
    """
    Find the latest backup by completion time.
    
    Args:
        backups: List of available backups
        
    Returns:
        Latest backup
        
    Raises:
        ValueError: If no backups available
    """
    if not backups:
        raise ValueError("No backups available")
    
    latest_backup = None
    latest_time = None
    
    for backup in backups:
        try:
            backup_time = backup.completed_at
            
            if latest_time is None or backup_time > latest_time:
                latest_time = backup_time
                latest_backup = backup
                
        except Exception as e:
            logger.warning(f"Failed to parse backup time for {backup.backup_id}: {e}")
            continue
    
    if latest_backup is None:
        raise ValueError("No valid backup timestamps found")
    
    return latest_backup


def create_restore_plan(
    backups_by_db: Dict[Tuple[str, str], List[Dict[str, Union[str, int]]]],
    target_timestamp: Optional[str] = None
) -> RestorePlan:
    """
    Create a restore plan by selecting appropriate backups for each database.
    
    Args:
        backups_by_db: Mapping of (server, db_name) to list of backup data
        target_timestamp: Optional target timestamp in ISO8601 format
        
    Returns:
        Complete restore plan
    """
    selections = []
    
    # Parse target timestamp if provided
    target_time = None
    if target_timestamp:
        try:
            target_time = parse_target_timestamp(target_timestamp)
        except ValueError as e:
            logger.error(f"Invalid target timestamp: {e}")
            raise
    
    # Process each database
    for (server, db_name), backup_data_list in backups_by_db.items():
        if not backup_data_list:
            logger.warning(f"No backups found for {db_name} on {server}")
            continue
        
        try:
            # Convert backup data to BackupInfo objects
            backups = []
            for backup_data in backup_data_list:
                try:
                    backup = BackupInfo(**backup_data)
                    backups.append(backup)
                except Exception as e:
                    logger.warning(f"Failed to parse backup data {backup_data}: {e}")
                    continue
            
            if not backups:
                logger.warning(f"No valid backups for {db_name} on {server}")
                continue
            
            # Select backup based on strategy
            if target_time:
                # Find closest backup to target time
                try:
                    selected_backup, time_delta = find_closest_backup(backups, target_time)
                    selection = RestoreSelection(
                        server=server,
                        db_name=db_name,
                        backup_id=selected_backup.backup_id,
                        completed_at_iso=selected_backup.completed_at_iso,
                        reason="closest to target",
                        time_delta_seconds=time_delta
                    )
                    selections.append(selection)
                    
                except ValueError as e:
                    logger.error(f"Failed to find closest backup for {db_name} on {server}: {e}")
                    continue
            else:
                # Find latest backup
                try:
                    selected_backup = find_latest_backup(backups)
                    selection = RestoreSelection(
                        server=server,
                        db_name=db_name,
                        backup_id=selected_backup.backup_id,
                        completed_at_iso=selected_backup.completed_at_iso,
                        reason="latest"
                    )
                    selections.append(selection)
                    
                except ValueError as e:
                    logger.error(f"Failed to find latest backup for {db_name} on {server}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error processing backups for {db_name} on {server}: {e}")
            continue
    
    return RestorePlan(
        target_timestamp=target_timestamp,
        selections=selections
    )


def group_selections_by_server(plan: RestorePlan) -> Dict[str, List[RestoreSelection]]:
    """
    Group restore selections by server for execution planning.
    
    Args:
        plan: Restore plan
        
    Returns:
        Dictionary mapping server names to their restore selections
    """
    grouped = {}
    
    for selection in plan.selections:
        server = selection.server
        if server not in grouped:
            grouped[server] = []
        grouped[server].append(selection)
    
    return grouped


if __name__ == "__main__":
    # Test the restore planner
    import json
    
    # Sample backup data
    sample_backups = {
        ("PG1", "customer_db"): [
            {
                "backup_id": "backup_001",
                "completed_at_iso": "2025-09-10T10:00:00Z",
                "size_bytes": 1024000,
                "backup_type": "full"
            },
            {
                "backup_id": "backup_002", 
                "completed_at_iso": "2025-09-10T12:00:00Z",
                "size_bytes": 204800,
                "backup_type": "incremental"
            }
        ],
        ("PG2", "hr_db"): [
            {
                "backup_id": "backup_003",
                "completed_at_iso": "2025-09-10T09:30:00Z",
                "size_bytes": 512000,
                "backup_type": "full"
            }
        ]
    }
    
    # Test latest backup selection
    print("=== Latest Backup Plan ===")
    latest_plan = create_restore_plan(sample_backups)
    print(latest_plan.display_summary())
    print()
    
    # Test target timestamp selection
    print("=== Target Timestamp Plan ===")
    target_plan = create_restore_plan(sample_backups, "2025-09-10T11:00:00Z")
    print(target_plan.display_summary())
    print()
    
    # Test grouping by server
    print("=== Grouped by Server ===")
    grouped = group_selections_by_server(target_plan)
    for server, selections in grouped.items():
        print(f"{server}: {len(selections)} databases")
        for selection in selections:
            print(f"  - {selection.db_name}: {selection.backup_id}")
