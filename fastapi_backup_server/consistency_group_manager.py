"""
Consistency Group Manager with JSON File Persistence
File: consistency_group_manager.py
Location: Project root directory
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
import json
import os
from pathlib import Path


class ConsistencyGroup(BaseModel):
    """Model for a consistency group linking PostgreSQL and Ceph backups"""
    group_id: str = Field(..., description="Unique identifier for the consistency group")
    timestamp: str = Field(..., description="ISO format timestamp when group was created")
    postgres_backup: str = Field(..., description="PostgreSQL backup filename")
    postgres_database: str = Field(..., description="PostgreSQL database name")
    ceph_objects: List[str] = Field(..., description="List of Ceph object filenames")
    ceph_bucket_source: str = Field(..., description="Source S3 bucket name")
    backup_type: str = Field(default="full", description="Type of backup: full, incremental, base")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Additional metadata")
    status: str = Field(default="active", description="Status: active, archived, deleted")


class ConsistencyGroupManager:
    """Manager class for consistency groups with JSON file persistence"""
    
    def __init__(self, storage_file: str = "consistency_groups.json"):
        """
        Initialize the manager with a JSON storage file.
        
        Args:
            storage_file: Path to the JSON file for storing consistency groups
        """
        self.storage_file = Path(storage_file)
        self.groups: Dict[str, ConsistencyGroup] = {}
        self._load_from_file()
    
    def _load_from_file(self):
        """Load consistency groups from JSON file"""
        if self.storage_file.exists():
            try:
                with open(self.storage_file, 'r') as f:
                    data = json.load(f)
                    self.groups = {
                        gid: ConsistencyGroup(**group_data) 
                        for gid, group_data in data.items()
                    }
                print(f"✅ Loaded {len(self.groups)} consistency group(s) from {self.storage_file}")
            except Exception as e:
                print(f"⚠️ Error loading consistency groups: {e}")
                self.groups = {}
        else:
            print(f"📝 No existing consistency groups file found. Creating new one at {self.storage_file}")
            self.groups = {}
            self._save_to_file()
    
    def _save_to_file(self):
        """Save consistency groups to JSON file"""
        try:
            # Convert to dictionary format for JSON serialization
            data = {
                gid: group.model_dump() 
                for gid, group in self.groups.items()
            }
            
            # Write to file with pretty formatting
            with open(self.storage_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            
            print(f"💾 Saved {len(self.groups)} consistency group(s) to {self.storage_file}")
        except Exception as e:
            print(f"❌ Error saving consistency groups: {e}")
            raise
    
    def create_consistency_group(
        self,
        postgres_backup: str,
        postgres_database: str,
        ceph_objects: List[str],
        ceph_bucket_source: str,
        backup_type: str = "full",
        metadata: Optional[Dict[str, Any]] = None
    ) -> ConsistencyGroup:
        """
        Create a new consistency group and save it to file.
        
        Args:
            postgres_backup: PostgreSQL backup filename
            postgres_database: PostgreSQL database name
            ceph_objects: List of Ceph object filenames
            ceph_bucket_source: Source S3 bucket name
            backup_type: Type of backup (full, incremental, base)
            metadata: Additional metadata dictionary
        
        Returns:
            ConsistencyGroup: The created consistency group
        """
        # Generate unique group ID
        timestamp = datetime.utcnow()
        group_id = f"cg_{timestamp.strftime('%Y%m%d_%H%M%S')}"
        
        # Ensure unique ID
        counter = 1
        original_id = group_id
        while group_id in self.groups:
            group_id = f"{original_id}_{counter}"
            counter += 1
        
        # Create consistency group
        group = ConsistencyGroup(
            group_id=group_id,
            timestamp=timestamp.isoformat() + "Z",
            postgres_backup=postgres_backup,
            postgres_database=postgres_database,
            ceph_objects=ceph_objects,
            ceph_bucket_source=ceph_bucket_source,
            backup_type=backup_type,
            metadata=metadata or {},
            status="active"
        )
        
        # Store in memory and save to file
        self.groups[group_id] = group
        self._save_to_file()
        
        print(f"✅ Created consistency group: {group_id}")
        return group
    
    def get_group(self, group_id: str) -> Optional[ConsistencyGroup]:
        """Get a specific consistency group by ID"""
        return self.groups.get(group_id)
    
    def list_all_groups(self) -> List[Dict[str, Any]]:
        """List all consistency groups as dictionaries"""
        return [group.model_dump() for group in self.groups.values()]
    
    def list_groups_by_database(self, database: str) -> List[ConsistencyGroup]:
        """List consistency groups for a specific database"""
        return [
            group for group in self.groups.values() 
            if group.postgres_database == database
        ]
    
    def get_latest_group(self, database: str) -> Optional[ConsistencyGroup]:
        """Get the most recent consistency group for a database"""
        database_groups = self.list_groups_by_database(database)
        if not database_groups:
            return None
        
        # Sort by timestamp (most recent first)
        sorted_groups = sorted(
            database_groups, 
            key=lambda x: x.timestamp, 
            reverse=True
        )
        return sorted_groups[0]
    
    def delete_group(self, group_id: str) -> bool:
        """
        Delete a consistency group.
        
        Args:
            group_id: ID of the group to delete
        
        Returns:
            bool: True if deleted, False if not found
        """
        if group_id in self.groups:
            del self.groups[group_id]
            self._save_to_file()
            print(f"🗑️ Deleted consistency group: {group_id}")
            return True
        return False
    
    def update_group_status(self, group_id: str, status: str) -> bool:
        """
        Update the status of a consistency group.
        
        Args:
            group_id: ID of the group to update
            status: New status (active, archived, deleted)
        
        Returns:
            bool: True if updated, False if not found
        """
        if group_id in self.groups:
            self.groups[group_id].status = status
            self._save_to_file()
            print(f"✏️ Updated consistency group {group_id} status to: {status}")
            return True
        return False
    
    def get_groups_summary(self) -> Dict[str, Any]:
        """Get a summary of all consistency groups"""
        total = len(self.groups)
        by_database = {}
        by_status = {}
        
        for group in self.groups.values():
            # Count by database
            db = group.postgres_database
            by_database[db] = by_database.get(db, 0) + 1
            
            # Count by status
            status = group.status
            by_status[status] = by_status.get(status, 0) + 1
        
        return {
            "total_groups": total,
            "by_database": by_database,
            "by_status": by_status,
            "storage_file": str(self.storage_file)
        }