# backup_restore_base.py
from pydantic import BaseModel
from abc import ABC, abstractmethod

class BackupRestoreBaseAgent(BaseModel, ABC):
    name: str
    stanza: str
    role: str = "backup_agent"
    goal: str = "Manage backups and restores"
    backstory: str = "Base agent for backup and restore operations"

    class Config:
        arbitrary_types_allowed = True

    @abstractmethod
    def perform_backup(self):
        """Perform a backup operation."""
        pass

    @abstractmethod
    def list_backups(self):
        """Return a list of backup folder names."""
        pass

    @abstractmethod
    def perform_restore(self, backup_name: str):
        """Restore from a given backup."""
        pass