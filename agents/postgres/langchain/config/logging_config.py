#!/usr/bin/env python3
"""
Logging configuration for the Postgres Backup and Restore System with pgBackRest
"""

import logging
import logging.handlers
import os
from datetime import datetime
from pathlib import Path

class BackupLoggingConfig:
    """Centralized logging configuration for backup operations."""
    
    def __init__(self, log_dir: str = "logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.timestamp = datetime.now().strftime("%Y%m%d")
        
        # Setup loggers
        self._setup_loggers()
    
    def _setup_loggers(self):
        """Setup all loggers with appropriate handlers."""
        
        # 1. Main system logger
        self._setup_system_logger()
        
        # 2. Backup operations logger
        self._setup_backup_logger()
        
        # 3. pgBackRest operations logger
        self._setup_pgbackrest_logger()
        
        # 4. Agent operations logger
        self._setup_agent_logger()
        
        # 5. Incremental backup specific logger
        self._setup_incremental_backup_logger()
        
        # 6. Error logger
        self._setup_error_logger()
    
    def _setup_system_logger(self):
        """Setup main system logger."""
        system_logger = logging.getLogger('system')
        system_logger.setLevel(logging.INFO)
        
        # Remove existing handlers
        system_logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        system_logger.addHandler(console_handler)
        
        # File handler with rotation
        file_handler = logging.handlers.RotatingFileHandler(
            self.log_dir / f'system_{self.timestamp}.log',
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        system_logger.addHandler(file_handler)
    
    def _setup_backup_logger(self):
        """Setup backup operations logger."""
        backup_logger = logging.getLogger('backup_operations')
        backup_logger.setLevel(logging.INFO)
        
        # Remove existing handlers
        backup_logger.handlers.clear()
        
        # File handler for all backup operations
        backup_handler = logging.handlers.RotatingFileHandler(
            self.log_dir / f'backup_operations_{self.timestamp}.log',
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        backup_handler.setLevel(logging.INFO)
        backup_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        backup_handler.setFormatter(backup_formatter)
        backup_logger.addHandler(backup_handler)
    
    def _setup_pgbackrest_logger(self):
        """Setup pgBackRest operations logger."""
        pgbackrest_logger = logging.getLogger('pgbackrest_operations')
        pgbackrest_logger.setLevel(logging.INFO)
        
        # Remove existing handlers
        pgbackrest_logger.handlers.clear()
        
        # File handler for pgBackRest operations
        pgbackrest_handler = logging.handlers.RotatingFileHandler(
            self.log_dir / f'pgbackrest_{self.timestamp}.log',
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        pgbackrest_handler.setLevel(logging.INFO)
        pgbackrest_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        pgbackrest_handler.setFormatter(pgbackrest_formatter)
        pgbackrest_logger.addHandler(pgbackrest_handler)
    
    def _setup_agent_logger(self):
        """Setup agent operations logger."""
        agent_logger = logging.getLogger('agent_operations')
        agent_logger.setLevel(logging.INFO)
        
        # Remove existing handlers
        agent_logger.handlers.clear()
        
        # File handler for agent operations
        agent_handler = logging.handlers.RotatingFileHandler(
            self.log_dir / f'agent_operations_{self.timestamp}.log',
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        agent_handler.setLevel(logging.INFO)
        agent_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        agent_handler.setFormatter(agent_formatter)
        agent_logger.addHandler(agent_handler)
    
    def _setup_incremental_backup_logger(self):
        """Setup incremental backup specific logger."""
        incremental_logger = logging.getLogger('incremental_backup')
        incremental_logger.setLevel(logging.INFO)
        
        # Remove existing handlers
        incremental_logger.handlers.clear()
        
        # File handler for incremental backups
        incremental_handler = logging.handlers.RotatingFileHandler(
            self.log_dir / f'incremental_backup_{self.timestamp}.log',
            maxBytes=5*1024*1024,  # 5MB
            backupCount=10
        )
        incremental_handler.setLevel(logging.INFO)
        incremental_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        incremental_handler.setFormatter(incremental_formatter)
        incremental_logger.addHandler(incremental_handler)
    
    def _setup_error_logger(self):
        """Setup error logger for all errors."""
        error_logger = logging.getLogger('errors')
        error_logger.setLevel(logging.ERROR)
        
        # Remove existing handlers
        error_logger.handlers.clear()
        
        # File handler for errors
        error_handler = logging.handlers.RotatingFileHandler(
            self.log_dir / f'errors_{self.timestamp}.log',
            maxBytes=5*1024*1024,  # 5MB
            backupCount=10
        )
        error_handler.setLevel(logging.ERROR)
        error_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(pathname)s:%(lineno)d'
        )
        error_handler.setFormatter(error_formatter)
        error_logger.addHandler(error_handler)
    
    def get_logger(self, name: str) -> logging.Logger:
        """Get a logger instance."""
        return logging.getLogger(name)
    
    def get_incremental_backup_logger(self) -> logging.Logger:
        """Get the incremental backup logger."""
        return logging.getLogger('incremental_backup')
    
    def get_pgbackrest_logger(self) -> logging.Logger:
        """Get the pgBackRest logger."""
        return logging.getLogger('pgbackrest_operations')
    
    def get_backup_logger(self) -> logging.Logger:
        """Get the backup operations logger."""
        return logging.getLogger('backup_operations')
    
    def get_agent_logger(self) -> logging.Logger:
        """Get the agent operations logger."""
        return logging.getLogger('agent_operations')
    
    def get_error_logger(self) -> logging.Logger:
        """Get the error logger."""
        return logging.getLogger('errors')
    
    def get_log_files(self) -> list:
        """Get list of current log files."""
        log_files = []
        
        if self.log_dir.exists():
            for file in self.log_dir.glob('*.log'):
                log_files.append(str(file))
        
        # Also check for existing log files in root directory
        root_logs = ['backup_scheduler.log', 'server.log']
        for log_file in root_logs:
            if Path(log_file).exists():
                log_files.append(log_file)
        
        return sorted(log_files)
    
    def log_incremental_backup_start(self, database_name: str, backup_type: str = "incremental"):
        """Log the start of an incremental backup."""
        logger = self.get_incremental_backup_logger()
        logger.info(f"Starting {backup_type} backup for database: {database_name}")
    
    def log_incremental_backup_success(self, database_name: str, backup_id: str = None, size: str = None):
        """Log successful incremental backup."""
        logger = self.get_incremental_backup_logger()
        message = f"Successfully completed incremental backup for {database_name}"
        if backup_id:
            message += f" - Backup ID: {backup_id}"
        if size:
            message += f" - Size: {size}"
        logger.info(message)
    
    def log_incremental_backup_error(self, database_name: str, error: str):
        """Log incremental backup error."""
        logger = self.get_incremental_backup_logger()
        logger.error(f"Incremental backup failed for {database_name}: {error}")
        
        # Also log to error logger
        error_logger = self.get_error_logger()
        error_logger.error(f"Incremental backup failed for {database_name}: {error}")

# Global logging configuration instance
logging_config = BackupLoggingConfig()

# Convenience functions
def get_incremental_backup_logger():
    """Get the incremental backup logger."""
    return logging_config.get_incremental_backup_logger()

def get_pgbackrest_logger():
    """Get the pgBackRest logger."""
    return logging_config.get_pgbackrest_logger()

def get_backup_logger():
    """Get the backup operations logger."""
    return logging_config.get_backup_logger()

def get_agent_logger():
    """Get the agent operations logger."""
    return logging_config.get_agent_logger()

def get_error_logger():
    """Get the error logger."""
    return logging_config.get_error_logger()

def log_incremental_backup_start(database_name: str, backup_type: str = "incremental"):
    """Log the start of an incremental backup."""
    logging_config.log_incremental_backup_start(database_name, backup_type)

def log_incremental_backup_success(database_name: str, backup_id: str = None, size: str = None):
    """Log successful incremental backup."""
    logging_config.log_incremental_backup_success(database_name, backup_id, size)

def log_incremental_backup_error(database_name: str, error: str):
    """Log incremental backup error."""
    logging_config.log_incremental_backup_error(database_name, error)
