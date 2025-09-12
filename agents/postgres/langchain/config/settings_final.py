#!/usr/bin/env python3
"""
Configuration settings for the Postgres Backup and Restore System with separate server support.
"""

import os
from typing import Dict, Any

class Settings:
    """Configuration settings for the backup and restore system."""
    
    # Database configurations
    DB1_CONFIG = {
        "host": os.getenv("DB1_HOST", "localhost"),
        "port": int(os.getenv("DB1_PORT", "5432")),
        "database": os.getenv("DB1_DATABASE", "customer_db"),
        "user": os.getenv("DB1_USER", "postgres"),
        "password": os.getenv("DB1_PASSWORD", "postgres")
    }
    
    DB2_CONFIG = {
        "host": os.getenv("DB2_HOST", "localhost"),
        "port": int(os.getenv("DB2_PORT", "5432")),
        "database": os.getenv("DB2_DATABASE", "employee_db"),
        "user": os.getenv("DB2_USER", "postgres"),
        "password": os.getenv("DB2_PASSWORD", "postgres")
    }
    
    # Server-specific configurations
    CUSTOMER_SERVER_CONFIG = {
        "host": os.getenv("CUSTOMER_SERVER_HOST", "localhost"),
        "port": int(os.getenv("CUSTOMER_SERVER_PORT", "5432")),
        "database": os.getenv("CUSTOMER_SERVER_DATABASE", "customer_db"),
        "user": os.getenv("CUSTOMER_SERVER_USER", "postgres"),
        "password": os.getenv("CUSTOMER_SERVER_PASSWORD", "postgres"),
        "server_name": "customerServer"
    }
    
    EMPLOYEE_SERVER_CONFIG = {
        "host": os.getenv("EMPLOYEE_SERVER_HOST", "localhost"),
        "port": int(os.getenv("EMPLOYEE_SERVER_PORT", "5432")),
        "database": os.getenv("EMPLOYEE_SERVER_DATABASE", "employee_db"),
        "user": os.getenv("EMPLOYEE_SERVER_USER", "postgres"),
        "password": os.getenv("EMPLOYEE_SERVER_PASSWORD", "postgres"),
        "server_name": "employeeServer"
    }
    
    # Update DB1 and DB2 configs to point to server configs
    DB1_CONFIG = CUSTOMER_SERVER_CONFIG
    DB2_CONFIG = EMPLOYEE_SERVER_CONFIG
    
    # Backup configuration
    BACKUP_DIR = os.getenv("BACKUP_DIR", "./backups")
    BACKUP_RETENTION_DAYS = int(os.getenv("BACKUP_RETENTION_DAYS", "30"))
    
    # pgBackRest configuration - Separate stanzas but same backup path for simplicity
    PGBACKREST_CUSTOMER_STANZA = os.getenv("PGBACKREST_CUSTOMER_STANZA", "customer_demo")
    PGBACKREST_CUSTOMER_BACKUP_PATH = os.getenv("PGBACKREST_CUSTOMER_BACKUP_PATH", "/Users/aarthiprashanth/backups/pgbackrest_customer")
    PGBACKREST_CUSTOMER_PGDATA = os.getenv("PGBACKREST_CUSTOMER_PGDATA", "/Users/aarthiprashanth/Library/Application Support/Postgres/var-17")
    
    PGBACKREST_EMPLOYEE_STANZA = os.getenv("PGBACKREST_EMPLOYEE_STANZA", "employee_demo")
    PGBACKREST_EMPLOYEE_BACKUP_PATH = os.getenv("PGBACKREST_EMPLOYEE_BACKUP_PATH", "/Users/aarthiprashanth/backups/pgbackrest_customer")
    PGBACKREST_EMPLOYEE_PGDATA = os.getenv("PGBACKREST_EMPLOYEE_PGDATA", "/Users/aarthiprashanth/Library/Application Support/Postgres/var-17")
    
    # Legacy support for existing pgBackRest configs
    PGBACKREST_STANZA = PGBACKREST_CUSTOMER_STANZA
    PGBACKREST_BACKUP_PATH = PGBACKREST_CUSTOMER_BACKUP_PATH
    PGBACKREST_PGDATA = PGBACKREST_CUSTOMER_PGDATA
    
    # Backup scheduling configuration
    FULL_BACKUP_SCHEDULE = os.getenv("FULL_BACKUP_SCHEDULE", "weekly")  # weekly, daily
    INCREMENTAL_BACKUP_SCHEDULE = os.getenv("INCREMENTAL_BACKUP_SCHEDULE", "2min")  # 2min, hourly, daily
    INCREMENTAL_BACKUP_INTERVAL_MINUTES = int(os.getenv("INCREMENTAL_BACKUP_INTERVAL_MINUTES", "2"))  # 2 minutes
    
    # MCP Server configuration
    MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://localhost:8082")
    
    # Logging configuration
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_DIR = os.getenv("LOG_DIR", "./logs")
    
    @classmethod
    def get_server_config(cls, server_name: str) -> Dict[str, Any]:
        """Get configuration for a specific server."""
        if server_name == "customerServer":
            return cls.CUSTOMER_SERVER_CONFIG
        elif server_name == "employeeServer":
            return cls.EMPLOYEE_SERVER_CONFIG
        else:
            raise ValueError(f"Unknown server: {server_name}")
    
    @classmethod
    def get_pgbackrest_config(cls, server_name: str) -> Dict[str, str]:
        """Get pgBackRest configuration for a specific server."""
        if server_name == "customerServer":
            return {
                "stanza": cls.PGBACKREST_CUSTOMER_STANZA,
                "backup_path": cls.PGBACKREST_CUSTOMER_BACKUP_PATH,
                "pgdata": cls.PGBACKREST_CUSTOMER_PGDATA,
                "server_name": server_name
            }
        elif server_name == "employeeServer":
            return {
                "stanza": cls.PGBACKREST_EMPLOYEE_STANZA,
                "backup_path": cls.PGBACKREST_EMPLOYEE_BACKUP_PATH,
                "pgdata": cls.PGBACKREST_EMPLOYEE_PGDATA,
                "server_name": server_name
            }
        else:
            raise ValueError(f"Unknown server: {server_name}")

# Create a global settings instance
settings = Settings()
