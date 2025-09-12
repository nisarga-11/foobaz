import os
from typing import Dict, Any
from dotenv import load_dotenv

load_dotenv()

class Settings:
    """Configuration settings for the Postgres backup and restore system."""
    
    # Database configurations - Separate servers for customer and employee databases
    CUSTOMER_SERVER_CONFIG = {
        "host": os.getenv("CUSTOMER_SERVER_HOST", "customerServer"),
        "port": int(os.getenv("CUSTOMER_SERVER_PORT", "5432")),
        "database": os.getenv("CUSTOMER_DB_NAME", "customer_db"),
        "user": os.getenv("CUSTOMER_DB_USER", "postgres"),
        "password": os.getenv("CUSTOMER_DB_PASSWORD", ""),
        "server_name": "customerServer"
    }
    
    EMPLOYEE_SERVER_CONFIG = {
        "host": os.getenv("EMPLOYEE_SERVER_HOST", "employeeServer"),
        "port": int(os.getenv("EMPLOYEE_SERVER_PORT", "5432")),
        "database": os.getenv("EMPLOYEE_DB_NAME", "employee_db"),
        "user": os.getenv("EMPLOYEE_DB_USER", "postgres"),
        "password": os.getenv("EMPLOYEE_DB_PASSWORD", ""),
        "server_name": "employeeServer"
    }
    
    # Legacy support for existing DB1/DB2 configs
    DB1_CONFIG = CUSTOMER_SERVER_CONFIG
    DB2_CONFIG = EMPLOYEE_SERVER_CONFIG
    
    # Ollama configuration
    OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama2")
    
    # Backup configuration
    BACKUP_DIR = os.getenv("BACKUP_DIR", "./backups")
    BACKUP_RETENTION_DAYS = int(os.getenv("BACKUP_RETENTION_DAYS", "30"))
    
    # pgBackRest configuration - Use same stanza and path for both servers (simplified for demo)
    PGBACKREST_CUSTOMER_STANZA = os.getenv("PGBACKREST_CUSTOMER_STANZA", "demo")
    PGBACKREST_CUSTOMER_BACKUP_PATH = os.getenv("PGBACKREST_CUSTOMER_BACKUP_PATH", "/Users/aarthiprashanth/backups/pgbackrest_customer")
    PGBACKREST_CUSTOMER_PGDATA = os.getenv("PGBACKREST_CUSTOMER_PGDATA", "/Users/aarthiprashanth/Library/Application Support/Postgres/var-17")
    
    PGBACKREST_EMPLOYEE_STANZA = os.getenv("PGBACKREST_EMPLOYEE_STANZA", "demo")
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
    
    # MCP tool configurations
    MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://localhost:8082")
    
    # Agent configurations
    AGENT_NAMES = {
        "db1": "CustomerDB_Agent",
        "db2": "EmployeeDB_Agent"
    }
    
    # Database relationships (for cross-database checks)
    DATABASE_RELATIONSHIPS = {
        "customer_db": ["employee_db"],
        "employee_db": ["customer_db"]
    }
    
    @classmethod
    def get_database_config(cls, db_name: str) -> Dict[str, Any]:
        """Get configuration for a specific database."""
        if db_name == "db1" or db_name == "customer_db":
            return cls.CUSTOMER_SERVER_CONFIG
        elif db_name == "db2" or db_name == "employee_db":
            return cls.EMPLOYEE_SERVER_CONFIG
        else:
            raise ValueError(f"Unknown database: {db_name}")
    
    @classmethod
    def get_server_config(cls, server_name: str) -> Dict[str, Any]:
        """Get configuration for a specific server."""
        if server_name == "customerServer" or server_name == "customer_server":
            return cls.CUSTOMER_SERVER_CONFIG
        elif server_name == "employeeServer" or server_name == "employee_server":
            return cls.EMPLOYEE_SERVER_CONFIG
        else:
            raise ValueError(f"Unknown server: {server_name}")
    
    @classmethod
    def get_pgbackrest_config(cls, server_name: str) -> Dict[str, str]:
        """Get pgBackRest configuration for a specific server."""
        if server_name == "customerServer" or server_name == "customer_server":
            return {
                "stanza": cls.PGBACKREST_CUSTOMER_STANZA,
                "backup_path": cls.PGBACKREST_CUSTOMER_BACKUP_PATH,
                "pgdata": cls.PGBACKREST_CUSTOMER_PGDATA
            }
        elif server_name == "employeeServer" or server_name == "employee_server":
            return {
                "stanza": cls.PGBACKREST_EMPLOYEE_STANZA,
                "backup_path": cls.PGBACKREST_EMPLOYEE_BACKUP_PATH,
                "pgdata": cls.PGBACKREST_EMPLOYEE_PGDATA
            }
        else:
            raise ValueError(f"Unknown server: {server_name}")
    
    @classmethod
    def get_related_databases(cls, db_name: str) -> list:
        """Get list of related databases for cross-database operations."""
        return cls.DATABASE_RELATIONSHIPS.get(db_name, [])

# Global settings instance
settings = Settings()
