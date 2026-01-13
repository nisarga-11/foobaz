# settings.py
import os
from typing import Dict, Any
from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Configuration settings for the backup and restore system."""

    # Customer PostgreSQL server configuration
    CUSTOMER_SERVER_CONFIG = {
        "host": os.getenv("CUSTOMER_SERVER_HOST", "localhost"),
        "port": int(os.getenv("CUSTOMER_SERVER_PORT", "5444")),
        "database": os.getenv("CUSTOMER_SERVER_DATABASE", "customer_db"),
        "user": os.getenv("CUSTOMER_SERVER_USER", "postgres"),
        "password": os.getenv("CUSTOMER_SERVER_PASSWORD", "postgres"),
        "server_name": "customerServer"
    }

    # Employee PostgreSQL server configuration
    EMPLOYEE_SERVER_CONFIG = {
        "host": os.getenv("EMPLOYEE_SERVER_HOST", "localhost"),
        "port": int(os.getenv("EMPLOYEE_SERVER_PORT", "5432")),
        "database": os.getenv("EMPLOYEE_SERVER_DATABASE", "employee_db"),
        "user": os.getenv("EMPLOYEE_SERVER_USER", "postgres"),
        "password": os.getenv("EMPLOYEE_SERVER_PASSWORD", "postgres"),
        "server_name": "employeeServer"
    }

    # pgBackRest configuration
    PGBACKREST_CUSTOMER_STANZA = os.getenv("PGBACKREST_CUSTOMER_STANZA", "db2_cluster")
    PGBACKREST_CUSTOMER_PGDATA = os.getenv("PGBACKREST_CUSTOMER_PGDATA", "/var/lib/postgresql/17/db2_cluster")

    PGBACKREST_EMPLOYEE_STANZA = os.getenv("PGBACKREST_EMPLOYEE_STANZA", "main")
    PGBACKREST_EMPLOYEE_PGDATA = os.getenv("PGBACKREST_EMPLOYEE_PGDATA", "/var/lib/postgresql/16/main")

    PGBACKREST_REPO_PATH = os.getenv("PGBACKREST_REPO_PATH", "/var/lib/pgbackrest/repo")

    # Ollama configuration for CrewAI (used as the LLM backend)
    OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1")

    @classmethod
    def get_server_config(cls, server_name: str) -> Dict[str, Any]:
        """Retrieve the server configuration by server name."""
        if server_name == "customerServer":
            return cls.CUSTOMER_SERVER_CONFIG
        elif server_name == "employeeServer":
            return cls.EMPLOYEE_SERVER_CONFIG
        else:
            raise ValueError(f"Unknown server: {server_name}")


# Create a single instance to be imported elsewhere
settings = Settings()
