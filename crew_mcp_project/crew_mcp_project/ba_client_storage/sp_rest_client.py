import requests

class StorageProtectClient:
    """
    Simple IBM Storage Protect REST API client for backup/restore management.
    """

    def __init__(self, base_url, username, password):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.token = None

    def signon(self):
        """Authenticate with IBM Storage Protect REST API."""
        url = f"{self.base_url}/api/v1/auth/token"
        data = {"grant_type": "password", "username": self.username, "password": self.password}
        response = requests.post(url, data=data, verify=False)
        if response.status_code == 200:
            self.token = response.json().get("access_token")
            return {"status": "ok", "token": self.token}
        else:
            raise Exception(f"Failed to sign on: {response.text}")

    def start_backup(self, directory, description="Backup job"):
        """Start a backup via IBM SP REST API."""
        if not self.token:
            raise Exception("Not authenticated. Call signon() first.")
        url = f"{self.base_url}/api/v1/backup/start"
        headers = {"Authorization": f"Bearer {self.token}"}
        payload = {"directory": directory, "description": description}
        response = requests.post(url, json=payload, headers=headers, verify=False)
        if response.status_code in (200, 201):
            return {"status": "started", "response": response.json()}
        else:
            raise Exception(f"Backup failed: {response.text}")

    def list_backups(self):
        """List backups available via IBM SP REST."""
        if not self.token:
            raise Exception("Not authenticated. Call signon() first.")
        url = f"{self.base_url}/api/v1/backup/list"
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(url, headers=headers, verify=False)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"List backups failed: {response.text}")

    def restore_backup(self, backup_id, target_dir):
        """Restore a backup from IBM SP REST."""
        if not self.token:
            raise Exception("Not authenticated. Call signon() first.")
        url = f"{self.base_url}/api/v1/restore/start"
        headers = {"Authorization": f"Bearer {self.token}"}
        payload = {"backup_id": backup_id, "target_dir": target_dir}
        response = requests.post(url, json=payload, headers=headers, verify=False)
        if response.status_code in (200, 201):
            return {"status": "restore_started", "response": response.json()}
        else:
            raise Exception(f"Restore failed: {response.text}")
