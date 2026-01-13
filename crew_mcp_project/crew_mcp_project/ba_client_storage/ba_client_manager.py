import os
import subprocess
import datetime

class BAClientManager:
    def __init__(self):
        self.dsmcad_service = "dsmcad"
        self.log_file = "/var/log/ba_client_manager.log"

    def _log(self, message):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        with open(self.log_file, "a") as f:
            f.write(f"[{timestamp}] {message}\n")

    def backup_directory(self, backup_dir: str):
        """Archive a directory using BA Client or fallback to tar."""
        if not os.path.exists(backup_dir):
            return f"❌ Directory not found: {backup_dir}"

        try:
            archive_name = f"{backup_dir.rstrip('/')}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.tar.gz"
            subprocess.run(["tar", "-czf", archive_name, "-C", backup_dir, "."], check=True)
            self._log(f"Archived directory: {archive_name}")
            return f"✅ Archived to {archive_name}"
        except subprocess.CalledProcessError as e:
            self._log(f"Archive failed: {e.stderr}")
            return f"❌ Archive failed: {e.stderr}"

    def check_status(self):
        """Check if dsmcad service is running."""
        try:
            result = subprocess.run(
                ["systemctl", "is-active", self.dsmcad_service],
                capture_output=True, text=True
            )
            status = result.stdout.strip()
            self._log(f"DSMcad status: {status}")
            return status
        except Exception as e:
            self._log(f"Error checking status: {e}")
            return f"❌ Error checking status: {e}"

    def start_dsmcad(self):
        """Start dsmcad service."""
        try:
            subprocess.run(
                ["systemctl", "start", self.dsmcad_service],
                check=True
            )
            self._log("DSMcad started successfully.")
            return "✅ dsmcad started"
        except subprocess.CalledProcessError as e:
            self._log(f"DSMcad start failed: {e.stderr}")
            return f"❌ DSMcad start failed: {e.stderr}"
