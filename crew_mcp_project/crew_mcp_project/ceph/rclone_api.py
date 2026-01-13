import requests
import json

class RcloneAPI:
    def __init__(self, host="http://localhost:5572", user=None, password=None):
        self.host = host
        self.auth = (user, password) if user and password else None

    def call(self, endpoint, params=None):
        """Send a POST request to rclone RC API."""
        url = f"{self.host}/{endpoint}"
        try:
            response = requests.post(url, json=params or {}, auth=self.auth)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print("Error calling rclone:", e)
            return None

    # ---------------------------
    # Utility API methods
    # ---------------------------

    def list_commands(self):
        return self.call("rc/list")

    def list_dir(self, remote_path):
        return self.call("operations/list", {
            "fs": remote_path,
            "remote": ""
        })

    def copy(self, src, dst, async_job=False):
        return self.call("operations/copyfile", {
            "srcFs": src,
            "dstFs": dst,
            "_async": async_job
        })

    def sync(self, src, dst, async_job=False):
        return self.call("sync/sync", {
            "srcFs": src,
            "dstFs": dst,
            "_async": async_job
        })

    def job_status(self, jobid):
        return self.call("job/status", {"jobid": jobid})

    def job_list(self):
        return self.call("job/list")

    def test_connection(self):
        return self.call("rc/noop", {"ping": "ok"})
