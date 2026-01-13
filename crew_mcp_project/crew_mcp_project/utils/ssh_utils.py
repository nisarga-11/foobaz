import subprocess
import logging

def run_remote_command(host: str, command: str) -> tuple[int, str, str]:
    ssh_command = ['ssh', host, command]
    try:
        logging.info(f"Executing remote command on {host}: {command}")
        result = subprocess.run(
            ssh_command,
            capture_output=True,
            text=True,
            check=True
        )
        return 0, result.stdout, result.stderr
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed on {host}: {e.stderr}")
        return e.returncode, e.stdout, e.stderr
    except FileNotFoundError:
        logging.error("SSH command not found. Make sure ssh is in your PATH.")
        return 1, "", "SSH command not found."