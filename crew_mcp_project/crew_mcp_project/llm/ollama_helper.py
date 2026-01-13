# llm/ollama_helper.py
import subprocess

# -----------------------------
# Ollama LLM helper
# -----------------------------
def create_backup_agent_llm(model: str = "llama3") -> object:
    """
    Returns an Ollama agent that can invoke the given model via CLI.
    """
    class OllamaAgent:
        def __init__(self, model_name):
            self.model_name = model_name

        def invoke(self, prompt: str) -> str:
            """
            Sends a prompt to the Ollama model and returns its output.
            """
            try:
                result = subprocess.run(
                    ["ollama", "run", self.model_name],
                    input=prompt,                   # send prompt via stdin
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                    check=True
                )
                return result.stdout.strip()
            except subprocess.CalledProcessError as e:
                print(f"❌ Ollama query failed: {e.stderr}")
                return ""

    return OllamaAgent(model)


# -----------------------------
# pgBackRest helper functions
# -----------------------------
def pgbackrest_backup(stanza: str) -> str:
    """Run pgBackRest full backup for a stanza"""
    cmd = ["sudo", "-u", "postgres", "pgbackrest", f"--stanza={stanza}", "backup"]
    try:
        result = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True
        )
        return f"✅ Backup completed for stanza {stanza}:\n{result.stdout}"
    except subprocess.CalledProcessError as e:
        return f"❌ Backup failed for stanza {stanza}:\n{e.stderr}"


def pgbackrest_restore(stanza: str) -> str:
    """
    Restore the latest backup for a stanza.
    WARNING: The DB will be restored to the latest backup, overwriting existing DB files.
    """
    cmd = ["sudo", "-u", "postgres", "pgbackrest", f"--stanza={stanza}", "restore"]
    try:
        result = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True
        )
        return f"✅ Restore completed for stanza {stanza}:\n{result.stdout}"
    except subprocess.CalledProcessError as e:
        return f"❌ Restore failed for stanza {stanza}:\n{e.stderr}"


def pgbackrest_check(stanza: str) -> str:
    """Check the configuration and WAL archive for a stanza"""
    cmd = ["sudo", "-u", "postgres", "pgbackrest", f"--stanza={stanza}", "check"]
    try:
        result = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True
        )
        return f"✅ Check successful for stanza {stanza}:\n{result.stdout}"
    except subprocess.CalledProcessError as e:
        return f"❌ Check failed for stanza {stanza}:\n{e.stderr}"
