import re, json

def ask_ollama(prompt: str) -> dict:
    """
    Query the local Ollama model and return a clean JSON dictionary.
    """
    import subprocess

    try:
        result = subprocess.run(
            ["ollama", "run", "llama3.1:8b", prompt],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode != 0:
            return {"error": result.stderr.strip(), "raw": result.stdout.strip()}

        text = result.stdout.strip()

        # Extract JSON code block
        match = re.search(r"```json\s*(\{.*?\})\s*```", text, re.DOTALL)
        if match:
            json_text = match.group(1)
            return json.loads(json_text)
        else:
            # fallback: return raw text
            return {"text": text}

    except json.JSONDecodeError:
        return {"error": "Invalid JSON from Ollama", "raw": text}
    except subprocess.TimeoutExpired:
        return {"error": "Ollama run timed out"}
    except Exception as e:
        return {"error": str(e)}