"""Ollama LLM integration helper for LangChain."""

import os
from typing import Optional

from langchain_ollama import ChatOllama
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def create_ollama_llm(
    model: Optional[str] = None,
    base_url: Optional[str] = None,
    temperature: Optional[float] = None,
    num_ctx: Optional[int] = None,
    **kwargs
) -> ChatOllama:
    """
    Create a ChatOllama instance with configuration from environment variables.

    Args:
        model: Ollama model name (defaults to OLLAMA_MODEL env var)
        base_url: Ollama server base URL (defaults to OLLAMA_BASE_URL env var)
        temperature: Sampling temperature (defaults to OLLAMA_TEMPERATURE env var)
        num_ctx: Context window size (defaults to OLLAMA_NUM_CTX env var)
        **kwargs: Additional arguments passed to ChatOllama

    Returns:
        Configured ChatOllama instance

    Raises:
        ValueError: If required environment variables are missing
    """
    # Get configuration from environment variables with fallbacks
    model = model or os.getenv("OLLAMA_MODEL", "llama3.1:8b-instruct")
    base_url = base_url or os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    
    # Parse optional numeric parameters
    if temperature is None:
        temp_str = os.getenv("OLLAMA_TEMPERATURE", "0.2")
        try:
            temperature = float(temp_str)
        except ValueError:
            temperature = 0.2
    
    if num_ctx is None:
        ctx_str = os.getenv("OLLAMA_NUM_CTX", "8192")
        try:
            num_ctx = int(ctx_str)
        except ValueError:
            num_ctx = 8192

    # Validate required parameters
    if not model:
        raise ValueError("OLLAMA_MODEL environment variable is required")
    if not base_url:
        raise ValueError("OLLAMA_BASE_URL environment variable is required")

    # Create and return ChatOllama instance
    return ChatOllama(
        model=model,
        base_url=base_url,
        temperature=temperature,
        num_ctx=num_ctx,
        **kwargs
    )


def create_supervisor_llm(**kwargs) -> ChatOllama:
    """
    Create Ollama LLM specifically configured for the Supervisor agent.
    
    Returns:
        ChatOllama configured for supervisor use
    """
    return create_ollama_llm(
        temperature=0.1,  # Lower temperature for more consistent routing decisions
        timeout=60,  # 60 second timeout
        top_k=10,   # Limit token selection for more focused responses
        top_p=0.9,  # Nucleus sampling for better quality
        **kwargs
    )


def create_backup_agent_llm(**kwargs) -> ChatOllama:
    """
    Create Ollama LLM specifically configured for Backup agents.
    
    Returns:
        ChatOllama configured for backup agent use
    """
    return create_ollama_llm(
        temperature=0.2,  # Slightly higher temperature for more natural responses
        **kwargs
    )


def test_ollama_connection(
    model: Optional[str] = None,
    base_url: Optional[str] = None
) -> bool:
    """
    Test connection to Ollama server.

    Args:
        model: Model to test (optional)
        base_url: Base URL to test (optional)

    Returns:
        True if connection successful, False otherwise
    """
    try:
        llm = create_ollama_llm(model=model, base_url=base_url)
        # Try a simple invocation to test connectivity
        response = llm.invoke("Hello, can you respond with just 'OK'?")
        return "OK" in str(response).upper()
    except Exception as e:
        print(f"Ollama connection test failed: {e}")
        return False


if __name__ == "__main__":
    # Test script
    print("Testing Ollama connection...")
    
    # Load environment variables
    load_dotenv()
    
    # Test connection
    if test_ollama_connection():
        print(" Ollama connection successful!")
        
        # Test LLM creation
        try:
            supervisor_llm = create_supervisor_llm()
            backup_llm = create_backup_agent_llm()
            
            print(f" Supervisor LLM: {supervisor_llm.model} @ {supervisor_llm.base_url}")
            print(f" Backup Agent LLM: {backup_llm.model} @ {backup_llm.base_url}")
            
        except Exception as e:
            print(f" Error creating LLMs: {e}")
    else:
        print(" Ollama connection failed!")
        print("Make sure Ollama is running and the model is available.")
        print("Try: ollama pull llama3.1:8b-instruct")
