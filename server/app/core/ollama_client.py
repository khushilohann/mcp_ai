import os

try:
    import ollama
except Exception:  # pragma: no cover - fallback for environments without ollama
    ollama = None


def ask_ollama(prompt: str):
    """Ask Ollama for a response. Use a lightweight mock if OLLAMA_MOCK is set (default true).

    Mock mode helps local development without an Ollama server.
    """
    mock = os.getenv("OLLAMA_MOCK", "true").lower() in ("1", "true", "yes")
    if mock:
        # Very simple mock: if prompt asks to convert NL->SQL we'll return a plausible SQL
        lower = prompt.lower()
        if "convert" in lower and "sql" in lower:
            return "SELECT * FROM users LIMIT 10;"
        if "explain" in lower or "explain query" in lower:
            return "MOCK EXPLAIN: SCAN TABLE users"
        return "MOCK_RESPONSE"

    if ollama is None:
        raise RuntimeError("Ollama not available and OLLAMA_MOCK is false")

    response = ollama.chat(
        model=os.getenv("OLLAMA_MODEL", "mistral"),
        messages=[{"role": "user", "content": prompt}],
    )
    # Ollama returns a mapping with message.content in many setups
    return response.get("message", {}).get("content") if isinstance(response, dict) else response
