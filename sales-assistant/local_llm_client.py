import requests
from typing import Optional

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "llama3.1"


def call_llm(prompt: str, system_prompt: Optional[str] = None, timeout: int = 120) -> str:
    """
    Вызывает локальную LLM через HTTP (Ollama).
    Возвращает текст ответа.
    """
    full_prompt = prompt
    if system_prompt:
        # простой формат объединения system + user
        full_prompt = f"<<SYS>>\n{system_prompt}\n<</SYS>>\n\n{prompt}"

    payload = {
        "model": MODEL_NAME,
        "prompt": full_prompt,
        "stream": False,
    }

    response = requests.post(OLLAMA_URL, json=payload, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    # У Ollama ответ обычно лежит в поле "response"
    return data.get("response", "").strip()
