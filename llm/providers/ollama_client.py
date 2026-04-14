"""
OpenAI-compatible HTTP client (Ollama `ollama serve` with OpenAI API, vLLM, LiteLLM, etc.).

Default base URL: http://127.0.0.1:11434/v1
"""
from __future__ import annotations

import os
from typing import Any

import requests

from llm.client import LLMClient, LLMResponseError


class OpenAICompatibleClient(LLMClient):
    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout: int = 120,
    ) -> None:
        self.base_url = (base_url or os.environ.get("LLM_BASE_URL") or "http://127.0.0.1:11434/v1").rstrip("/")
        self.api_key = api_key or os.environ.get("LLM_API_KEY") or "ollama"
        self.timeout = timeout

    def complete_json(
        self,
        *,
        system: str,
        user: str,
        model: str | None = None,
        temperature: float = 0.1,
    ) -> dict[str, Any]:
        model = model or os.environ.get("LLM_MODEL") or "llama3.2"
        url = f"{self.base_url}/chat/completions"
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        payload: dict[str, Any] = {
            "model": model,
            "temperature": temperature,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "response_format": {"type": "json_object"},
        }

        try:
            r = requests.post(url, json=payload, headers=headers, timeout=self.timeout)
            if r.status_code == 400 and "response_format" in (r.text or "").lower():
                payload.pop("response_format", None)
                r = requests.post(url, json=payload, headers=headers, timeout=self.timeout)
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            raise LLMResponseError(f"HTTP error: {e}") from e

        try:
            content = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError) as e:
            raise LLMResponseError(f"Unexpected response shape: {data!r}") from e

        return self.parse_json_loose(content)

    def complete_text(
        self,
        *,
        system: str,
        user: str,
        model: str | None = None,
        temperature: float = 0.5,
    ) -> str:
        """Plain chat completion (no JSON mode) for conversational UIs."""
        model = model or os.environ.get("LLM_MODEL") or "llama3.2"
        url = f"{self.base_url}/chat/completions"
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        payload: dict[str, Any] = {
            "model": model,
            "temperature": temperature,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }

        try:
            r = requests.post(url, json=payload, headers=headers, timeout=self.timeout)
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            raise LLMResponseError(f"HTTP error: {e}") from e

        try:
            content = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError) as e:
            raise LLMResponseError(f"Unexpected response shape: {data!r}") from e

        return (content or "").strip()
