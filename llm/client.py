"""Abstract LLM interface for structured JSON adjudication."""
from __future__ import annotations

import json
import re
from abc import ABC, abstractmethod
from typing import Any


class LLMResponseError(Exception):
    """Raised when the model returns unparseable or invalid output."""


class LLMClient(ABC):
    """
    Pluggable backend (Ollama OpenAI-compatible, OpenAI, etc.).
    """

    @abstractmethod
    def complete_json(
        self,
        *,
        system: str,
        user: str,
        model: str | None = None,
        temperature: float = 0.1,
    ) -> dict[str, Any]:
        """
        Return a parsed JSON object from the model.
        Implementations must extract JSON from markdown fences if present.
        """

    @staticmethod
    def parse_json_loose(text: str) -> dict[str, Any]:
        """Strip markdown fences and parse JSON; raise LLMResponseError on failure."""
        t = text.strip()
        if "```json" in t:
            m = re.search(r"```json\s*([\s\S]*?)\s*```", t, re.I)
            if m:
                t = m.group(1).strip()
        elif "```" in t:
            m = re.search(r"```\s*([\s\S]*?)\s*```", t)
            if m:
                t = m.group(1).strip()
        try:
            out = json.loads(t)
            if not isinstance(out, dict):
                raise LLMResponseError("JSON root must be an object")
            return out
        except json.JSONDecodeError as e:
            raise LLMResponseError(f"Invalid JSON: {e}") from e
