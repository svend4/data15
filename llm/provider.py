"""
llm/provider.py
===============
Мультипровайдерный LLM-слой для Orchestrator v6.

Поддерживаемые провайдеры:
  • anthropic  — Claude (claude-3-5-haiku, claude-sonnet-4-6, claude-opus-4-8)
  • openai     — GPT-4o, GPT-4o-mini, o1, o3
  • ollama     — любая локальная модель (llama3, mistral, qwen2.5, ...)
  • minimax    — MiniMax API (обратная совместимость с v5)

Конфигурация через orchestrator/state/config.json:
  {
    "llm": {
      "provider": "anthropic",
      "model": "claude-haiku-4-5-20251001",
      "api_key_env": "ANTHROPIC_API_KEY",
      "temperature": 0.7,
      "max_tokens": 4096,
      "timeout": 60,
      "fallback_chain": ["anthropic", "openai", "ollama"]
    }
  }
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Response dataclass
# ---------------------------------------------------------------------------

@dataclass
class LLMResponse:
    content: str
    provider: str
    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    latency_ms: float = 0.0
    cached: bool = False
    raw: dict = field(default_factory=dict)

    def __str__(self) -> str:
        return self.content


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class LLMProvider(ABC):
    """Базовый класс провайдера. Все провайдеры реализуют только `complete()`."""

    name: str = "base"

    def __init__(self, config: dict) -> None:
        self.config = config
        self.model = config.get("model", self._default_model())
        self.temperature = config.get("temperature", 0.7)
        self.max_tokens = config.get("max_tokens", 4096)
        self.timeout = config.get("timeout", 60)
        self._api_key = self._load_api_key(config)

    def _default_model(self) -> str:
        return "unknown"

    def _load_api_key(self, config: dict) -> Optional[str]:
        env_var = config.get("api_key_env", "")
        key = os.environ.get(env_var, "") or config.get("api_key", "")
        return key or None

    @abstractmethod
    def complete(self, messages: list[dict], **kwargs) -> LLMResponse:
        """Выполнить запрос к модели. messages — список {role, content}."""
        ...

    def ask(self, prompt: str, system: str = "", **kwargs) -> LLMResponse:
        """Упрощённый вызов: один текстовый промпт."""
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        return self.complete(messages, **kwargs)

    def _post_json(self, url: str, payload: dict, headers: dict) -> dict:
        """HTTP POST с urllib (без зависимостей)."""
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={
            "Content-Type": "application/json",
            **headers,
        })
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"[{self.name}] HTTP {e.code}: {body}") from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"[{self.name}] Connection error: {e.reason}") from e

    def is_available(self) -> bool:
        """Проверить доступность провайдера (наличие API-ключа или сервера)."""
        return bool(self._api_key)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} model={self.model!r}>"


# ---------------------------------------------------------------------------
# Anthropic (Claude)
# ---------------------------------------------------------------------------

class AnthropicProvider(LLMProvider):
    """Anthropic Claude API — Messages API v1."""

    name = "anthropic"
    _API_URL = "https://api.anthropic.com/v1/messages"
    _ANTHROPIC_VERSION = "2023-06-01"

    def _default_model(self) -> str:
        return "claude-haiku-4-5-20251001"

    def complete(self, messages: list[dict], **kwargs) -> LLMResponse:
        if not self._api_key:
            raise RuntimeError("[anthropic] ANTHROPIC_API_KEY not set")

        # Отделить system от остальных сообщений
        system = ""
        user_messages = []
        for m in messages:
            if m["role"] == "system":
                system = m["content"]
            else:
                user_messages.append(m)

        payload: dict[str, Any] = {
            "model": kwargs.get("model", self.model),
            "max_tokens": kwargs.get("max_tokens", self.max_tokens),
            "temperature": kwargs.get("temperature", self.temperature),
            "messages": user_messages,
        }
        if system:
            payload["system"] = system

        t0 = time.perf_counter()
        resp = self._post_json(self._API_URL, payload, {
            "x-api-key": self._api_key,
            "anthropic-version": self._ANTHROPIC_VERSION,
        })
        latency = (time.perf_counter() - t0) * 1000

        content = resp["content"][0]["text"]
        usage = resp.get("usage", {})
        return LLMResponse(
            content=content,
            provider=self.name,
            model=resp.get("model", self.model),
            input_tokens=usage.get("input_tokens", 0),
            output_tokens=usage.get("output_tokens", 0),
            latency_ms=latency,
            raw=resp,
        )

    def is_available(self) -> bool:
        return bool(self._api_key)


# ---------------------------------------------------------------------------
# OpenAI (GPT-4o, o1, o3, ...)
# ---------------------------------------------------------------------------

class OpenAIProvider(LLMProvider):
    """OpenAI Chat Completions API."""

    name = "openai"
    _API_URL = "https://api.openai.com/v1/chat/completions"

    def _default_model(self) -> str:
        return "gpt-4o-mini"

    def complete(self, messages: list[dict], **kwargs) -> LLMResponse:
        if not self._api_key:
            raise RuntimeError("[openai] OPENAI_API_KEY not set")

        payload: dict[str, Any] = {
            "model": kwargs.get("model", self.model),
            "messages": messages,
            "max_tokens": kwargs.get("max_tokens", self.max_tokens),
            "temperature": kwargs.get("temperature", self.temperature),
        }

        t0 = time.perf_counter()
        resp = self._post_json(self._API_URL, payload, {
            "Authorization": f"Bearer {self._api_key}",
        })
        latency = (time.perf_counter() - t0) * 1000

        content = resp["choices"][0]["message"]["content"]
        usage = resp.get("usage", {})
        return LLMResponse(
            content=content,
            provider=self.name,
            model=resp.get("model", self.model),
            input_tokens=usage.get("prompt_tokens", 0),
            output_tokens=usage.get("completion_tokens", 0),
            latency_ms=latency,
            raw=resp,
        )


# ---------------------------------------------------------------------------
# Ollama (локальные модели — llama3, mistral, qwen2.5, ...)
# ---------------------------------------------------------------------------

class OllamaProvider(LLMProvider):
    """Ollama REST API — локальный инференс без API-ключей."""

    name = "ollama"

    def _default_model(self) -> str:
        return "llama3"

    def _load_api_key(self, config: dict) -> Optional[str]:
        return "local"  # Ключ не нужен

    def complete(self, messages: list[dict], **kwargs) -> LLMResponse:
        base_url = self.config.get("ollama_url", "http://localhost:11434")
        url = f"{base_url}/api/chat"

        payload: dict[str, Any] = {
            "model": kwargs.get("model", self.model),
            "messages": messages,
            "stream": False,
            "options": {
                "temperature": kwargs.get("temperature", self.temperature),
                "num_predict": kwargs.get("max_tokens", self.max_tokens),
            },
        }

        t0 = time.perf_counter()
        resp = self._post_json(url, payload, {})
        latency = (time.perf_counter() - t0) * 1000

        content = resp.get("message", {}).get("content", "")
        return LLMResponse(
            content=content,
            provider=self.name,
            model=resp.get("model", self.model),
            latency_ms=latency,
            raw=resp,
        )

    def is_available(self) -> bool:
        """Проверить что ollama запущен."""
        try:
            base_url = self.config.get("ollama_url", "http://localhost:11434")
            with urllib.request.urlopen(f"{base_url}/api/tags", timeout=2):
                return True
        except Exception:
            return False


# ---------------------------------------------------------------------------
# MiniMax (обратная совместимость с v5)
# ---------------------------------------------------------------------------

class MiniMaxProvider(LLMProvider):
    """MiniMax API — fallback из v5."""

    name = "minimax"
    _ENDPOINTS = [
        "https://api.minimax.chat/v1/text/chatcompletion_v2",
        "https://api.minimaxi.chat/v1/text/chatcompletion_v2",
    ]

    def _default_model(self) -> str:
        return "MiniMax-Text-01"

    def _load_api_key(self, config: dict) -> Optional[str]:
        return (
            os.environ.get("MINIMAX_API_KEY")
            or config.get("api_key", "")
            or None
        )

    def complete(self, messages: list[dict], **kwargs) -> LLMResponse:
        if not self._api_key:
            raise RuntimeError("[minimax] MINIMAX_API_KEY not set")

        payload: dict[str, Any] = {
            "model": kwargs.get("model", self.model),
            "messages": messages,
            "max_tokens": kwargs.get("max_tokens", self.max_tokens),
            "temperature": kwargs.get("temperature", self.temperature),
        }

        last_error: Exception | None = None
        for endpoint in self._ENDPOINTS:
            try:
                t0 = time.perf_counter()
                resp = self._post_json(endpoint, payload, {
                    "Authorization": f"Bearer {self._api_key}",
                })
                latency = (time.perf_counter() - t0) * 1000
                content = resp["choices"][0]["message"]["content"]
                usage = resp.get("usage", {})
                return LLMResponse(
                    content=content,
                    provider=self.name,
                    model=resp.get("model", self.model),
                    input_tokens=usage.get("prompt_tokens", 0),
                    output_tokens=usage.get("completion_tokens", 0),
                    latency_ms=latency,
                    raw=resp,
                )
            except RuntimeError as e:
                last_error = e
                continue
        raise RuntimeError(f"[minimax] All endpoints failed. Last: {last_error}")


# ---------------------------------------------------------------------------
# Built-in fallback (нет ни одного провайдера)
# ---------------------------------------------------------------------------

class BuiltinProvider(LLMProvider):
    """
    Встроенный провайдер без внешних API.
    Используется когда ничего не настроено.
    Возвращает структурированные заглушки для тестирования.
    """

    name = "builtin"

    def _load_api_key(self, config: dict) -> Optional[str]:
        return "builtin"

    def _default_model(self) -> str:
        return "builtin-v1"

    def complete(self, messages: list[dict], **kwargs) -> LLMResponse:
        prompt = messages[-1]["content"] if messages else ""
        content = self._generate_response(prompt)
        return LLMResponse(
            content=content,
            provider=self.name,
            model=self.model,
        )

    def _generate_response(self, prompt: str) -> str:
        """Генерирует структурированный ответ для распространённых запросов."""
        p = prompt.lower()
        if "decompose" in p or "разбей" in p or "подзадач" in p:
            return json.dumps({
                "tasks": [
                    {"step": 1, "title": "Research", "agent": "OpenClaw", "description": f"Исследовать: {prompt[:60]}"},
                    {"step": 2, "title": "Analysis", "agent": "Hermes", "description": "Анализ собранных данных"},
                    {"step": 3, "title": "Report", "agent": "Hermes", "description": "Сформировать итоговый отчёт"},
                ],
                "note": "Generated by builtin provider — set up a real LLM for intelligent decomposition"
            }, ensure_ascii=False, indent=2)
        return (
            f"[builtin] Обработан запрос: {prompt[:120]}...\n\n"
            "Для получения качественных результатов настройте LLM-провайдер "
            "в orchestrator/state/config.json → секция 'llm'."
        )

    def is_available(self) -> bool:
        return True


# ---------------------------------------------------------------------------
# Провайдер с fallback-цепочкой
# ---------------------------------------------------------------------------

class FallbackProvider(LLMProvider):
    """
    Оборачивает несколько провайдеров: пробует по порядку,
    переключается при ошибке.
    """

    name = "fallback"

    def __init__(self, providers: list[LLMProvider]) -> None:
        # Must call super().__init__ so .model / .temperature / .max_tokens etc.
        # are defined — FallbackProvider.complete() delegates to sub-providers
        # but callers may still inspect these attributes via repr() or logging.
        super().__init__({})
        self._providers = [p for p in providers if p.is_available()]
        if not self._providers:
            self._providers = [BuiltinProvider({})]

    def _load_api_key(self, config: dict) -> Optional[str]:
        return "fallback"

    def complete(self, messages: list[dict], **kwargs) -> LLMResponse:
        last_error: Exception | None = None
        for provider in self._providers:
            try:
                resp = provider.complete(messages, **kwargs)
                if len(self._providers) > 1:
                    resp.content = f"[via {provider.name}] " + resp.content if False else resp.content
                return resp
            except Exception as e:
                print(f"[fallback] Provider {provider.name} failed: {e}")
                last_error = e
                continue
        raise RuntimeError(f"[fallback] All providers failed. Last: {last_error}")

    def is_available(self) -> bool:
        return bool(self._providers)

    def __repr__(self) -> str:
        names = [p.name for p in self._providers]
        return f"<FallbackProvider chain={names!r}>"


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

_REGISTRY: dict[str, type[LLMProvider]] = {
    "anthropic": AnthropicProvider,
    "openai": OpenAIProvider,
    "ollama": OllamaProvider,
    "minimax": MiniMaxProvider,
    "builtin": BuiltinProvider,
}


def get_provider(config: dict | None = None) -> LLMProvider:
    """
    Создать провайдера из конфигурации.

    config = {
        "provider": "anthropic",          # или список для fallback
        "model": "claude-haiku-4-5-20251001",
        "api_key_env": "ANTHROPIC_API_KEY",
        "fallback_chain": ["anthropic", "ollama", "builtin"]
    }
    """
    if config is None:
        config = _load_config()

    llm_cfg = config.get("llm", config)  # поддержка плоского и вложенного формата
    provider_name = llm_cfg.get("provider", "builtin")
    fallback_chain = llm_cfg.get("fallback_chain", [])

    if fallback_chain:
        providers = []
        for name in fallback_chain:
            cls = _REGISTRY.get(name)
            if cls:
                cfg = {**llm_cfg, "provider": name}
                providers.append(cls(cfg))
        return FallbackProvider(providers)

    cls = _REGISTRY.get(provider_name, BuiltinProvider)
    return cls(llm_cfg)


def list_providers() -> list[str]:
    """Список поддерживаемых провайдеров."""
    return list(_REGISTRY.keys())


def _load_config() -> dict:
    """Загрузить config.json если он существует."""
    paths = [
        "orchestrator/state/config.json",
        "state/config.json",
        "config.json",
    ]
    for p in paths:
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            continue
    return {}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    provider = get_provider()
    print(f"Active provider: {provider}")
    print(f"Available: {provider.is_available()}")

    if len(sys.argv) > 1:
        prompt = " ".join(sys.argv[1:])
        resp = provider.ask(prompt)
        print(f"\n--- Response ({resp.provider}/{resp.model}, {resp.latency_ms:.0f}ms) ---")
        print(resp.content)
    else:
        print("\nUsage: python provider.py 'Your prompt here'")
        print(f"Supported providers: {list_providers()}")
