"""LLM provider abstraction — единый интерфейс для всех AI-провайдеров."""
from .provider import LLMProvider, get_provider, list_providers

__all__ = ["LLMProvider", "get_provider", "list_providers"]
