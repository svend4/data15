"""
utils/time.py
=============
Единый формат UTC-временны́х меток для всего Orchestrator v6.

Проблема до этого файла:
  • sqlite_board.py   использовал datetime.now(timezone.utc).isoformat()
  • vector_store.py   использовал time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
  → Разные форматы в одной БД → сортировка/сравнение неверны

Решение: один вызов utcnow_iso() во всём проекте.

Формат: ISO 8601 с микросекундами и явным UTC-суффиксом
  2025-04-15T14:32:01.123456+00:00

Использование:
    from utils.time import utcnow_iso, parse_iso, elapsed_seconds

    created_at = utcnow_iso()          # новая метка
    ts = parse_iso(created_at)         # → datetime объект
    elapsed = elapsed_seconds(created_at)  # → float секунд с момента метки
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional


def utcnow_iso() -> str:
    """
    Текущее UTC-время в ISO 8601 с суффиксом +00:00.

    Пример: '2025-04-15T14:32:01.123456+00:00'

    Returns:
        str: ISO-строка, пригодная для сортировки и сравнения в SQLite
    """
    return datetime.now(timezone.utc).isoformat()


def parse_iso(iso_str: Optional[str]) -> Optional[datetime]:
    """
    Разобрать ISO-строку → datetime.

    Поддерживает оба формата:
      • '2025-04-15T14:32:01.123456+00:00'  (новый, с sufix)
      • '2025-04-15T14:32:01Z'               (старый, из vector_store.py)

    Args:
        iso_str: ISO-строка или None

    Returns:
        datetime с timezone=UTC, или None если строка пустая/None
    """
    if not iso_str:
        return None
    s = iso_str.strip()
    # Нормализовать старый формат с 'Z'
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        # Последняя попытка — без timezone
        try:
            dt = datetime.fromisoformat(s)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None


def elapsed_seconds(iso_str: Optional[str]) -> float:
    """
    Секунд прошло с момента iso_str до сейчас.

    Args:
        iso_str: ISO-строка, созданная utcnow_iso()

    Returns:
        float: секунды (>0 если в прошлом). 0.0 если строка невалидна.
    """
    dt = parse_iso(iso_str)
    if dt is None:
        return 0.0
    now = datetime.now(timezone.utc)
    return max(0.0, (now - dt).total_seconds())


def format_duration(seconds: float) -> str:
    """
    Форматировать длительность для отображения.

    Args:
        seconds: длительность в секундах

    Returns:
        str: '42s', '3m 12s', '1h 5m'
    """
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        m, s = divmod(int(seconds), 60)
        return f"{m}m {s:02d}s"
    else:
        h, rem = divmod(int(seconds), 3600)
        m = rem // 60
        return f"{h}h {m:02d}m"


def is_expired(iso_str: Optional[str], ttl_seconds: float) -> bool:
    """
    Проверить, истёк ли TTL.

    Args:
        iso_str: ISO-строка времени создания
        ttl_seconds: срок жизни в секундах

    Returns:
        bool: True если (now - iso_str) > ttl_seconds
    """
    return elapsed_seconds(iso_str) > ttl_seconds
