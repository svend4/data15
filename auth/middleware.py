"""
auth/middleware.py
==================
JWT-аутентификация и RBAC для REST API Orchestrator v6.

Что изменилось vs v5 (нет аутентификации вообще):
  ❌ v5: HTTP эндпоинты открыты без проверки — любой может дать команды агентам
  ✅ v6: JWT токены + 4 роли (admin/operator/viewer/guest)
         Каждый эндпоинт требует минимальную роль

Реализация без сторонних библиотек (только stdlib):
  • HMAC-SHA256 подпись JWT
  • Stateless — сервер не хранит сессии
  • Bearer token в заголовке Authorization

Конфигурация в orchestrator/state/config.json:
  {
    "auth": {
      "enabled": true,
      "secret_key_env": "ORCHESTRATOR_JWT_SECRET",
      "token_ttl_hours": 24,
      "guest_read_only": true,
      "users": {
        "admin": {"password_hash": "<sha256>", "role": "admin"},
        "operator": {"password_hash": "<sha256>", "role": "operator"}
      }
    }
  }
"""

from __future__ import annotations

import base64
import functools
import hashlib
import hmac
import json
import os
import re
import time
import warnings
from dataclasses import dataclass, field
from enum import IntEnum
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any, Callable, Optional


# ---------------------------------------------------------------------------
# RBAC роли (IntEnum — можно сравнивать: admin > operator > viewer > guest)
# ---------------------------------------------------------------------------

class Role(IntEnum):
    GUEST    = 0
    VIEWER   = 1
    OPERATOR = 2
    ADMIN    = 3

    @classmethod
    def from_str(cls, s: str) -> "Role":
        mapping = {
            "admin":    cls.ADMIN,
            "operator": cls.OPERATOR,
            "viewer":   cls.VIEWER,
            "guest":    cls.GUEST,
        }
        return mapping.get(str(s).lower(), cls.GUEST)

    def to_str(self) -> str:
        return self.name.lower()


# ---------------------------------------------------------------------------
# JWT — только HMAC-SHA256, без сторонних библиотек
# ---------------------------------------------------------------------------

_JWT_ALG = "HS256"
_JWT_HEADER = base64.urlsafe_b64encode(
    json.dumps({"alg": _JWT_ALG, "typ": "JWT"}).encode()
).rstrip(b"=")


def _b64enc(data: bytes) -> bytes:
    return base64.urlsafe_b64encode(data).rstrip(b"=")


def _b64dec(data: str) -> bytes:
    # Восстановить padding
    padded = data + "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(padded)


def _sign(header_b64: bytes, payload_b64: bytes, secret: bytes) -> bytes:
    msg = header_b64 + b"." + payload_b64
    sig = hmac.new(secret, msg, hashlib.sha256).digest()
    return _b64enc(sig)


def create_token(
    sub: str,
    role: Role,
    secret: str,
    ttl_hours: float = 24,
    extra: Optional[dict] = None,
) -> str:
    """
    Создать JWT токен.

    Args:
        sub:       Идентификатор пользователя (username)
        role:      Роль пользователя
        secret:    Секретный ключ подписи
        ttl_hours: Срок действия в часах
        extra:     Дополнительные claims (произвольные поля)

    Returns:
        JWT строка вида header.payload.signature
    """
    now = int(time.time())
    payload = {
        "sub": sub,
        "role": role.to_str(),
        "iat": now,
        "exp": now + int(ttl_hours * 3600),
        "jti": _b64enc(os.urandom(12)).decode(),  # уникальный ID токена
    }
    if extra:
        payload.update(extra)

    payload_b64 = _b64enc(json.dumps(payload, separators=(",", ":")).encode())
    signature = _sign(_JWT_HEADER, payload_b64, secret.encode("utf-8"))
    return f"{_JWT_HEADER.decode()}.{payload_b64.decode()}.{signature.decode()}"


def verify_token(token: str, secret: str) -> dict:
    """
    Проверить JWT токен и вернуть payload.

    Raises:
        JWTError: если токен невалиден, истёк или подпись неверна
    """
    parts = token.split(".")
    if len(parts) != 3:
        raise JWTError("Invalid token format")

    header_b64, payload_b64, sig_b64 = parts

    # Проверить подпись
    expected_sig = _sign(
        header_b64.encode(), payload_b64.encode(), secret.encode("utf-8")
    )
    if not hmac.compare_digest(expected_sig, sig_b64.encode()):
        raise JWTError("Invalid signature")

    # Декодировать payload
    try:
        payload = json.loads(_b64dec(payload_b64))
    except Exception as e:
        raise JWTError(f"Payload decode error: {e}")

    # Проверить срок действия
    exp = payload.get("exp", 0)
    if time.time() > exp:
        raise JWTError(f"Token expired at {exp}")

    return payload


class JWTError(Exception):
    pass


# ---------------------------------------------------------------------------
# Пользователи и хэширование паролей
# ---------------------------------------------------------------------------

def hash_password(password: str) -> str:
    """
    PBKDF2-SHA256 хэш пароля с salt.

    Формат: pbkdf2:sha256:<iterations>:<salt_hex>:<hash_hex>

    Backward compat: если передан старый SHA-256 хэш (64 hex chars),
    check_password() всё ещё принимает его, но hash_password() всегда
    генерирует новый PBKDF2-формат.
    """
    salt = os.urandom(16)
    iterations = 260_000  # OWASP minimum 2024
    key = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2:sha256:{iterations}:{salt.hex()}:{key.hex()}"


def check_password(password: str, password_hash: str) -> bool:
    """
    Проверить пароль против сохранённого хэша.

    Поддерживает:
    • Новый формат pbkdf2:sha256:<iter>:<salt>:<hash>
    • Старый формат (plain SHA-256, 64 hex chars) — для backward compat
    """
    if password_hash.startswith("pbkdf2:"):
        parts = password_hash.split(":")
        if len(parts) != 5:
            return False
        _, algo, iterations_str, salt_hex, stored_hex = parts
        try:
            iterations = int(iterations_str)
            salt = bytes.fromhex(salt_hex)
            key = hashlib.pbkdf2_hmac(algo, password.encode("utf-8"), salt, iterations)
        except (ValueError, TypeError):
            return False
        return hmac.compare_digest(key.hex(), stored_hex)
    else:
        # Legacy SHA-256 (plain, no salt) — still accepted but deprecated
        legacy = hashlib.sha256(password.encode("utf-8")).hexdigest()
        return hmac.compare_digest(legacy, password_hash)


# ---------------------------------------------------------------------------
# JWTAuth — основной класс аутентификации
# ---------------------------------------------------------------------------

@dataclass
class TokenInfo:
    sub: str
    role: Role
    issued_at: int
    expires_at: int
    jti: str = ""
    raw: dict = field(default_factory=dict)

    @property
    def is_expired(self) -> bool:
        return time.time() > self.expires_at


class JWTAuth:
    """
    JWT аутентификация и RBAC.

    Использование с BaseHTTPRequestHandler:
        auth = JWTAuth.from_config()

        # В обработчике запроса:
        token_info = auth.authenticate(handler)   # или None если нет токена
        if not auth.authorize(token_info, Role.OPERATOR):
            handler.send_error(403, "Insufficient role")
            return

        # Создать токен (для /auth/login):
        token = auth.login("admin", "password")
    """

    def __init__(
        self,
        secret: str,
        token_ttl_hours: float = 24,
        enabled: bool = True,
        users: Optional[dict] = None,      # {username: {password_hash, role}}
        guest_read_only: bool = True,
    ) -> None:
        self._secret = secret
        self._ttl = token_ttl_hours
        self.enabled = enabled
        self._users: dict[str, dict] = users or {}
        self.guest_read_only = guest_read_only

    @classmethod
    def from_config(cls, config: Optional[dict] = None) -> "JWTAuth":
        """Создать JWTAuth из config.json → секция 'auth'."""
        if config is None:
            config = _load_config()

        auth_cfg = config.get("auth", {})
        enabled = auth_cfg.get("enabled", False)

        # Загрузить секрет из env или config
        secret_env = auth_cfg.get("secret_key_env", "ORCHESTRATOR_JWT_SECRET")
        secret = os.environ.get(secret_env, "") or auth_cfg.get("secret_key", "")

        if not secret and enabled:
            # Попробовать загрузить ранее сохранённый секрет из файла
            _secret_file = Path("orchestrator/state/.jwt_secret")
            if _secret_file.exists():
                try:
                    secret = _secret_file.read_text(encoding="utf-8").strip()
                    print(f"[JWTAuth] Loaded persisted JWT secret from {_secret_file}")
                except OSError:
                    pass

            if not secret:
                # Сгенерировать новый и сохранить
                secret = base64.urlsafe_b64encode(os.urandom(32)).decode()
                try:
                    _secret_file.parent.mkdir(parents=True, exist_ok=True)
                    _secret_file.write_text(secret, encoding="utf-8")
                    print(f"[JWTAuth] Generated and persisted JWT secret → {_secret_file}")
                    print(f"[JWTAuth] Tip: set env var {secret_env} to override.")
                except OSError:
                    print(
                        f"[JWTAuth] WARNING: Could not persist JWT secret "
                        f"— tokens will be invalidated on restart. "
                        f"Set {secret_env} env var for production use."
                    )

        if enabled and auth_cfg.get("users"):
            # Warn if any user still has a legacy SHA-256 hash (no salt)
            for uname, udata in auth_cfg.get("users", {}).items():
                ph = udata.get("password_hash", "")
                if ph and not ph.startswith("pbkdf2:") and len(ph) == 64:
                    warnings.warn(
                        f"[JWTAuth] User '{uname}' has a legacy unsalted SHA-256 password hash. "
                        f"Re-hash with: python -m auth.middleware hash <password>",
                        UserWarning,
                        stacklevel=2,
                    )

        return cls(
            secret=secret or "dev-secret-change-me",
            token_ttl_hours=auth_cfg.get("token_ttl_hours", 24),
            enabled=enabled,
            users=auth_cfg.get("users", {}),
            guest_read_only=auth_cfg.get("guest_read_only", True),
        )

    def login(self, username: str, password: str) -> str:
        """
        Аутентифицировать пользователя и выдать JWT.

        Raises:
            AuthError: если имя/пароль неверны
        """
        user = self._users.get(username)
        if not user:
            raise AuthError("Invalid credentials")
        if not check_password(password, user.get("password_hash", "")):
            raise AuthError("Invalid credentials")

        role = Role.from_str(user.get("role", "viewer"))
        return create_token(username, role, self._secret, self._ttl)

    def authenticate(self, handler: BaseHTTPRequestHandler) -> Optional[TokenInfo]:
        """
        Извлечь и проверить JWT из заголовка Authorization.

        Returns:
            TokenInfo если токен валиден, None если заголовка нет.

        Raises:
            JWTError: если токен есть но невалиден
        """
        if not self.enabled:
            return TokenInfo(sub="system", role=Role.ADMIN, issued_at=0, expires_at=9999999999)

        auth_header = handler.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return None

        token = auth_header[7:]
        payload = verify_token(token, self._secret)
        return TokenInfo(
            sub=payload["sub"],
            role=Role.from_str(payload.get("role", "guest")),
            issued_at=payload.get("iat", 0),
            expires_at=payload.get("exp", 0),
            jti=payload.get("jti", ""),
            raw=payload,
        )

    def authorize(
        self,
        token_info: Optional[TokenInfo],
        required_role: Role,
    ) -> bool:
        """
        Проверить что пользователь имеет нужную роль.

        - Если аутентификация отключена — всегда True
        - Если токена нет и требуется VIEWER+ — False
        - Если токена нет и требуется только GUEST — True (guest_read_only)
        """
        if not self.enabled:
            return True

        if token_info is None:
            if required_role <= Role.GUEST and self.guest_read_only:
                return True
            return False

        return token_info.role >= required_role

    def add_user(self, username: str, password: str, role: Role) -> None:
        """Добавить пользователя в локальный реестр."""
        self._users[username] = {
            "password_hash": hash_password(password),
            "role": role.to_str(),
        }

    def create_token_for(self, username: str, role: Role) -> str:
        """Создать токен напрямую (для тестов и CLI)."""
        return create_token(username, role, self._secret, self._ttl)

    # ------------------------------------------------------------------
    # HTTP helpers для BaseHTTPRequestHandler
    # ------------------------------------------------------------------

    def send_401(self, handler: BaseHTTPRequestHandler, msg: str = "Unauthorized") -> None:
        body = json.dumps({"error": msg}).encode("utf-8")
        handler.send_response(401)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(body)))
        handler.send_header("WWW-Authenticate", 'Bearer realm="orchestrator"')
        handler.end_headers()
        handler.wfile.write(body)

    def send_403(self, handler: BaseHTTPRequestHandler, required: Role) -> None:
        body = json.dumps({
            "error": "Forbidden",
            "required_role": required.to_str(),
        }).encode("utf-8")
        handler.send_response(403)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)


class AuthError(Exception):
    pass


# ---------------------------------------------------------------------------
# require_role — декоратор для метода do_GET/do_POST обработчика
# ---------------------------------------------------------------------------

def require_role(role: Role, auth_attr: str = "auth"):
    """
    Декоратор для методов BaseHTTPRequestHandler.

    class MyHandler(BaseHTTPRequestHandler):
        auth = JWTAuth.from_config()

        @require_role(Role.OPERATOR)
        def do_POST_task(self):
            ...

    Если роли недостаточно — отправляет 401/403 и возвращает False из декоратора.
    """
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(self, *args, **kwargs):
            auth: JWTAuth = getattr(self, auth_attr)
            try:
                token_info = auth.authenticate(self)
            except JWTError as e:
                auth.send_401(self, str(e))
                return
            if not auth.authorize(token_info, role):
                if token_info is None:
                    auth.send_401(self, "Authentication required")
                else:
                    auth.send_403(self, role)
                return
            return fn(self, *args, **kwargs)
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Вспомогательная HTTP-middleware функция (альтернатива декоратору)
# ---------------------------------------------------------------------------

def guard(
    handler: BaseHTTPRequestHandler,
    auth: JWTAuth,
    required_role: Role,
) -> Optional[TokenInfo]:
    """
    Проверить аутентификацию в теле обработчика.

    Returns TokenInfo если OK, None и уже отправлен ответ 401/403 если нет.

    Использование:
        def do_POST(self):
            token = guard(self, self.auth, Role.OPERATOR)
            if token is None:
                return
            # Proceed with authenticated logic...
    """
    try:
        token_info = auth.authenticate(handler)
    except JWTError as e:
        auth.send_401(handler, str(e))
        return None
    if not auth.authorize(token_info, required_role):
        if token_info is None:
            auth.send_401(handler, "Authentication required")
        else:
            auth.send_403(handler, required_role)
        return None
    return token_info


# ---------------------------------------------------------------------------
# Конфигурация
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    paths = ["orchestrator/state/config.json", "state/config.json", "config.json"]
    for p in paths:
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            continue
    return {}


# ---------------------------------------------------------------------------
# CLI — управление пользователями и выпуск токенов
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    def _help():
        print("""
auth/middleware.py CLI

  python -m auth.middleware token <username> <role>     Создать JWT токен
  python -m auth.middleware verify <token>              Проверить токен
  python -m auth.middleware hash <password>             SHA-256 хэш пароля
  python -m auth.middleware user <username> <password> <role>  Добавить пользователя в config
""".strip())

    if len(sys.argv) < 2:
        _help()
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "hash" and len(sys.argv) == 3:
        print(hash_password(sys.argv[2]))

    elif cmd == "token" and len(sys.argv) >= 3:
        username = sys.argv[2]
        role_str = sys.argv[3] if len(sys.argv) > 3 else "viewer"
        role = Role.from_str(role_str)
        auth = JWTAuth.from_config()
        token = auth.create_token_for(username, role)
        print(f"Token ({role.to_str()}):")
        print(token)

    elif cmd == "verify" and len(sys.argv) == 3:
        auth = JWTAuth.from_config()
        try:
            info = verify_token(sys.argv[2], auth._secret)
            print(f"✅ Valid token:")
            print(json.dumps(info, indent=2))
        except JWTError as e:
            print(f"❌ Invalid: {e}")

    elif cmd == "user" and len(sys.argv) == 5:
        username, password, role_str = sys.argv[2], sys.argv[3], sys.argv[4]
        h = hash_password(password)
        print(f'Add to config.json → auth → users:')
        print(json.dumps({username: {"password_hash": h, "role": role_str}}, indent=2))

    else:
        _help()
