"""Utilidades compartidas por el servidor HTTP: JSON, autenticación y argumentos."""

from __future__ import annotations

import json
import logging
import time
from collections import OrderedDict
from typing import Any, Optional

from aiohttp import web

from ..errors import BinanceAPIError, ErrorDoctor
from ..grid import GridError
from ..service import COMMANDS, CommandError, ExecutorService

log = logging.getLogger("executor.web")

try:  # orjson es opcional: acelera la serialización si está instalado
    import orjson

    def dumps(data) -> str:
        return orjson.dumps(data, option=orjson.OPT_NON_STR_KEYS, default=str).decode()
except ImportError:  # pragma: no cover
    def dumps(data) -> str:
        return json.dumps(data, separators=(",", ":"), default=str)


def json_response(body: Any, status: int = 200, headers: Optional[dict] = None) -> web.Response:
    return web.Response(text=dumps(body), status=status, content_type="application/json",
                        headers={"Cache-Control": "no-store", **(headers or {})})


def client_ip(request: web.Request, trust_proxy: bool) -> str:
    """IP del cliente para logs (detrás de Render: X-Forwarded-For).

    Solo informativa: ningún control de seguridad depende de ella.
    """
    if trust_proxy:
        fwd = request.headers.get("X-Forwarded-For", "")
        if fwd:
            return fwd.split(",")[0].strip()[:64]
    return (request.remote or "?")[:64]


_SECRET_KEYS = ("secret", "token", "signal_secret")


def request_secrets(request: web.Request, body: Optional[dict] = None) -> list[str]:
    """Todas las credenciales enviadas (headers, Bearer, query string y cuerpo)."""
    found: list[str] = []
    for header in ("X-Signal-Secret", "X-Dashboard-Token"):
        found.append(request.headers.get(header, ""))
    auth = request.headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        found.append(auth[7:].strip())
    for key in _SECRET_KEYS:
        found.append(request.query.get(key, ""))
    if isinstance(body, dict):
        for key in _SECRET_KEYS:
            if isinstance(body.get(key), (str, int)):
                found.append(str(body[key]))
    return [v for v in dict.fromkeys(found) if v][:8]


def request_secret(request: web.Request, body: Optional[dict] = None) -> str:
    """Primera credencial enviada (compatibilidad)."""
    values = request_secrets(request, body)
    return values[0] if values else ""


def any_valid(values: list[str], check) -> bool:
    return any(check(v) for v in values)


async def read_body(request: web.Request) -> tuple[Any, bool]:
    """Cuerpo como dict (JSON o formulario). Devuelve (datos, json_inválido)."""
    if request.method in ("GET", "HEAD", "DELETE") and not request.can_read_body:
        return {}, False
    if not request.can_read_body:
        return {}, False
    ctype = request.content_type or ""
    if ctype in ("application/x-www-form-urlencoded", "multipart/form-data"):
        try:
            form = await request.post()
        except (ValueError, AssertionError, UnicodeDecodeError, KeyError):
            return None, True
        return {k: v for k, v in form.items() if isinstance(v, str)}, False
    raw = await request.read()
    if not raw.strip():
        return {}, False
    try:
        return json.loads(raw.decode("utf-8")), False
    except (ValueError, UnicodeDecodeError):
        return None, True


async def read_args(request: web.Request, defaults: Optional[dict] = None) -> tuple[Optional[dict], list[str], str]:
    """Argumentos de un comando: query + cuerpo (JSON/form) + parámetros de la ruta.

    Devuelve (args | None si el cuerpo es inválido, credenciales enviadas, error).
    """
    body, invalid = await read_body(request)
    if invalid:
        return None, [], "invalid json"
    if body is not None and not isinstance(body, dict):
        return None, [], "el cuerpo debe ser un objeto JSON"
    secret = request_secrets(request, body)
    args: dict = dict(defaults or {})
    args.update({k: v for k, v in request.query.items()})
    args.update(body or {})
    args.update(request.match_info)
    if isinstance(args.get("args"), dict):  # forma {"cmd":..,"args":{...}} aceptada también aquí
        args.update(args.pop("args"))
    for key in _SECRET_KEYS:
        args.pop(key, None)
    if "symbol" in args:
        args["symbol"] = str(args["symbol"]).upper()
    return args, secret, ""


class Throttle:
    """Limita avisos repetidos por clave, con memoria acotada."""

    def __init__(self, interval: float, maxlen: int = 512):
        self.interval = interval
        self.maxlen = maxlen
        self._seen: OrderedDict = OrderedDict()

    def last(self, key: str) -> float:
        return self._seen.get(key, 0.0)

    def hit(self, key: str) -> bool:
        """True si la clave no se vio en los últimos ``interval`` segundos (y la registra)."""
        now = time.time()
        if now - self._seen.get(key, 0.0) <= self.interval:
            return False
        self._seen[key] = now
        self._seen.move_to_end(key)
        while len(self._seen) > self.maxlen:
            self._seen.popitem(last=False)
        return True


class Auth:
    """Reglas de acceso de la API HTTP."""

    def __init__(self, service: ExecutorService):
        self.service = service
        self.settings = service.settings
        self._warned = Throttle(60)

    def can_read(self, request: web.Request, secrets: list[str]) -> bool:
        if not self.settings.dashboard_auth_required:
            return True
        return any_valid(secrets, self.settings.check_write_token)

    def can_write(self, request: web.Request, secrets: list[str]) -> bool:
        if self.settings.api_write_open:
            # Escritura sin secreto: solo desde el mismo sitio o sin Origin (bots, scripts),
            # para que una página web ajena no pueda operar (CSRF).
            return origin_allowed(request, self.settings.cors_origins, strict=True)
        return any_valid(secrets, self.settings.check_write_token)

    def can(self, request: web.Request, secrets: list[str], write: bool) -> bool:
        return self.can_write(request, secrets) if write else self.can_read(request, secrets)

    def warn_denied(self, request: web.Request, what: str) -> None:
        ip = client_ip(request, self.settings.trust_proxy)
        route = request.match_info.route.resource.canonical if request.match_info.route.resource else "?"
        if self._warned.hit(f"{ip}|{request.method}|{route}"):
            log.warning("%s %s rechazado desde %s: falta el secreto (X-Signal-Secret o X-Dashboard-Token)",
                        request.method, str(what)[:80], ip)

    @staticmethod
    def denied(write: bool) -> web.Response:
        hint = ("envía X-Signal-Secret (o X-Dashboard-Token / Authorization: Bearer / ?secret=) con SIGNAL_SECRET"
                if write else "envía X-Dashboard-Token o X-Signal-Secret")
        return json_response({"ok": False, "error": "unauthorized", "detail": hint}, 401)


def origin_allowed(request: web.Request, cors_origins: str, strict: bool = False) -> bool:
    """¿El Origin es el mismo sitio o está permitido por CORS_ORIGINS?

    Sin Origin (bots, scripts, curl) siempre se permite. ``strict`` ignora el
    comodín ``*`` (solo mismo sitio u orígenes listados explícitamente).
    """
    origin = request.headers.get("Origin", "")
    if not origin:
        return True
    host = origin.split("://", 1)[-1].rstrip("/")
    own = {request.host, request.headers.get("X-Forwarded-Host", "")}
    if host in own:
        return True
    allowed = [o.strip().rstrip("/") for o in cors_origins.split(",") if o.strip()]
    if "*" in allowed:
        return not strict
    return origin.rstrip("/") in allowed


async def run_command(service: ExecutorService, cmd: str, args: dict) -> tuple[int, dict]:
    """Ejecuta un comando y devuelve (código HTTP, cuerpo)."""
    try:
        result = await service.execute(cmd, args)
        return 200, {"ok": True, "data": result}
    except BinanceAPIError as err:
        diag = ErrorDoctor.diagnose(err)
        return 502, {"ok": False, "error": f"[{diag.code}] {diag.info.title}", "diagnosis": diag.to_dict()}
    except (CommandError, GridError) as exc:
        return getattr(exc, "status", 400), {"ok": False, "error": str(exc)}
    except (ValueError, KeyError, TypeError) as exc:
        return 400, {"ok": False, "error": str(exc) or exc.__class__.__name__}
    except Exception as exc:  # pragma: no cover - errores inesperados
        log.exception("Comando %s falló", cmd)
        diag = ErrorDoctor.diagnose(exc)
        return 500, {"ok": False, "error": str(exc), "diagnosis": diag.to_dict()}


def is_write(cmd: str) -> bool:
    return bool(COMMANDS.get(cmd, {}).get("write", True))
