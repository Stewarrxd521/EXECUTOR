"""Configuración del executor a partir de variables de entorno.

Se conservan todos los nombres de variables del executor anterior para que un
despliegue existente siga funcionando sin tocar su configuración.
"""

from __future__ import annotations

import hmac
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger("executor.config")

PACKAGE_DIR = Path(__file__).resolve().parent
ROOT_DIR = PACKAGE_DIR.parent
BUNDLED_EXCHANGE_INFO = ROOT_DIR / "exchangeInfo.txt"
# Secreto por defecto = el que usan los bridges ya desplegados (app_25.py y
# executor_bridge_*.py: EXECUTOR_SECRET / signal_secret="clave-secreta-aleatoria").
DEFAULT_SIGNAL_SECRET = "clave-secreta-aleatoria"
# Si SIGNAL_SECRET no está definido se aceptan ambos valores históricos por defecto.
LEGACY_SIGNAL_SECRETS = ("clave-secreta-aleatoria", "cambiar-por-secreto-seguro")


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in ("1", "true", "yes", "si", "sí", "on")


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.environ.get(name, default)))
    except (TypeError, ValueError):
        log.warning("Variable %s inválida; se usa %s", name, default)
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        log.warning("Variable %s inválida; se usa %s", name, default)
        return default


def _env_list(name: str, default: str = "") -> list[str]:
    raw = os.environ.get(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass
class Settings:
    # ── Credenciales y entorno ────────────────────────────────────────────
    api_key: str = ""
    api_secret: str = ""
    testnet: bool = False
    signal_secret: str = DEFAULT_SIGNAL_SECRET
    signal_secrets: list[str] = field(default_factory=list)
    signal_secret_is_default: bool = False
    dashboard_token: str = ""
    port: int = 10000

    # ── Endpoints ─────────────────────────────────────────────────────────
    ws_api_url: str = ""
    stream_base_url: str = ""
    rest_url: str = ""
    proxy_urls: list[str] = field(default_factory=list)
    rest_proxy_all: bool = False

    # ── Riesgo / tamaño ───────────────────────────────────────────────────
    leverage: int = 4
    leverage_env: int = 4
    leverage_required: bool = False
    high_price_threshold: float = 2.0
    high_price_leverage: int = 20
    hedge_mode_hint: bool = False
    min_notional_usdt: float = 5.1
    notional_buffer_pct: float = 2.0
    max_notional_overshoot_x: float = 4.0
    default_notional_usdt: float = 0.0
    blocked_symbols: set[str] = field(default_factory=set)
    assume_on_margin_error: bool = True
    margin_auto_reduce: bool = False
    max_price_age_s: float = 5.0
    signal_dedupe_ttl_s: float = 0.0
    signal_wait_timeout_s: float = 7.0

    # ── Infra ─────────────────────────────────────────────────────────────
    recv_window_ms: int = 5000
    balance_poll_s: int = 60
    position_poll_s: int = 30
    data_dir: Path = Path("data")
    exchange_info_file: Path = BUNDLED_EXCHANGE_INFO
    exchange_info_refresh_h: float = 0.0
    exchange_info_bootstrap: bool = True
    seed_open_orders_rest: bool = True
    state_requires_auth: bool = False
    api_write_open: bool = False
    cors_origins: str = "*"
    trust_proxy: bool = False
    adopt_positions: bool = True
    restore_trading_pause: bool = True
    keepalive_url: str = ""
    keepalive_s: int = 0
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    log_level: str = "INFO"

    # ── Grid bots ─────────────────────────────────────────────────────────
    grid_max_levels: int = 150
    grid_order_concurrency: int = 4

    def check_signal_secret(self, value: str) -> bool:
        """True si ``value`` coincide con alguno de los secretos aceptados."""
        if not value:
            return False
        accepted = self.signal_secrets or [self.signal_secret]
        return any(s and hmac.compare_digest(value.encode(), s.encode()) for s in accepted)

    def check_write_token(self, value: str) -> bool:
        """Credencial de la API HTTP/dashboard: DASHBOARD_TOKEN o SIGNAL_SECRET.

        Con DASHBOARD_TOKEN definido, los secretos por defecto (públicos) de los
        bots no sirven como token: hay que definir SIGNAL_SECRET o usar el token.
        """
        if not value:
            return False
        if self.dashboard_token:
            if hmac.compare_digest(value.encode(), self.dashboard_token.encode()):
                return True
            if self.signal_secret_is_default:
                return False
        return self.check_signal_secret(value)

    @property
    def dashboard_auth_required(self) -> bool:
        """El dashboard solo pide token si DASHBOARD_TOKEN está definido."""
        return bool(self.dashboard_token)

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret)

    @property
    def env_label(self) -> str:
        return "TESTNET" if self.testnet else "REAL"

    def public_view(self) -> dict:
        """Configuración visible en el dashboard (sin secretos)."""
        return {
            "env": self.env_label,
            "testnet": self.testnet,
            "leverage": self.leverage,
            "high_price_threshold": self.high_price_threshold,
            "high_price_leverage": self.high_price_leverage,
            "min_notional_usdt": self.min_notional_usdt,
            "notional_buffer_pct": self.notional_buffer_pct,
            "default_notional_usdt": self.default_notional_usdt,
            "blocked_symbols": sorted(self.blocked_symbols),
            "proxies": len(self.proxy_urls),
            "proxy_configured": bool(self.proxy_urls),
            "signal_secret_default": self.signal_secret_is_default,
            "signal_dedupe_ttl_s": self.signal_dedupe_ttl_s,
            "http_write_auth": not self.api_write_open,
            "cors_origins": self.cors_origins,
            "leverage_required": self.leverage_required,
            "adopt_positions": self.adopt_positions,
            "keepalive": bool(self.keepalive_url and self.keepalive_s),
            "assume_on_margin_error": self.assume_on_margin_error,
            "margin_auto_reduce": self.margin_auto_reduce,
            "telegram": bool(self.telegram_bot_token and self.telegram_chat_id),
        }


def load_settings() -> Settings:
    testnet = _env_bool("USE_TESTNET", False)

    proxies = _env_list("PROXY_URLS")
    if not proxies and _env("FIXIE_URL"):
        proxies = [_env("FIXIE_URL")]

    # SIGNAL_SECRET se usa tal cual (como antes, puede contener comas);
    # SIGNAL_SECRETS añade secretos extra separados por comas.
    raw = os.environ.get("SIGNAL_SECRET", "")
    raw_secrets = [v for v in dict.fromkeys([raw, raw.strip()]) if v.strip()] + _env_list("SIGNAL_SECRETS")
    if raw_secrets:
        signal_secrets, secret_is_default = list(dict.fromkeys(raw_secrets)), False
    else:
        signal_secrets, secret_is_default = list(LEGACY_SIGNAL_SECRETS), True
    signal_secret = signal_secrets[0]
    leverage = max(1, min(125, _env_int("LEVERAGE", _env_int("DEFAULT_LEVERAGE", 4))))
    on_render = bool(os.environ.get("RENDER"))
    keepalive_url = _env("KEEPALIVE_URL", os.environ.get("RENDER_EXTERNAL_URL", "")).rstrip("/")

    settings = Settings(
        api_key=_env("BINANCE_API_KEY"),
        api_secret=_env("BINANCE_API_SECRET"),
        testnet=testnet,
        signal_secret=signal_secret,
        signal_secrets=signal_secrets,
        signal_secret_is_default=secret_is_default,
        dashboard_token=_env("DASHBOARD_TOKEN"),
        port=_env_int("PORT", 10000),
        ws_api_url=_env(
            "BINANCE_WS_FAPI_URL",
            "wss://testnet.binancefuture.com/ws-fapi/v1" if testnet else "wss://ws-fapi.binance.com/ws-fapi/v1",
        ),
        stream_base_url=_env(
            "BINANCE_STREAM_URL",
            "wss://fstream.binancefuture.com" if testnet else "wss://fstream.binance.com",
        ).rstrip("/"),
        rest_url=_env(
            "BINANCE_REST_FAPI_URL",
            "https://testnet.binancefuture.com" if testnet else "https://fapi.binance.com",
        ).rstrip("/"),
        proxy_urls=proxies,
        rest_proxy_all=_env_bool("REST_PROXY_ALL", False),
        leverage=leverage,
        leverage_env=leverage,
        leverage_required=_env_bool("LEVERAGE_REQUIRED", False),
        high_price_threshold=_env_float("HIGH_PRICE_THRESHOLD", 2.0),
        high_price_leverage=max(1, min(125, _env_int("HIGH_PRICE_LEVERAGE", 20))),
        hedge_mode_hint=_env_bool("HEDGE_MODE", False),
        min_notional_usdt=_env_float("MIN_NOTIONAL_USDT", 5.1),
        notional_buffer_pct=_env_float("NOTIONAL_SAFETY_BUFFER_PCT", 2.0),
        max_notional_overshoot_x=_env_float("MAX_NOTIONAL_OVERSHOOT_X", 4.0),
        default_notional_usdt=_env_float("DEFAULT_NOTIONAL_USDT", 0.0),
        blocked_symbols={s.upper() for s in _env_list("BLOCKED_SYMBOLS", "BTCUSDT,ETHUSDT,BTCUSDC,ETHUSDC")},
        assume_on_margin_error=_env_bool("ASSUME_ON_MARGIN_ERROR", True),
        margin_auto_reduce=_env_bool("MARGIN_AUTO_REDUCE", False),
        max_price_age_s=_env_float("MAX_PRICE_AGE_S", 5.0),
        signal_dedupe_ttl_s=_env_float("SIGNAL_DEDUPE_TTL_S", 0.0),
        signal_wait_timeout_s=_env_float("SIGNAL_WAIT_TIMEOUT_S", 7.0),
        recv_window_ms=_env_int("RECV_WINDOW_MS", 5000),
        balance_poll_s=max(10, _env_int("BALANCE_POLL_S", 60)),
        position_poll_s=max(10, _env_int("POSITION_POLL_S", 30)),
        data_dir=Path(_env("DATA_DIR", "data")),
        exchange_info_file=Path(_env("EXCHANGE_INFO_FILE") or BUNDLED_EXCHANGE_INFO),
        exchange_info_refresh_h=_env_float("EXCHANGE_INFO_REFRESH_H", 0.0),
        exchange_info_bootstrap=_env_bool("EXCHANGE_INFO_BOOTSTRAP", True),
        seed_open_orders_rest=_env_bool("SEED_OPEN_ORDERS_REST", True),
        state_requires_auth=_env_bool("STATE_REQUIRES_AUTH", False),
        api_write_open=_env_bool("API_WRITE_OPEN", False),
        cors_origins=_env("CORS_ORIGINS", "*"),
        trust_proxy=_env_bool("TRUST_PROXY", on_render),
        adopt_positions=_env_bool("ADOPT_POSITIONS", True),
        restore_trading_pause=_env_bool("RESTORE_TRADING_PAUSE", True),
        keepalive_url=keepalive_url,
        keepalive_s=_env_int("KEEPALIVE_S", 600 if keepalive_url else 0),
        telegram_bot_token=_env("TELEGRAM_BOT_TOKEN"),
        telegram_chat_id=_env("TELEGRAM_CHAT_ID"),
        log_level=_env("LOG_LEVEL", "INFO").upper(),
        grid_max_levels=max(2, _env_int("GRID_MAX_LEVELS", 150)),
        grid_order_concurrency=max(1, _env_int("GRID_ORDER_CONCURRENCY", 4)),
    )
    return settings


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
