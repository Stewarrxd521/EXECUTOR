"""Configuración del executor a partir de variables de entorno.

Se conservan todos los nombres de variables del executor anterior para que un
despliegue existente siga funcionando sin tocar su configuración.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger("executor.config")

PACKAGE_DIR = Path(__file__).resolve().parent
BUNDLED_EXCHANGE_INFO = PACKAGE_DIR / "data" / "exchange_info.json"
DEFAULT_SIGNAL_SECRET = "cambiar-por-secreto-seguro"


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
    signal_dedupe_ttl_s: float = 10.0

    # ── Infra ─────────────────────────────────────────────────────────────
    recv_window_ms: int = 5000
    balance_poll_s: int = 60
    position_poll_s: int = 30
    data_dir: Path = Path("data")
    exchange_info_refresh_h: float = 0.0
    exchange_info_bootstrap: bool = True
    seed_open_orders_rest: bool = True
    state_requires_auth: bool = False
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    log_level: str = "INFO"

    # ── Grid bots ─────────────────────────────────────────────────────────
    grid_max_levels: int = 150
    grid_order_concurrency: int = 4

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
            "assume_on_margin_error": self.assume_on_margin_error,
            "margin_auto_reduce": self.margin_auto_reduce,
            "telegram": bool(self.telegram_bot_token and self.telegram_chat_id),
        }


def load_settings() -> Settings:
    testnet = _env_bool("USE_TESTNET", False)

    proxies = _env_list("PROXY_URLS")
    if not proxies and _env("FIXIE_URL"):
        proxies = [_env("FIXIE_URL")]

    signal_secret = _env("SIGNAL_SECRET", DEFAULT_SIGNAL_SECRET) or DEFAULT_SIGNAL_SECRET
    leverage = _env_int("LEVERAGE", _env_int("DEFAULT_LEVERAGE", 4))

    settings = Settings(
        api_key=_env("BINANCE_API_KEY"),
        api_secret=_env("BINANCE_API_SECRET"),
        testnet=testnet,
        signal_secret=signal_secret,
        dashboard_token=_env("DASHBOARD_TOKEN") or signal_secret,
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
        leverage=max(1, min(125, leverage)),
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
        signal_dedupe_ttl_s=_env_float("SIGNAL_DEDUPE_TTL_S", 10.0),
        recv_window_ms=_env_int("RECV_WINDOW_MS", 5000),
        balance_poll_s=max(10, _env_int("BALANCE_POLL_S", 60)),
        position_poll_s=max(10, _env_int("POSITION_POLL_S", 30)),
        data_dir=Path(_env("DATA_DIR", "data")),
        exchange_info_refresh_h=_env_float("EXCHANGE_INFO_REFRESH_H", 0.0),
        exchange_info_bootstrap=_env_bool("EXCHANGE_INFO_BOOTSTRAP", True),
        seed_open_orders_rest=_env_bool("SEED_OPEN_ORDERS_REST", True),
        state_requires_auth=_env_bool("STATE_REQUIRES_AUTH", False),
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
