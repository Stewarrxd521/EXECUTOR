"""Contenedor de dependencias compartidas entre los módulos del executor."""

from __future__ import annotations

import secrets
import string
import time
from dataclasses import dataclass

from .account import AccountState
from .binance_api import BinanceWsApi, RestClient, ServerClock
from .config import Settings
from .errors import ErrorJournal
from .exchange_info import ExchangeInfo
from .notifier import EventBus, TelegramNotifier
from .streams import MarketData

_ALPHABET = string.ascii_letters + string.digits


def new_client_id(prefix: str) -> str:
    """clientOrderId único y válido para Binance (``^[.A-Z:/a-z0-9_-]{1,36}$``)."""
    stamp = format(int(time.time() * 1000) % 36**7, "x")
    rand = "".join(secrets.choice(_ALPHABET) for _ in range(6))
    return f"{prefix}-{stamp}{rand}"[:36]


def utc_now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())


@dataclass
class Core:
    settings: Settings
    clock: ServerClock
    ws: BinanceWsApi
    rest: RestClient
    account: AccountState
    market: MarketData
    exinfo: ExchangeInfo
    journal: ErrorJournal
    bus: EventBus
    telegram: TelegramNotifier
