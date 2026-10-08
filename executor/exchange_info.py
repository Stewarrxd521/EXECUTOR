"""Reglas de trading por símbolo desde un exchangeInfo local predefinido.

El executor NO consulta ``/fapi/v1/exchangeInfo`` al operar. Usa, en orden:

1. ``DATA_DIR/exchange_info.json`` — snapshot local (el más reciente).
2. ``executor/data/exchange_info.json`` — snapshot empaquetado con el código.
3. Reglas heurísticas derivadas del precio, para símbolos recién listados que
   todavía no están en el snapshot.

Sobre eso aplica correcciones *aprendidas* (``exchange_info_learned.json``):
cuando Binance rechaza una orden por precisión (-1111/-4014) en un símbolo sin
reglas exactas, el executor engrosa el paso y lo recuerda.

El stream ``!contractInfo`` (WebSocket) mantiene al día el estado de cada
contrato (TRADING, SETTLING, CLOSE...) y el leverage máximo de sus brackets.
"""

from __future__ import annotations

import json
import logging
import math
import os
import tempfile
import time
from dataclasses import dataclass, fields, replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

from .precision import D, ceil_step, coarser_step, decimals_of, floor_step, fmt, round_step

log = logging.getLogger("executor.exchange_info")

SNAPSHOT_VERSION = 1

# Tabla del executor anterior: stepSize de cantidad según el precio.
PRICE_STEP_TIERS: list[tuple[float, str]] = [
    (2.0, "0.1"),
    (100.0, "0.01"),
    (1000.0, "0.001"),
    (10000.0, "0.0001"),
    (100000.0, "0.00001"),
]

_DECIMAL_FIELDS = {
    "tick_size", "min_price", "max_price", "step_size", "min_qty", "max_qty",
    "market_step_size", "market_min_qty", "market_max_qty", "min_notional",
    "multiplier_up", "multiplier_down",
}


@dataclass
class SymbolRules:
    symbol: str
    status: str = "TRADING"
    base_asset: str = ""
    quote_asset: str = "USDT"
    contract_type: str = "PERPETUAL"
    onboard_date: int = 0
    tick_size: Decimal = Decimal("0.0001")
    min_price: Decimal = Decimal("0")
    max_price: Decimal = Decimal("0")
    step_size: Decimal = Decimal("1")
    min_qty: Decimal = Decimal("1")
    max_qty: Decimal = Decimal("0")
    market_step_size: Decimal = Decimal("1")
    market_min_qty: Decimal = Decimal("1")
    market_max_qty: Decimal = Decimal("0")
    min_notional: Decimal = Decimal("5")
    multiplier_up: Optional[Decimal] = None
    multiplier_down: Optional[Decimal] = None
    max_num_orders: int = 200
    max_num_algo_orders: int = 10
    price_precision: int = 4
    quantity_precision: int = 0
    max_leverage: int = 0
    source: str = "snapshot"

    # ── Redondeo ──────────────────────────────────────────────────────────
    def round_price(self, price, mode: str = "nearest") -> Decimal:
        fn = {"down": floor_step, "up": ceil_step}.get(mode, round_step)
        out = fn(price, self.tick_size)
        if self.min_price > 0 and out < self.min_price:
            out = self.min_price
        if self.max_price > 0 and out > self.max_price:
            out = self.max_price
        return out

    def qty_step(self, market: bool) -> Decimal:
        step = self.market_step_size if market else self.step_size
        return step if step > 0 else self.step_size

    def qty_min(self, market: bool) -> Decimal:
        return self.market_min_qty if market and self.market_min_qty > 0 else self.min_qty

    def qty_max(self, market: bool) -> Decimal:
        mx = self.market_max_qty if market else self.max_qty
        return mx if mx > 0 else Decimal("0")

    def round_qty(self, qty, market: bool = True, mode: str = "down") -> Decimal:
        fn = {"up": ceil_step, "nearest": round_step}.get(mode, floor_step)
        return fn(qty, self.qty_step(market))

    def qty_for_notional(self, notional, price, market: bool = True, buffer_pct: float = 0.0,
                         min_notional_floor: float = 0.0) -> Decimal:
        """Cantidad mínima (múltiplo de step) que cubre ``notional`` al ``price``
        y cumple minQty y el notional mínimo (con colchón opcional)."""
        p = D(price)
        if p <= 0:
            raise ValueError("el precio debe ser > 0")
        step = self.qty_step(market)
        qty = ceil_step(D(notional) / p, step)
        qty = max(qty, self.qty_min(market))
        floor_notional = max(self.min_notional, D(min_notional_floor)) * (1 + D(buffer_pct) / 100)
        if qty * p < floor_notional:
            qty = max(ceil_step(floor_notional / p, step), self.qty_min(market))
        return qty

    def min_qty_for_price(self, price, market: bool = False, buffer_pct: float = 0.0) -> Decimal:
        return self.qty_for_notional(0, price, market=market, buffer_pct=buffer_pct)

    def price_band(self, mark) -> tuple[Optional[Decimal], Optional[Decimal]]:
        """Banda PERCENT_PRICE (precio mínimo, máximo) alrededor del mark."""
        if not mark or self.multiplier_up is None or self.multiplier_down is None:
            return None, None
        m = D(mark)
        return m * self.multiplier_down, m * self.multiplier_up

    def clamp_to_band(self, price, mark) -> Decimal:
        """Ajusta ``price`` dentro de la banda PERCENT_PRICE (si se conoce)."""
        low, high = self.price_band(mark)
        p = D(price)
        if high is not None and high > 0 and p > high:
            p = floor_step(high, self.tick_size)
        if low is not None and low > 0 and p < low:
            p = ceil_step(low, self.tick_size)
        return p

    def validate(self, qty, price=None, market: bool = True) -> list[str]:
        """Problemas de la orden según los filtros (lista vacía = válida)."""
        issues = []
        q = D(qty)
        if q <= 0:
            issues.append("cantidad <= 0")
        if (q % self.qty_step(market)) != 0:
            issues.append(f"cantidad {fmt(q)} no es múltiplo de stepSize {fmt(self.qty_step(market))}")
        if q < self.qty_min(market):
            issues.append(f"cantidad menor a minQty {fmt(self.qty_min(market))}")
        mx = self.qty_max(market)
        if mx > 0 and q > mx:
            issues.append(f"cantidad mayor a maxQty {fmt(mx)}")
        if price is not None:
            p = D(price)
            if (p % self.tick_size) != 0:
                issues.append(f"precio {fmt(p)} no es múltiplo de tickSize {fmt(self.tick_size)}")
            if q * p < self.min_notional:
                issues.append(f"notional {fmt(q * p)} menor al mínimo {fmt(self.min_notional)}")
        return issues

    @property
    def tradable(self) -> bool:
        return self.status == "TRADING"

    # ── Serialización ─────────────────────────────────────────────────────
    def to_json(self) -> dict:
        out = {}
        for f in fields(self):
            value = getattr(self, f.name)
            if f.name in _DECIMAL_FIELDS:
                out[f.name] = fmt(value) if value is not None else None
            else:
                out[f.name] = value
        return out

    def to_public(self) -> dict:
        return {
            "symbol": self.symbol,
            "status": self.status,
            "tick": fmt(self.tick_size),
            "step": fmt(self.step_size),
            "market_step": fmt(self.market_step_size),
            "min_qty": fmt(self.min_qty),
            "max_qty": fmt(self.max_qty),
            "market_max_qty": fmt(self.market_max_qty),
            "min_notional": fmt(self.min_notional),
            "price_decimals": decimals_of(self.tick_size),
            "qty_decimals": decimals_of(self.step_size),
            "max_leverage": self.max_leverage,
            "max_orders": self.max_num_orders,
            "source": self.source,
        }

    @classmethod
    def from_json(cls, data: dict) -> "SymbolRules":
        kwargs: dict[str, Any] = {}
        names = {f.name for f in fields(cls)}
        for key, value in data.items():
            if key not in names:
                continue
            if key in _DECIMAL_FIELDS:
                kwargs[key] = D(value) if value not in (None, "") else None
            else:
                kwargs[key] = value
        return cls(**kwargs)

    @classmethod
    def from_binance(cls, entry: dict) -> "SymbolRules":
        filters = {f.get("filterType"): f for f in entry.get("filters", [])}
        price_f = filters.get("PRICE_FILTER", {})
        lot_f = filters.get("LOT_SIZE", {})
        mlot_f = filters.get("MARKET_LOT_SIZE", lot_f)
        notional_f = filters.get("MIN_NOTIONAL", {})
        pct_f = filters.get("PERCENT_PRICE", {})

        def dec(src: dict, key: str, default: str) -> Decimal:
            value = src.get(key)
            try:
                return D(value) if value not in (None, "") else D(default)
            except ValueError:
                return D(default)

        return cls(
            symbol=entry["symbol"],
            status=entry.get("status", "TRADING"),
            base_asset=entry.get("baseAsset", ""),
            quote_asset=entry.get("quoteAsset", "USDT"),
            contract_type=entry.get("contractType", "PERPETUAL"),
            onboard_date=int(entry.get("onboardDate") or 0),
            tick_size=dec(price_f, "tickSize", "0.0001").normalize(),
            min_price=dec(price_f, "minPrice", "0").normalize(),
            max_price=dec(price_f, "maxPrice", "0").normalize(),
            step_size=dec(lot_f, "stepSize", "1").normalize(),
            min_qty=dec(lot_f, "minQty", "1").normalize(),
            max_qty=dec(lot_f, "maxQty", "0").normalize(),
            market_step_size=dec(mlot_f, "stepSize", "1").normalize(),
            market_min_qty=dec(mlot_f, "minQty", "1").normalize(),
            market_max_qty=dec(mlot_f, "maxQty", "0").normalize(),
            min_notional=dec(notional_f, "notional", "5").normalize(),
            multiplier_up=dec(pct_f, "multiplierUp", "0").normalize() if pct_f else None,
            multiplier_down=dec(pct_f, "multiplierDown", "0").normalize() if pct_f else None,
            max_num_orders=int(filters.get("MAX_NUM_ORDERS", {}).get("limit", 200) or 200),
            max_num_algo_orders=int(filters.get("MAX_NUM_ALGO_ORDERS", {}).get("limit", 10) or 10),
            price_precision=int(entry.get("pricePrecision", 4) or 0),
            quantity_precision=int(entry.get("quantityPrecision", 0) or 0),
            source="snapshot",
        )


def step_for_price(price: float) -> Decimal:
    step = Decimal("1")
    for threshold, tier_step in PRICE_STEP_TIERS:
        if price > threshold:
            step = Decimal(tier_step)
        else:
            break
    return step


def tick_for_price(price: float) -> Decimal:
    """Tick aproximado con ~5 cifras significativas (heurística)."""
    if price <= 0:
        return Decimal("0.0001")
    exponent = math.floor(math.log10(price)) - 4
    exponent = max(-8, min(exponent, 0))
    return Decimal(1).scaleb(exponent)


def heuristic_rules(symbol: str, price: float, min_notional: float) -> SymbolRules:
    step = step_for_price(price)
    tick = tick_for_price(price)
    return SymbolRules(
        symbol=symbol,
        tick_size=tick,
        step_size=step,
        min_qty=step,
        market_step_size=step,
        market_min_qty=step,
        min_notional=D(min_notional),
        price_precision=decimals_of(tick),
        quantity_precision=decimals_of(step),
        source="heuristic",
    )


def parse_exchange_info(payload: dict) -> dict[str, SymbolRules]:
    out: dict[str, SymbolRules] = {}
    for entry in payload.get("symbols", []):
        if entry.get("quoteAsset") not in ("USDT", "USDC") and entry.get("marginAsset") not in ("USDT", "USDC"):
            continue
        try:
            rules = SymbolRules.from_binance(entry)
        except Exception as exc:  # una entrada rara no debe invalidar el snapshot
            log.warning("exchangeInfo: no se pudo leer %s: %s", entry.get("symbol"), exc)
            continue
        out[rules.symbol] = rules
    return out


def apply_brackets(rules: dict[str, SymbolRules], brackets: list) -> int:
    """Aplica el leverage máximo de /fapi/v1/leverageBracket (opcional)."""
    count = 0
    for item in brackets or []:
        sym = item.get("symbol")
        lv = [int(b.get("initialLeverage", 0) or 0) for b in item.get("brackets", [])]
        if sym in rules and lv:
            rules[sym].max_leverage = max(lv)
            count += 1
    return count


def _atomic_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name, dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def build_snapshot(rules: dict[str, SymbolRules], source: str) -> dict:
    return {
        "version": SNAPSHOT_VERSION,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": source,
        "count": len(rules),
        "symbols": {sym: r.to_json() for sym, r in sorted(rules.items())},
    }


@dataclass
class SnapshotMeta:
    path: str = ""
    generated_at: str = ""
    source: str = ""
    count: int = 0
    loaded_at: float = 0.0
    learned: int = 0
    contract_updates: int = 0


class ExchangeInfo:
    """Registro de reglas por símbolo, sin REST en caliente."""

    def __init__(self, data_dir: Path, bundled_path: Path, min_notional_floor: float = 5.0):
        self.data_dir = Path(data_dir)
        self.bundled_path = Path(bundled_path)
        self.snapshot_path = self.data_dir / "exchange_info.json"
        self.learned_path = self.data_dir / "exchange_info_learned.json"
        self.min_notional_floor = min_notional_floor
        self._rules: dict[str, SymbolRules] = {}
        self._learned: dict[str, dict] = {}
        self.meta = SnapshotMeta()

    # ── Carga / guardado ──────────────────────────────────────────────────
    def load(self) -> None:
        for candidate in (self.snapshot_path, self.bundled_path):
            if candidate.exists():
                try:
                    data = json.loads(candidate.read_text(encoding="utf-8"))
                    self._rules = {s: SymbolRules.from_json(r) for s, r in data.get("symbols", {}).items()}
                    self.meta = SnapshotMeta(
                        path=str(candidate),
                        generated_at=data.get("generated_at", ""),
                        source=data.get("source", ""),
                        count=len(self._rules),
                        loaded_at=time.time(),
                    )
                    log.info("exchangeInfo local: %d símbolos (%s, generado %s)",
                             len(self._rules), candidate, self.meta.generated_at or "¿?")
                    break
                except Exception as exc:
                    log.error("exchangeInfo: snapshot %s corrupto (%s); se ignora", candidate, exc)
        if not self._rules:
            log.warning("exchangeInfo: no hay snapshot local; se usarán reglas heurísticas hasta generarlo")

        if self.learned_path.exists():
            try:
                self._learned = json.loads(self.learned_path.read_text(encoding="utf-8"))
                self.meta.learned = len(self._learned)
            except Exception as exc:
                log.warning("exchangeInfo: no se pudo leer %s: %s", self.learned_path, exc)

    def replace_all(self, rules: dict[str, SymbolRules], source: str) -> None:
        """Reemplaza el snapshot completo (bootstrap/refresh) y lo persiste."""
        # Conservar leverage máximo ya conocido si el nuevo no lo trae.
        for sym, r in rules.items():
            old = self._rules.get(sym)
            if old and not r.max_leverage and old.max_leverage:
                r.max_leverage = old.max_leverage
        self._rules = rules
        snapshot = build_snapshot(rules, source)
        _atomic_write_json(self.snapshot_path, snapshot)
        # Las reglas exactas sustituyen a lo aprendido por heurística.
        self._learned = {s: v for s, v in self._learned.items() if s not in rules}
        self._save_learned()
        self.meta = SnapshotMeta(
            path=str(self.snapshot_path),
            generated_at=snapshot["generated_at"],
            source=source,
            count=len(rules),
            loaded_at=time.time(),
            learned=len(self._learned),
        )
        log.info("exchangeInfo actualizado: %d símbolos guardados en %s", len(rules), self.snapshot_path)

    def _save_learned(self) -> None:
        try:
            _atomic_write_json(self.learned_path, self._learned)
        except Exception as exc:
            log.warning("exchangeInfo: no se pudo guardar lo aprendido: %s", exc)

    # ── Consulta ──────────────────────────────────────────────────────────
    def __len__(self) -> int:
        return len(self._rules)

    def known(self, symbol: str) -> bool:
        return symbol.upper() in self._rules

    def symbols(self) -> list[str]:
        return sorted(self._rules)

    def get(self, symbol: str, ref_price: float = 0.0) -> SymbolRules:
        symbol = symbol.upper()
        base = self._rules.get(symbol)
        if base is None:
            base = heuristic_rules(symbol, ref_price or 1.0, self.min_notional_floor)
        learned = self._learned.get(symbol)
        if learned:
            changes = {k: (D(v) if k in _DECIMAL_FIELDS else v) for k, v in learned.items() if k != "_ts"}
            base = replace(base, **changes, source="learned" if base.source == "heuristic" else base.source)
        return base

    def tradable(self, symbol: str) -> tuple[bool, str]:
        symbol = symbol.upper()
        rules = self._rules.get(symbol)
        if rules is None:
            if self._rules:
                return True, "símbolo fuera del snapshot: se usan reglas heurísticas"
            return True, ""
        if rules.status != "TRADING":
            return False, f"{symbol} está en estado {rules.status} (no operable)"
        return True, ""

    # ── Aprendizaje por errores ───────────────────────────────────────────
    def learn(self, symbol: str, **changes) -> SymbolRules:
        symbol = symbol.upper()
        entry = self._learned.setdefault(symbol, {})
        for key, value in changes.items():
            entry[key] = fmt(value) if key in _DECIMAL_FIELDS else value
        entry["_ts"] = int(time.time())
        self.meta.learned = len(self._learned)
        self._save_learned()
        log.warning("exchangeInfo: aprendido para %s → %s", symbol,
                    ", ".join(f"{k}={fmt(v) if k in _DECIMAL_FIELDS else v}" for k, v in changes.items()))
        return self.get(symbol)

    def learn_coarser_qty(self, symbol: str, current: SymbolRules) -> SymbolRules:
        step = coarser_step(current.step_size)
        return self.learn(symbol, step_size=step, min_qty=max(step, current.min_qty),
                          market_step_size=step, market_min_qty=max(step, current.market_min_qty))

    def learn_coarser_tick(self, symbol: str, current: SymbolRules) -> SymbolRules:
        return self.learn(symbol, tick_size=coarser_step(current.tick_size))

    def learn_min_notional(self, symbol: str, value) -> SymbolRules:
        return self.learn(symbol, min_notional=D(value))

    def learn_status(self, symbol: str, status: str) -> None:
        rules = self._rules.get(symbol.upper())
        if rules is not None:
            rules.status = status
        else:
            self.learn(symbol, status=status)

    # ── Stream !contractInfo ──────────────────────────────────────────────
    def apply_contract_info(self, event: dict) -> None:
        symbol = (event.get("s") or "").upper()
        if not symbol:
            return
        self.meta.contract_updates += 1
        rules = self._rules.get(symbol)
        status = event.get("cs")
        if rules is None:
            if status == "TRADING" and self._rules:
                log.info("contractInfo: nuevo contrato %s (no está en el snapshot; reglas heurísticas)", symbol)
            if status:
                self._learned.setdefault(symbol, {})["status"] = status
            return
        if status and status != rules.status:
            log.info("contractInfo: %s %s → %s", symbol, rules.status, status)
            rules.status = status
        brackets = event.get("bks") or []
        levs = [int(b.get("ma", 0) or 0) for b in brackets]
        if levs and max(levs) > 0:
            rules.max_leverage = max(levs)

    def stats(self) -> dict:
        return {
            "count": len(self._rules),
            "generated_at": self.meta.generated_at,
            "source": self.meta.source,
            "path": self.meta.path,
            "learned": len(self._learned),
            "contract_updates": self.meta.contract_updates,
            "age_h": round(self.age_hours(), 1) if self.meta.generated_at else None,
        }

    def age_hours(self) -> float:
        if not self.meta.generated_at:
            return float("inf")
        try:
            ts = datetime.strptime(self.meta.generated_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except ValueError:
            return float("inf")
        return (datetime.now(timezone.utc) - ts).total_seconds() / 3600
