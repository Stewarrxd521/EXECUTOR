"""Estado de la cuenta alimentado por eventos (User Data Stream).

Las posiciones, balances, órdenes abiertas y algo orders (TP/SL) se mantienen
en memoria a partir de los eventos push de Binance. Las consultas por la WS API
(``account.position``, ``v2/account.balance``) solo se usan al arrancar, al
reconectar el stream y como verificación periódica de baja frecuencia.
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Iterable, Optional

from .precision import safe_float

log = logging.getLogger("executor.account")

OPEN_STATUSES = {"NEW", "PARTIALLY_FILLED"}
ALGO_OPEN_STATUSES = {"NEW", "TRIGGERING"}


@dataclass
class Position:
    symbol: str
    side: str  # LONG | SHORT | BOTH
    amount: float  # con signo (+ long / − short)
    entry_price: float = 0.0
    break_even: float = 0.0
    unrealized: float = 0.0
    margin_type: str = "cross"
    isolated_wallet: float = 0.0
    liquidation_price: float = 0.0
    update_ts: float = field(default_factory=time.time)

    @property
    def direction(self) -> str:
        if self.side in ("LONG", "SHORT"):
            return self.side
        return "LONG" if self.amount > 0 else "SHORT"

    @property
    def qty(self) -> float:
        return abs(self.amount)

    def pnl(self, mark: float) -> float:
        if not mark or not self.entry_price:
            return self.unrealized
        return (mark - self.entry_price) * self.amount

    def to_dict(self) -> dict:
        data = asdict(self)
        data["direction"] = self.direction
        data["qty"] = self.qty
        return data


@dataclass
class OpenOrder:
    kind: str  # order | algo
    id: int
    client_id: str
    symbol: str
    side: str
    position_side: str
    type: str
    status: str
    price: float = 0.0
    trigger_price: float = 0.0
    qty: float = 0.0
    filled: float = 0.0
    reduce_only: bool = False
    close_position: bool = False
    time: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class OrderUpdate:
    """Evento ORDER_TRADE_UPDATE normalizado."""

    symbol: str
    client_id: str
    side: str
    order_type: str
    orig_type: str
    status: str
    exec_type: str
    order_id: int
    qty: float
    price: float
    avg_price: float
    stop_price: float
    last_qty: float
    cum_qty: float
    last_price: float
    commission: float
    commission_asset: str
    realized_pnl: float
    position_side: str
    reduce_only: bool
    close_position: bool
    trade_time: int
    maker: bool

    @classmethod
    def from_event(cls, evt: dict) -> "OrderUpdate":
        o = evt.get("o", {})
        return cls(
            symbol=o.get("s", ""),
            client_id=o.get("c", ""),
            side=o.get("S", ""),
            order_type=o.get("o", ""),
            orig_type=o.get("ot", o.get("o", "")),
            status=o.get("X", ""),
            exec_type=o.get("x", ""),
            order_id=int(o.get("i") or 0),
            qty=safe_float(o.get("q")),
            price=safe_float(o.get("p")),
            avg_price=safe_float(o.get("ap")),
            stop_price=safe_float(o.get("sp")),
            last_qty=safe_float(o.get("l")),
            cum_qty=safe_float(o.get("z")),
            last_price=safe_float(o.get("L")),
            commission=safe_float(o.get("n")),
            commission_asset=o.get("N") or "",
            realized_pnl=safe_float(o.get("rp")),
            position_side=o.get("ps", "BOTH"),
            reduce_only=bool(o.get("R")),
            close_position=bool(o.get("cp")),
            trade_time=int(o.get("T") or 0),
            maker=bool(o.get("m")),
        )

    @property
    def is_fill(self) -> bool:
        return self.exec_type == "TRADE" and self.last_qty > 0

    @property
    def fee_usdt(self) -> float:
        return self.commission if self.commission_asset in ("USDT", "USDC") else 0.0


@dataclass
class Balance:
    asset: str
    wallet: float = 0.0
    cross_wallet: float = 0.0
    available: float = 0.0
    cross_unpnl: float = 0.0


class AccountState:
    def __init__(self, hedge_hint: bool = False):
        self.positions: dict[tuple[str, str], Position] = {}
        self.balances: dict[str, Balance] = {}
        self.leverage: dict[str, int] = {}
        self.margin_type: dict[str, str] = {}
        self.orders: dict[int, OpenOrder] = {}
        self.algos: dict[int, OpenOrder] = {}
        self.hedge_mode: Optional[bool] = None
        self.hedge_hint = hedge_hint
        self.last_sync = 0.0
        self.last_event = 0.0
        self.version = 0

    # ── Modo de posición ──────────────────────────────────────────────────
    @property
    def is_hedge(self) -> bool:
        return self.hedge_mode if self.hedge_mode is not None else self.hedge_hint

    def position_side_for(self, direction: str) -> str:
        return direction.upper() if self.is_hedge else "BOTH"

    def _touch(self) -> None:
        self.version += 1
        self.last_event = time.time()

    # ── Snapshots por WS API ──────────────────────────────────────────────
    def apply_positions_snapshot(self, rows: Iterable[dict]) -> None:
        rows = list(rows)
        sides = {r.get("positionSide") for r in rows}
        if sides & {"LONG", "SHORT"}:
            self.hedge_mode = True
        elif "BOTH" in sides:
            self.hedge_mode = False
        fresh: dict[tuple[str, str], Position] = {}
        for r in rows:
            sym = r.get("symbol", "")
            if not sym:
                continue
            lev = int(safe_float(r.get("leverage"), 0))
            if lev:
                self.leverage[sym] = lev
            mt = (r.get("marginType") or "").lower()
            if mt:
                self.margin_type[sym] = mt
            amt = safe_float(r.get("positionAmt"))
            if amt == 0:
                continue
            side = r.get("positionSide", "BOTH")
            fresh[(sym, side)] = Position(
                symbol=sym, side=side, amount=amt,
                entry_price=safe_float(r.get("entryPrice")),
                break_even=safe_float(r.get("breakEvenPrice")),
                unrealized=safe_float(r.get("unRealizedProfit")),
                margin_type=mt or "cross",
                isolated_wallet=safe_float(r.get("isolatedWallet") or r.get("isolatedMargin")),
                liquidation_price=safe_float(r.get("liquidationPrice")),
            )
        self.positions = fresh
        self.last_sync = time.time()
        self._touch()

    def apply_balances(self, rows: Iterable[dict]) -> None:
        for r in rows:
            asset = r.get("asset")
            if not asset:
                continue
            b = self.balances.setdefault(asset, Balance(asset))
            b.wallet = safe_float(r.get("balance"), b.wallet)
            b.cross_wallet = safe_float(r.get("crossWalletBalance"), b.cross_wallet)
            b.available = safe_float(r.get("availableBalance"), b.available)
            b.cross_unpnl = safe_float(r.get("crossUnPnl"), b.cross_unpnl)
        self._touch()

    def seed_orders(self, rows: Iterable[dict]) -> None:
        for r in rows:
            oid = int(r.get("orderId") or 0)
            if not oid:
                continue
            self.orders[oid] = OpenOrder(
                kind="order", id=oid, client_id=r.get("clientOrderId", ""), symbol=r.get("symbol", ""),
                side=r.get("side", ""), position_side=r.get("positionSide", "BOTH"), type=r.get("type", ""),
                status=r.get("status", "NEW"), price=safe_float(r.get("price")), trigger_price=safe_float(r.get("stopPrice")),
                qty=safe_float(r.get("origQty")), filled=safe_float(r.get("executedQty")),
                reduce_only=bool(r.get("reduceOnly")), close_position=bool(r.get("closePosition")),
                time=safe_float(r.get("time")) / 1000 or time.time(),
            )
        self._touch()

    def seed_algos(self, rows: Iterable[dict]) -> None:
        for r in rows:
            aid = int(r.get("algoId") or 0)
            if not aid:
                continue
            self.algos[aid] = OpenOrder(
                kind="algo", id=aid, client_id=r.get("clientAlgoId", ""), symbol=r.get("symbol", ""),
                side=r.get("side", ""), position_side=r.get("positionSide", "BOTH"),
                type=r.get("orderType") or r.get("type", ""), status=r.get("algoStatus", "NEW"),
                price=safe_float(r.get("price")), trigger_price=safe_float(r.get("triggerPrice")),
                qty=safe_float(r.get("quantity")), reduce_only=bool(r.get("reduceOnly")),
                close_position=bool(r.get("closePosition")),
                time=safe_float(r.get("createTime")) / 1000 or time.time(),
            )
        self._touch()

    def register_algo_result(self, result: dict) -> None:
        """Registra al instante un algo order recién creado (antes del evento)."""
        if isinstance(result, dict) and result.get("algoId"):
            self.seed_algos([result])

    # ── Eventos del User Data Stream ──────────────────────────────────────
    def apply_account_update(self, evt: dict) -> list[tuple[str, str]]:
        """Devuelve las claves (símbolo, lado) de posiciones que cambiaron."""
        a = evt.get("a", {})
        for b in a.get("B", []):
            bal = self.balances.setdefault(b.get("a", ""), Balance(b.get("a", "")))
            bal.wallet = safe_float(b.get("wb"), bal.wallet)
            bal.cross_wallet = safe_float(b.get("cw"), bal.cross_wallet)
        changed = []
        for p in a.get("P", []):
            sym, side = p.get("s", ""), p.get("ps", "BOTH")
            key = (sym, side)
            amt = safe_float(p.get("pa"))
            if amt == 0:
                self.positions.pop(key, None)
            else:
                pos = self.positions.get(key) or Position(sym, side, amt)
                pos.amount = amt
                pos.entry_price = safe_float(p.get("ep"), pos.entry_price)
                pos.break_even = safe_float(p.get("bep"), pos.break_even)
                pos.unrealized = safe_float(p.get("up"), pos.unrealized)
                pos.margin_type = p.get("mt", pos.margin_type)
                pos.isolated_wallet = safe_float(p.get("iw"), pos.isolated_wallet)
                pos.update_ts = time.time()
                self.positions[key] = pos
            if side in ("LONG", "SHORT"):
                self.hedge_mode = True
            changed.append(key)
        self._touch()
        return changed

    def apply_order_update(self, evt: dict) -> OrderUpdate:
        upd = OrderUpdate.from_event(evt)
        if upd.status in OPEN_STATUSES:
            self.orders[upd.order_id] = OpenOrder(
                kind="order", id=upd.order_id, client_id=upd.client_id, symbol=upd.symbol, side=upd.side,
                position_side=upd.position_side, type=upd.order_type, status=upd.status, price=upd.price,
                trigger_price=upd.stop_price, qty=upd.qty, filled=upd.cum_qty, reduce_only=upd.reduce_only,
                close_position=upd.close_position,
                time=(self.orders[upd.order_id].time if upd.order_id in self.orders else time.time()),
            )
        else:
            self.orders.pop(upd.order_id, None)
        self._touch()
        return upd

    def apply_algo_update(self, evt: dict) -> dict:
        o = evt.get("o", {})
        aid = int(o.get("aid") or 0)
        status = o.get("X", "")
        if aid:
            if status in ALGO_OPEN_STATUSES:
                prev = self.algos.get(aid)
                self.algos[aid] = OpenOrder(
                    kind="algo", id=aid, client_id=o.get("caid", ""), symbol=o.get("s", ""), side=o.get("S", ""),
                    position_side=o.get("ps", "BOTH"), type=o.get("o", ""), status=status,
                    price=safe_float(o.get("p")), trigger_price=safe_float(o.get("tp")), qty=safe_float(o.get("q")),
                    reduce_only=bool(o.get("R")), close_position=bool(o.get("cp")),
                    time=prev.time if prev else time.time(),
                )
            else:
                self.algos.pop(aid, None)
        self._touch()
        return o

    def apply_config_update(self, evt: dict) -> None:
        ac = evt.get("ac")
        if ac and ac.get("s"):
            self.leverage[ac["s"]] = int(safe_float(ac.get("l"), 0)) or self.leverage.get(ac["s"], 0)
            self._touch()

    # ── Consultas ─────────────────────────────────────────────────────────
    def position(self, symbol: str, direction: Optional[str] = None) -> Optional[Position]:
        symbol = symbol.upper()
        if direction:
            direction = direction.upper()
            pos = self.positions.get((symbol, direction))
            if pos:
                return pos
            both = self.positions.get((symbol, "BOTH"))
            if both and both.direction == direction:
                return both
            return None
        matches = [p for (s, _), p in self.positions.items() if s == symbol]
        return matches[0] if len(matches) == 1 else None

    def positions_for(self, symbol: str) -> list[Position]:
        return [p for (s, _), p in self.positions.items() if s == symbol.upper()]

    def orders_for(self, symbol: Optional[str] = None) -> list[OpenOrder]:
        return [o for o in self.orders.values() if symbol is None or o.symbol == symbol]

    def algos_for(self, symbol: Optional[str] = None, position_side: Optional[str] = None,
                  types: Optional[set] = None) -> list[OpenOrder]:
        out = []
        for o in self.algos.values():
            if symbol and o.symbol != symbol:
                continue
            if position_side and o.position_side != position_side:
                continue
            if types and o.type not in types:
                continue
            out.append(o)
        return out

    def usdt(self) -> Balance:
        return self.balances.get("USDT") or Balance("USDT")

    def unrealized(self, marks: dict) -> float:
        total = 0.0
        for p in self.positions.values():
            m = marks.get(p.symbol)
            total += p.pnl(m.mark if m else 0.0)
        return total

    def summary(self, marks: dict) -> dict:
        usdt = self.usdt()
        upnl = self.unrealized(marks)
        return {
            "wallet": usdt.wallet,
            "available": usdt.available,
            "cross_wallet": usdt.cross_wallet,
            "unrealized": upnl,
            "margin_balance": usdt.wallet + upnl,
            "hedge_mode": self.is_hedge,
            "hedge_detected": self.hedge_mode is not None,
            "positions": len(self.positions),
            "orders": len(self.orders),
            "algos": len(self.algos),
            "last_sync": self.last_sync,
        }
