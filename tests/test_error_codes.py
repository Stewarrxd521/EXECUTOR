"""Cada código de error de Binance: o se corrige solo, o la operación se rechaza limpia (sin posición fantasma y
sin tocar la cantidad de otros trade_id)."""

import asyncio

import pytest

import executor.trading as trading_mod
from executor.errors import CATALOG, Action
from tests.conftest import wait_for
from tests.test_lots import SYM, amt, close, lots, open_, opened

# Lo que el executor arregla solo al abrir (y vuelve a enviar).
FIXED_ON_OPEN = {Action.RETRY, Action.RESYNC_TIME, Action.CHECK_STATUS, Action.NEW_CLIENT_ID,
                 Action.FIX_PRECISION, Action.FIX_PRICE_TICK, Action.RAISE_QTY, Action.RAISE_NOTIONAL,
                 Action.CLAMP_PRICE, Action.LOWER_LEVERAGE, Action.SPLIT_QTY, Action.FLIP_POSITION_SIDE}
MSG = {-4164: "Order's notional must be no smaller than 20 (unless you choose reduce only).",
       -1003: "Too many requests; current limit is 2400 requests per minute."}


@pytest.fixture(autouse=True)
def quick(monkeypatch):
    monkeypatch.setattr(trading_mod, "CLOSE_RETRY_DELAYS", (0.0, 0.3, 0.6))
    monkeypatch.setattr(trading_mod, "UNKNOWN_OPEN_CHECKS", (0.3, 0.6, 1.0))


def settled(t):
    return not t.inflight(SYM) and not t._reconciling and not t._closing


@pytest.mark.parametrize("code", sorted(CATALOG))
async def test_open_with_each_error_code(service, fake, code):
    t = service.trades
    info = CATALOG[code]
    fake.inject.append(("order.place", code, MSG.get(code, info.name)))
    open_(t, 1, 0.01)
    await asyncio.sleep(0.2)
    await wait_for(lambda: settled(t) and t.signals(1, trade_id=1), timeout=15)
    await asyncio.sleep(0.3)
    trade = t.get_trade(SYM, "LONG")
    real = amt(fake)
    if info.action in FIXED_ON_OPEN:
        assert real > 0 and trade is not None, f"{code} {info.name}: debía corregirse solo"
    if trade is not None and not trade.order_assumed:
        assert abs(trade.ledger_qty - real) < 1e-9, f"{code}: libro {trade.ledger_qty} != Binance {real}"
    if real == 0:
        assert trade is None or trade.order_assumed, f"{code}: trade sin posición en Binance"


@pytest.mark.parametrize("code", sorted(CATALOG))
async def test_lot_close_with_each_error_code_never_touches_the_other_lot(service, fake, code):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    open_(t, 2, 0.02)
    trade = await opened(t, fake, 0.03)
    info = CATALOG[code]
    fake.inject.append(("order.place", code, MSG.get(code, info.name)))
    close(t, 1)
    await asyncio.sleep(0.2)
    await wait_for(lambda: settled(t) and t.signals(1, trade_id=1)
                   and t.signals(1, trade_id=1)[0]["action"] == "close", timeout=20)
    await asyncio.sleep(0.3)
    real = amt(fake)
    assert real in (0.02, 0.03), f"{code} {info.name}: Binance {real} (el lote 2 tiene 0.02)"
    if real == 0.02:
        assert lots(trade) == {"2": 0.02}
    else:
        assert lots(trade) == {"1": 0.01, "2": 0.02}


async def test_max_position_for_the_leverage_lowers_it_step_by_step(service, fake):
    t = service.trades
    fake.max_position_by_leverage = {7: 50.0, 5: 100.0}      # 90 USDT no caben a 7x, sí a 5x
    t.handle_signal({"action": "open", "trade_id": 1, "symbol": SYM, "direction": "LONG", "price": 3000,
                     "quantity": 0.03, "leverage": 7})
    trade = await opened(t, fake, 0.03)
    assert fake.leverage[SYM] == 5 and trade.leverage == 5


async def test_max_position_below_the_minimum_leverage_is_rejected(service, fake):
    t = service.trades
    service.settings.leverage_min = 4
    fake.max_position_by_leverage = {7: 10.0, 5: 10.0, 4: 10.0, 3: 1000.0}
    t.handle_signal({"action": "open", "trade_id": 1, "symbol": SYM, "direction": "LONG", "price": 3000,
                     "quantity": 0.03, "leverage": 7})
    await wait_for(lambda: t.signals(1, trade_id=1) and not t.inflight(SYM), timeout=10)
    assert amt(fake) == 0 and t.get_trade(SYM, "LONG") is None
    assert fake.leverage[SYM] == 4                            # nunca por debajo de LEVERAGE_MIN
    assert not t.signals(1, trade_id=1)[0]["ok"]


async def test_order_above_the_max_quantity_is_split(service, fake):
    t = service.trades
    fake.inject.append(("order.place", -4005, "Quantity greater than max quantity."))
    open_(t, 1, 0.04)
    trade = await opened(t, fake, 0.04)
    assert lots(trade) == {"1": 0.04}
    assert sum(1 for m, p in fake.requests if m == "order.place") >= 3


async def test_tp_already_crossed_closes_its_lot_at_market(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    open_(t, 2, 0.02)
    await opened(t, fake, 0.03)
    fake.inject.append(("algoOrder.place", -2021, "Order would immediately trigger."))
    t.handle_signal({"action": "open_tp", "trade_id": 1, "symbol": SYM, "direction": "LONG", "trigger_price": 2990})
    await wait_for(lambda: amt(fake) == 0.02)              # solo el lote del trade_id 1
    await wait_for(lambda: lots(t.get_trade(SYM, "LONG")) == {"2": 0.02})


async def test_rate_limited_lot_close_waits_and_closes(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    open_(t, 2, 0.02)
    trade = await opened(t, fake, 0.03)
    fake.inject.append(("order.place", -1003, MSG[-1003]))
    close(t, 1)
    await wait_for(lambda: amt(fake) == 0.02, timeout=25)
    assert lots(trade) == {"2": 0.02}


async def test_rate_limited_open_is_rejected_at_once(service, fake):
    t = service.trades
    fake.inject.append(("order.place", -1003, MSG[-1003]))
    open_(t, 1, 0.01)
    await wait_for(lambda: t.signals(1, trade_id=1) and not t.inflight(SYM), timeout=5)
    assert amt(fake) == 0 and not t.signals(1, trade_id=1)[0]["ok"]
