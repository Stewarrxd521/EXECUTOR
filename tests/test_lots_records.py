"""Registros e historial de los cierres por trade_id: PnL, comisiones y cierres sin confirmar."""

import asyncio
from decimal import Decimal

import pytest

import executor.trading as trading_mod
from tests.conftest import wait_for
from tests.fake_binance import _Err
from tests.test_lots import SYM, amt, close, open_, opened
from tests.test_lots_ledger import THIN, TIMEOUT, lag, last_close, lost_close, outside, start, status_down


@pytest.fixture(autouse=True)
def quick_retries(monkeypatch):
    monkeypatch.setattr(trading_mod, "CLOSE_RETRY_DELAYS", (0.0, 0.5, 1.0))
    monkeypatch.setattr(trading_mod, "UNKNOWN_OPEN_CHECKS", (1.0, 2.0, 3.0))


def wallet(fake):
    return fake.wallet - 10_000.0


def consistent(t):
    return abs(t.realized_total - sum(r.pnl_usdt for r in t.closed)) < 1e-6


async def test_many_positions_never_mix_their_records(service, fake):
    """Cada posición cerrada por su último lote deja de existir; los fills de las siguientes nunca van a su
    registro (antes se buscaba por id() de un objeto ya liberado, que Python reutiliza)."""
    t = service.trades
    place = service.core.ws.place_order

    async def slow(**kw):                          # el user stream llega antes que la respuesta de la WS API
        r = await place(**kw)
        await asyncio.sleep(0.05)
        return r
    service.core.ws.place_order = slow
    for cycle in range(12):
        a, b = 100 + 2 * cycle, 101 + 2 * cycle
        direction = "LONG" if cycle % 2 else "SHORT"
        open_(t, a, 0.01, direction=direction)
        await opened(t, fake, 0.01, direction)
        open_(t, b, 0.01, direction=direction)
        await opened(t, fake, 0.02, direction)
        close(t, a, direction=direction)
        await wait_for(lambda: amt(fake, direction) == 0.01)
        close(t, b, direction=direction)
        await wait_for(lambda: amt(fake, direction) == 0 and t.get_trade(SYM, direction) is None)
    await asyncio.sleep(0.5)
    assert len(t.closed) == 24 and len({id(r) for r in t.closed}) == 24
    assert abs(t.realized_total - wallet(fake)) < 1e-6 and consistent(t)


# ── Cierre de lote con varias órdenes cuya última respuesta se pierde ────────
def thin_then_lost(fake, down):
    place, state = fake._place, {"n": 0}

    def hooked(p):
        if fake._is_closing(p) and state["n"] < 3:
            state["n"] += 1
            if state["n"] == 1:                    # MARKET fuera de la banda de PERCENT_PRICE
                raise _Err(*THIN)
            if state["n"] == 2:                    # LIMIT IOC: se llena a medias y expira
                q = Decimal(p["quantity"])
                res = place({**p, "quantity": str((q / 2).quantize(Decimal("0.001")))})
                res.update({"origQty": str(q), "status": "EXPIRED"})
                return res
            down["on"] = True                      # el MARKET siguiente se ejecuta; respuesta y evento perdidos
            fake.inject_after.append(TIMEOUT)
        return place(p)
    fake._place = hooked


async def three_lots(t, fake):
    for i, tid in enumerate((1, 2, 3)):
        open_(t, tid, 0.02)
        await opened(t, fake, 0.02 * (i + 1))
    return t.get_trade(SYM, "LONG")


async def test_lost_second_order_of_a_lot_close_is_counted_once(service, fake):
    t = service.trades
    await three_lots(t, fake)
    down = status_down(fake, order_events=True)
    down["on"] = False
    thin_then_lost(fake, down)
    close(t, 1)
    await wait_for(lambda: amt(fake) == 0.04, timeout=10)
    await wait_for(lambda: last_close(t, 1) is not None, timeout=20)
    down["on"] = False
    await t.verify_lot_attempts()
    close(t, 2)
    await wait_for(lambda: amt(fake) == 0.02)
    close(t, 3)
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)


async def test_lost_second_order_survives_a_restart(fake, tmp_path, snapshot_file, monkeypatch):
    monkeypatch.setattr(trading_mod, "CLOSE_RETRY_DELAYS", (0.0,))
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        trade = await three_lots(t, fake)
        down = status_down(fake, order_events=True)
        down["on"] = False
        thin_then_lost(fake, down)
        close(t, 1)
        await wait_for(lambda: amt(fake) == 0.04 and (trade.key, "1") in t._lot_attempts
                       and not t._lot_attempts[(trade.key, "1")]["active"], timeout=10)
    finally:
        await svc.stop()                           # deploy antes de confirmarlo
    down["on"] = False
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        await t.verify_lot_attempts()
        close(t, 2)
        await wait_for(lambda: amt(fake) == 0.02)
        close(t, 3)
        await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)
        assert sorted((r.paper_trade_id, round(r.quantity, 6)) for r in t.closed) == [(1, 0.02), (2, 0.02),
                                                                                      (3, 0.02)]
    finally:
        await svc.stop()


# ── Cierres sin confirmar: precio, cierre total y concurrencia ───────────────
async def test_confirmed_lost_close_uses_its_fill_price(service, fake):
    t = service.trades
    open_(t, 1, 0.02)
    await opened(t, fake, 0.02)
    open_(t, 2, 0.02)
    await opened(t, fake, 0.04)
    fake.set_mark(SYM, 3100.0)
    await asyncio.sleep(0.3)
    down = await lost_close(t, fake, 1, order_events=True)   # se ejecuta a 3100
    fake.set_mark(SYM, 3300.0)                    # el mercado se mueve antes de poder confirmarlo
    await asyncio.sleep(0.3)
    down["on"] = False
    await t.verify_lot_attempts()
    rec = next(r for r in t.closed if r.paper_trade_id == 1)
    close(t, 2)
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)
    await asyncio.sleep(0.5)
    lost_fee = 0.02 * 3100 * 0.0004               # solo venía en el evento perdido
    assert abs(rec.close_price - 3100.0) < 1e-6
    assert abs(t.realized_total - lost_fee - wallet(fake)) < 1e-6 and consistent(t)


async def test_whole_close_confirming_a_lost_lot_close_books_both(service, fake):
    t = service.trades
    open_(t, 1, 0.02)
    await opened(t, fake, 0.02)
    open_(t, 2, 0.02)
    await opened(t, fake, 0.04)
    fake.set_mark(SYM, 3100.0)
    await asyncio.sleep(0.3)
    down = await lost_close(t, fake, 1, order_events=True)
    down["on"] = False                            # order.status vuelve antes de la verificación de 15 s
    close(t)                                      # cierre total (sin trade_id)
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)
    await asyncio.sleep(0.5)
    assert sorted((r.paper_trade_id, round(r.quantity, 6)) for r in t.closed) == [(1, 0.02), (2, 0.02)]
    lost_fee = 0.02 * 3100 * 0.0004
    assert abs(t.realized_total - lost_fee - wallet(fake)) < 1e-6 and consistent(t)


async def test_external_close_and_the_periodic_check_book_a_lost_close_once(service, fake):
    t = service.trades
    open_(t, 1, 0.02)
    await opened(t, fake, 0.02)
    open_(t, 2, 0.02)
    await opened(t, fake, 0.04)
    down = await lost_close(t, fake, 1, order_events=True)
    down["on"] = False                            # order.status vuelve, pero lento
    ws = service.core.ws
    status = ws.order_status

    async def slow_status(*a, **kw):
        await asyncio.sleep(0.3)
        return await status(*a, **kw)
    ws.order_status = slow_status
    settle = t._settle_before_finalize

    async def hooked(trade):
        t._spawn(t.verify_lot_attempts())         # la verificación de cada 15 s llega a la vez
        await settle(trade)
    t._settle_before_finalize = hooked
    outside(fake, "SELL", 0.02, "web_manual_close")   # el resto se cierra a mano en Binance
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None, timeout=15)
    await asyncio.sleep(1.5)
    assert len(t.closed) == len({id(r) for r in t.closed})
    assert [r.paper_trade_id for r in t.closed].count(1) == 1 and consistent(t)


async def test_late_opening_fee_reaches_the_closed_record(service, fake):
    t = service.trades
    lag(fake, 1.0)
    open_(t, 5, 0.01)
    await wait_for(lambda: t.get_trade(SYM, "LONG") is not None)
    close(t, 5)
    await wait_for(lambda: t.closed and t.get_trade(SYM, "LONG") is None, timeout=10)
    await asyncio.sleep(2.5)
    assert abs(t.realized_total - wallet(fake)) < 1e-6 and consistent(t)


async def test_detached_record_keeps_its_own_entry(service, fake):
    t = service.trades
    open_(t, 1, 0.02)
    await opened(t, fake, 0.02)
    fake.set_mark(SYM, 3100.0)
    await asyncio.sleep(0.3)
    open_(t, 2, 0.02)
    await opened(t, fake, 0.04)
    fake.set_mark(SYM, 3200.0)
    await asyncio.sleep(0.3)
    down = await lost_close(t, fake, 1, order_events=True)
    down["events"] = False
    close(t)
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)
    down["on"] = False
    await t.verify_lot_attempts()
    await asyncio.sleep(0.3)
    gross = {r.paper_trade_id: round(r.pnl_usdt + r.fees_usdt, 6) for r in t.closed}
    assert gross == {1: 4.0, 2: 2.0}              # cada uno con su propia entrada (3000 y 3100), cierre a 3200
    assert consistent(t)
