"""Operación asumida (-2019, margen insuficiente): Binance no ejecuta nada, así que no hay precio real. Tanto la
apertura como el cierre se registran al precio al que se habría llenado la orden de mercado en ese instante: el
libro de Binance recorrido por la cantidad (abrir LONG / cerrar SHORT contra los asks; abrir SHORT / cerrar LONG
contra los bids), no el precio de la señal ni el mark."""

import pytest

from tests.conftest import wait_for
from tests.test_lots import SYM, amt, close, open_, opened

MARGIN = (("order.place", -2019, "Margin is insufficient."))


async def assumed(t, direction="LONG"):
    await wait_for(lambda: (tr := t.get_trade(SYM, direction)) is not None and tr.tranches)
    return t.get_trade(SYM, direction)


@pytest.mark.parametrize("direction,expected", [("LONG", 3015.0), ("SHORT", 2985.0)])
async def test_assumed_open_takes_the_book_side_not_the_signal_price(service, fake, direction, expected):
    t = service.trades
    fake.slippage_pct = 0.5                     # libro: ask 3015 / bid 2985 (mark 3000, el bot manda 3000)
    fake.inject.append(MARGIN)
    open_(t, 1, 0.01, direction=direction)
    trade = await assumed(t, direction)
    assert amt(fake, direction) == 0 and trade.order_assumed
    assert trade.entry_price == expected
    assert trade.tranches[0]["fill_price"] == expected and trade.tranches[0]["price_source"] == "libro"


async def test_assumed_open_walks_the_book_for_its_quantity(service, fake):
    t = service.trades
    fake.book[SYM] = {"bids": [[2999.0, 5]], "asks": [[3001.0, 0.01], [3002.0, 0.01], [3010.0, 5]]}
    fake.inject.append(MARGIN)
    open_(t, 1, 0.04)
    trade = await assumed(t)
    # 0.01@3001 + 0.01@3002 + 0.02@3010 = 120.27 / 0.04
    assert abs(trade.entry_price - 3005.75) < 1e-9


async def test_assumed_open_beyond_the_visible_book_uses_the_worst_level(service, fake):
    t = service.trades
    fake.book[SYM] = {"bids": [[2999.0, 5]], "asks": [[3001.0, 0.01], [3004.0, 0.01]]}
    fake.inject.append(MARGIN)
    open_(t, 1, 0.04)
    trade = await assumed(t)
    assert abs(trade.entry_price - (3001 * 0.01 + 3004 * 0.03) / 0.04) < 1e-9


async def test_without_depth_the_best_ask_is_used(service, fake):
    t = service.trades
    fake.slippage_pct = 0.5
    fake.inject += [MARGIN, ("depth", -1121, "Invalid symbol.")]
    open_(t, 1, 0.01)
    trade = await assumed(t)
    assert trade.entry_price == 3015.0 and trade.tranches[0]["price_source"] == "mejor ask/bid"


async def test_without_book_the_reference_price_is_kept(service, fake):
    t = service.trades
    fake.inject += [MARGIN, ("depth", -1003, "Too many requests."), ("ticker.book", -1003, "Too many requests.")]
    open_(t, 1, 0.01)
    trade = await assumed(t)
    assert trade.entry_price == 3000.0 and trade.tranches[0]["price_source"] == "referencia"


@pytest.mark.parametrize("direction,entry,exit_", [("LONG", 3015.0, 2985.0), ("SHORT", 2985.0, 3015.0)])
@pytest.mark.parametrize("by_trade_id", [True, False], ids=["close_trade_id", "close_sin_id"])
async def test_assumed_close_takes_the_book_side_too(service, fake, direction, entry, exit_, by_trade_id):
    t = service.trades
    fake.slippage_pct = 0.5                     # libro: ask 3015 / bid 2985 (mark 3000)
    fake.inject.append(MARGIN)
    open_(t, 1, 0.01, direction=direction)
    await assumed(t, direction)
    close(t, 1 if by_trade_id else None, direction=direction, price=3000)
    await wait_for(lambda: t.closed and t.get_trade(SYM, direction) is None)
    rec = t.closed[-1]
    assert rec.order_assumed and (rec.entry_price, rec.close_price) == (entry, exit_)
    assert rec.close_price_source == "libro"
    sign = 1 if direction == "LONG" else -1
    assert abs(rec.pnl_usdt - (exit_ - entry) * 0.01 * sign) < 1e-9     # el coste del spread, como en real
    assert amt(fake, direction) == 0 and fake.wallet == 10_000.0       # nada se envió a Binance


async def test_assumed_lot_in_a_real_position_keeps_its_own_prices(service, fake):
    """El trade_id 1 es real; el 2 llega con -2019 cuando el precio ya subió: su registro usa su propia entrada
    (el ask de ese momento), no la media de la posición real, y la posición real no se toca."""
    t = service.trades
    fake.slippage_pct = 0.5
    open_(t, 1, 0.01)
    trade = await opened(t, fake, 0.01)
    await wait_for(lambda: trade.entry_price == 3015.0)
    fake.set_mark(SYM, 3100.0)                  # ask 3115.5 / bid 3084.5
    fake.inject.append(MARGIN)
    open_(t, 2, 0.02)
    await wait_for(lambda: trade.assumed_lots.get("2") == 0.02)
    assert trade.entry_price == 3015.0          # la entrada real no cambia por lo asumido
    close(t, 2, price=3100)
    await wait_for(lambda: t.closed)
    rec = t.closed[-1]
    assert rec.order_assumed and rec.paper_trade_id == 2
    assert (rec.entry_price, rec.close_price, rec.close_price_source) == (3115.5, 3084.5, "libro")
    assert amt(fake) == 0.01 and t.get_trade(SYM, "LONG") is trade and trade.assumed_lots == {}
