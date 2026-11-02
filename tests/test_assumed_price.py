"""Apertura asumida (-2019, margen insuficiente): Binance no ejecuta nada, así que no hay precio real. Se registra
el precio al que se habría llenado la orden de mercado en ese instante: el libro de Binance (asks para LONG, bids
para SHORT) recorrido por la cantidad, no el precio de la señal ni el mark."""

import pytest

from tests.conftest import wait_for
from tests.test_lots import SYM, amt, open_

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
