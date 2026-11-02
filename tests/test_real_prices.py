"""Los precios de apertura y cierre que guarda el executor son los reales de Binance (los de sus fills), no el
precio que envía el bot ni el mark price: una orden de mercado se llena contra el libro (con deslizamiento) y la
respuesta de la orden puede no traer el precio (NEW sin avgPrice); el precio real llega por el user stream."""

import asyncio

import pytest

from tests.conftest import wait_for
from tests.test_lots import SYM, amt, close, open_, opened


@pytest.fixture(params=[False, True], ids=["respuesta_con_precio", "respuesta_NEW_sin_precio"])
def binance_fills(request, fake):
    fake.slippage_pct = 0.5                      # BUY se llena a 3015 y SELL a 2985 (mark 3000)
    fake.market_reply_new = request.param
    return fake


async def test_open_and_close_prices_are_binance_fills(service, binance_fills):
    fake, t = binance_fills, service.trades
    open_(t, 1, 0.01)                            # el bot manda price 3000
    trade = await opened(t, fake, 0.01)
    await wait_for(lambda: trade.entry_price == 3015.0 and trade.tranches[0]["fill_price"] == 3015.0)
    close(t, 1)
    await wait_for(lambda: amt(fake) == 0 and t.closed)
    await asyncio.sleep(0.3)
    rec = t.closed[-1]
    assert (rec.entry_price, rec.close_price) == (3015.0, 2985.0)
    assert abs(t.realized_total - (fake.wallet - 10_000.0)) < 1e-6   # PnL = lo que realizó Binance


async def test_short_stopped_and_flipped_to_long_records_both_real_fills(service, binance_fills):
    """Un bot en SHORT toca su SL: cierra el SHORT y abre LONG en el mismo instante."""
    fake, t = binance_fills, service.trades
    open_(t, 1, 0.01, direction="SHORT")
    short = await opened(t, fake, 0.01, "SHORT")
    await wait_for(lambda: short.entry_price == 2985.0)          # SELL al bid
    fake.set_mark(SYM, 3100.0)                                   # el precio sube y toca el SL del bot
    await asyncio.sleep(0.2)
    close(t, 1, direction="SHORT")
    open_(t, 2, 0.01, direction="LONG")
    long = await opened(t, fake, 0.01, "LONG")
    await wait_for(lambda: amt(fake, "SHORT") == 0 and t.closed)
    await asyncio.sleep(0.3)
    rec = t.closed[-1]
    # Las dos son compras a mercado (cerrar SHORT y abrir LONG): ambas al ask real, 3100 + 0.5 %.
    assert rec.close_price == pytest.approx(3115.5) and long.entry_price == pytest.approx(3115.5)
    assert rec.entry_price == 2985.0
