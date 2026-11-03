"""Un close sin trade_id ni dirección es «cerrar todo» el símbolo: LONG y SHORT, con o sin trade local."""

from tests.conftest import wait_for
from tests.test_assumed_protection import manual_open
from tests.test_lots import SYM, amt, open_, opened


async def test_close_without_id_or_direction_closes_both_sides(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    manual_open(fake, 0.02, direction="SHORT")          # abierto fuera del executor
    await wait_for(lambda: amt(fake, "SHORT") == 0.02)
    t.handle_signal({"action": "close", "symbol": SYM, "reason": "STOP"})
    await wait_for(lambda: amt(fake) == 0 and amt(fake, "SHORT") == 0 and not t.trades, timeout=10)
    await wait_for(lambda: t.signals(5) and t.signals(5)[0]["action"] == "close")
    assert t.signals(5)[0]["ok"]


async def test_close_with_direction_keeps_the_other_side(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    manual_open(fake, 0.02, direction="SHORT")
    await wait_for(lambda: amt(fake, "SHORT") == 0.02)
    t.handle_signal({"action": "close", "symbol": SYM, "direction": "LONG", "reason": "STOP"})
    await wait_for(lambda: amt(fake) == 0 and not t.trades)
    assert amt(fake, "SHORT") == 0.02
