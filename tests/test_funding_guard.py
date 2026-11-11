"""Protección de funding: antes de un cobro caro que la posición paga, se abre en el lado contrario la misma
cantidad neta (LONG y SHORT se compensan: el funding neto es cero) y se cierra cuando Binance ya cobró.
Se repite en cada cobro mientras la posición siga abierta y nunca toca los lotes de los bots."""

import asyncio
import time

import pytest

from executor.funding import BOT
from tests.conftest import wait_for
from tests.test_lots import SYM, amt, close, lots, open_, opened

MARGIN = ("order.place", -2019, "Margin is insufficient.")


@pytest.fixture
def guard(service):
    s = service.settings
    s.funding_hedge_lead_ms, s.funding_hedge_lag_ms, s.funding_hedge_max_hold_ms = 500, 300, 2500
    return service.funding


def now_ms() -> int:
    return int(time.time() * 1000)


async def schedule(service, fake, rate: float, in_ms: int = 1800, symbol: str = SYM) -> int:
    """Próximo cobro de ``symbol`` dentro de ``in_ms`` con esa tasa (0.005 = 0.5 %)."""
    at = now_ms() + in_ms
    fake.funding[symbol] = (rate, at)
    await wait_for(lambda: (m := service.core.market.marks.get(symbol)) is not None and m.next_funding_ms == at
                   and m.funding_rate == rate)
    return at


def charge_at(fake, at: int, delay_ms: int = 0, symbol: str = SYM, next_ms: int = 0) -> asyncio.Task:
    """Binance cobra el funding ``delay_ms`` después de la hora; devuelve la tarea (su resultado: posiciones
    que había en ese instante)."""
    async def run():
        await asyncio.sleep(max(0.0, (at + delay_ms - now_ms()) / 1000))
        held = {d: amt(fake, d) for d in ("LONG", "SHORT")}
        fake.charge_funding(symbol, next_ms=next_ms)
        return held
    return asyncio.create_task(run())


def records(t):
    return [r for r in t.closed if r.bot == BOT]


async def test_expensive_funding_is_hedged_and_nets_to_zero(service, fake, guard):
    t = service.trades
    open_(t, 1, 0.01)
    trade = await opened(t, fake, 0.01)
    at = await schedule(service, fake, 0.005)                    # +0.5 %: el LONG paga
    held = await charge_at(fake, at)
    assert held == {"LONG": 0.01, "SHORT": 0.01}                  # cubierta en el instante del cobro
    assert abs(fake.funding_paid[SYM]) < 1e-9                     # lo que paga el LONG lo cobra el SHORT
    await wait_for(lambda: amt(fake, "SHORT") == 0 and records(t), timeout=10)
    assert amt(fake) == 0.01 and lots(trade) == {"1": 0.01}       # la posición del bot, intacta
    rec = records(t)[-1]
    assert (rec.direction, rec.quantity, rec.status, rec.closed_by) == ("SHORT", 0.01, "FUNDING", "funding")
    stats = t.stats()                                             # no cuenta como operación perdida del bot
    assert (stats["funding_hedges"], stats["wins"], stats["losses"]) == (1, 0, 0)
    assert abs(stats["funding_hedge_pnl"] - rec.pnl_usdt) < 1e-12
    done = guard.recent[-1]
    assert done.state == "cerrada" and done.opened_ms < at        # abierta antes del cobro
    assert at - done.opened_ms <= service.settings.funding_hedge_lead_ms + 100


@pytest.mark.parametrize("direction,rate", [("LONG", 0.0005), ("LONG", -0.005), ("SHORT", 0.005)],
                         ids=["barato", "a_favor_del_long", "a_favor_del_short"])
async def test_cheap_or_favourable_funding_is_left_alone(service, fake, guard, direction, rate):
    t = service.trades
    other = "SHORT" if direction == "LONG" else "LONG"
    open_(t, 1, 0.01, direction=direction)
    await opened(t, fake, 0.01, direction)
    at = await schedule(service, fake, rate)
    held = await charge_at(fake, at)
    assert held[other] == 0 and held[direction] == 0.01
    await asyncio.sleep(0.5)
    assert amt(fake, other) == 0 and not records(t) and not guard.recent


async def test_short_paying_negative_funding_is_hedged_with_a_long(service, fake, guard):
    t = service.trades
    open_(t, 1, 0.01, direction="SHORT")
    await opened(t, fake, 0.01, "SHORT")
    at = await schedule(service, fake, -0.004)                   # −0.4 %: el SHORT paga
    held = await charge_at(fake, at)
    assert held == {"LONG": 0.01, "SHORT": 0.01} and abs(fake.funding_paid[SYM]) < 1e-9
    await wait_for(lambda: amt(fake, "LONG") == 0 and records(t), timeout=10)
    assert amt(fake, "SHORT") == 0.01


async def test_hourly_funding_uses_the_lower_threshold(service, fake, guard):
    """0.2 % no llega al umbral general (0.3 %), pero con cobro cada hora el umbral es 0.15 %."""
    t = service.trades
    fake.funding_intervals[SYM] = 1
    await guard.refresh_intervals()
    assert guard.interval_h(SYM) == 1 and guard.threshold_pct(1) == 0.15 and guard.threshold_pct(8) == 0.3
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    at = await schedule(service, fake, 0.002)
    held = await charge_at(fake, at)
    assert held["SHORT"] == 0.01
    await wait_for(lambda: amt(fake, "SHORT") == 0 and records(t), timeout=10)


async def test_only_the_net_position_is_hedged_and_bot_lots_are_untouched(service, fake, guard):
    """LONG 0.03 de un bot y SHORT 0.01 de otro: paga el neto (LONG 0.02); se cubre solo eso."""
    t = service.trades
    open_(t, 1, 0.03)
    long_trade = await opened(t, fake, 0.03)
    open_(t, 2, 0.01, direction="SHORT")
    short_trade = await opened(t, fake, 0.01, "SHORT")
    at = await schedule(service, fake, 0.005)
    held = await charge_at(fake, at)
    assert held == {"LONG": 0.03, "SHORT": 0.03} and abs(fake.funding_paid[SYM]) < 1e-9
    await wait_for(lambda: amt(fake, "SHORT") == 0.01 and records(t), timeout=10)
    assert lots(long_trade) == {"1": 0.03} and lots(short_trade) == {"2": 0.01}
    assert t.get_trade(SYM, "SHORT") is short_trade and records(t)[-1].quantity == 0.02


async def test_every_funding_is_hedged_while_the_position_stays_open(service, fake, guard):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    at = await schedule(service, fake, 0.005, in_ms=1500)
    first = await charge_at(fake, at, next_ms=at + 2500)          # el siguiente cobro, 2.5 s después
    second = await charge_at(fake, at + 2500, next_ms=at + 3600_000)
    assert first["SHORT"] == 0.01 and second["SHORT"] == 0.01
    assert abs(fake.funding_paid[SYM]) < 1e-9
    await wait_for(lambda: amt(fake, "SHORT") == 0 and len(records(t)) == 2, timeout=10)
    assert amt(fake) == 0.01


async def test_hedge_is_kept_until_binance_reports_the_charge(service, fake, guard):
    """Binance cobra 1.2 s tarde: la cobertura sigue abierta hasta su aviso, no solo «lag» ms."""
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    at = await schedule(service, fake, 0.005)
    held = await charge_at(fake, at, delay_ms=1200)
    assert held["SHORT"] == 0.01 and abs(fake.funding_paid[SYM]) < 1e-9
    await wait_for(lambda: amt(fake, "SHORT") == 0, timeout=10)


async def test_without_a_charge_notice_the_hedge_closes_after_max_hold(service, fake, guard):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    at = await schedule(service, fake, 0.005)
    await wait_for(lambda: amt(fake, "SHORT") == 0.01, timeout=10)
    await wait_for(lambda: amt(fake, "SHORT") == 0, timeout=10)
    assert now_ms() >= at + service.settings.funding_hedge_max_hold_ms - 50
    assert amt(fake) == 0.01


async def test_notice_without_symbol_still_closes_the_hedge(service, fake, guard):
    fake.funding_event_symbol = False                             # Binance antiguo: el aviso no trae el símbolo
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    at = await schedule(service, fake, 0.005)
    held = await charge_at(fake, at)
    assert held["SHORT"] == 0.01
    await wait_for(lambda: amt(fake, "SHORT") == 0, timeout=10)
    assert now_ms() < at + service.settings.funding_hedge_max_hold_ms


async def test_rejected_hedge_is_reported_and_leaves_nothing_behind(service, fake, guard):
    """Sin margen para la cobertura (-2019): no se registra una cobertura asumida; se avisa."""
    t = service.trades
    open_(t, 1, 0.01)
    trade = await opened(t, fake, 0.01)
    at = await schedule(service, fake, 0.005)
    fake.inject.append(MARGIN)
    held = await charge_at(fake, at)
    assert held == {"LONG": 0.01, "SHORT": 0.0}
    await wait_for(lambda: guard.recent and guard.recent[-1].state == "fallida")
    assert t.get_trade(SYM, "SHORT") is None and lots(trade) == {"1": 0.01}
    assert any(e["kind"] == "funding" and e["level"] == "error" for e in service.core.bus.recent)


async def test_hedge_left_open_by_a_restart_is_closed(service, fake, guard):
    """Una cobertura de un cobro ya pasado (el executor se reinició con ella abierta) se cierra sola."""
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    old = (now_ms() - 60_000) // 1000
    await t.open_trade(SYM, "SHORT", quantity=0.01, paper_trade_id=old, source="funding", bot=BOT, exact_qty=True,
                       allow_assumed=False)
    await wait_for(lambda: amt(fake, "SHORT") == 0 and records(t), timeout=10)
    assert amt(fake) == 0.01


async def test_bot_close_during_the_hedge_only_closes_its_own_lot(service, fake, guard):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    at = await schedule(service, fake, 0.005)
    await wait_for(lambda: amt(fake, "SHORT") == 0.01, timeout=10)
    close(t, 1)                                                   # el bot cierra su LONG con la cobertura abierta
    await wait_for(lambda: amt(fake) == 0)
    assert amt(fake, "SHORT") == 0.01                             # la cobertura no es de ese trade_id
    fake.charge_funding(SYM)
    await wait_for(lambda: amt(fake, "SHORT") == 0, timeout=10)
    assert at and not t.trades


async def test_plan_explains_why_a_position_is_not_hedged(service, fake, guard):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    await schedule(service, fake, 0.005, in_ms=600_000)
    plan = guard.plan(SYM)
    assert plan.hedge and (plan.direction, plan.qty) == ("SHORT", 0.01) and plan.threshold_pct == 0.3
    service.core.account.hedge_mode = False
    assert not guard.plan(SYM).hedge and "One-way" in guard.plan(SYM).reason
    service.core.account.hedge_mode = True
    service.settings.funding_hedge_min_rate_pct = 0.6
    assert not guard.plan(SYM).hedge and "umbral" in guard.plan(SYM).reason
    view = service.snapshot()["funding"]
    assert view["enabled"] and view["symbols"][0]["symbol"] == SYM


async def test_disabled_guard_does_nothing(service, fake, guard):
    service.settings.funding_hedge = False
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    at = await schedule(service, fake, 0.005)
    held = await charge_at(fake, at)
    assert held["SHORT"] == 0 and fake.funding_paid[SYM] > 0


async def test_bot_open_on_the_hedge_side_keeps_its_lot_when_the_hedge_closes(service, fake, guard):
    """Un bot abre SHORT mientras la cobertura SHORT está abierta: al cerrarla solo sale el lote de la cobertura."""
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    at = await schedule(service, fake, 0.005)
    await wait_for(lambda: amt(fake, "SHORT") == 0.01, timeout=10)
    open_(t, 2, 0.02, direction="SHORT")
    await wait_for(lambda: amt(fake, "SHORT") == 0.03)
    await charge_at(fake, at)
    await wait_for(lambda: amt(fake, "SHORT") == 0.02 and records(t), timeout=10)
    assert lots(t.get_trade(SYM, "SHORT")) == {"2": 0.02} and amt(fake) == 0.01
