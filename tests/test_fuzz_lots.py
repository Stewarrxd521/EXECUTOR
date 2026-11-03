"""Fuzz con semillas de los lotes por trade_id (FUZZ_SEEDS="0-29" para más; ver el resto de opciones abajo).

Extends verify_lots2/fuzz/test_fuzz_lots.py with:
  (a) user-stream lag: per seed one of none / fifo (every event delayed 0.1-0.5 s, order kept) /
      au_reorder (ACCOUNT_UPDATE delayed 0.1-0.5 s independently, ORDER_TRADE_UPDATE on time) /
      otu_lag (ORDER_TRADE_UPDATE delayed independently; only with FUZZ_AVOID=0, finding G1);
      delayed events only reach the connections that existed when Binance emitted them (a restart loses them);
  (b) lost close responses (inject_after order.place -1007) + order.status outage toggle on fake._dispatch
      for a random duration (short: settled by submit; medium: by the retries; long: by the 15 s check);
  (c) TP/SL sent with opens (tp/sl fields) and fired with fake.set_mark;
  (d) partial fills on closes: k closing MARKET -> -4131 and LIMIT IOC half filled;
  (e) outside opens / reductions (web_* client ids), also right when another bot's open is placed;
  (f) assumed opens (-2019);
  (g) restart mid-scenario (between steps, and with signals in flight).

Model: expected real quantity per (bot, trade_id, symbol, direction) plus the outside quantity, applied in the
true Binance order. Outside reductions follow the executor's documented policy (FUZZ_OUTSIDE=prorata: pro rata
over lots + outside quantity, TP/SL owner lot first) or FUZZ_OUTSIDE=first (outside quantity first, finding G2).

FUZZ_SEEDS="0-29" (~9 min sequential, or 4 shards in parallel ~3 min); FUZZ_STEPS_MIN/MAX (10/18);
FUZZ_LAG=none|fifo|au_reorder|otu_lag forces the lag mode; FUZZ_OUT=<dir> for the per-seed JSON traces.
FUZZ_AVOID=1 (default) keeps the generator away from the confirmed findings (see FINDINGS.md):
  G1 no otu_lag/au_reorder lag modes; G2 outside reductions modelled pro rata (FUZZ_OUTSIDE=first shows G2);
  G3 with stream lag, the closes of a group wait 0.7 s after its outside fill; G5 no thin book on closes without
  trade_id; G7 order.status outage of close_lost_group capped below the retry window; G8 I7 ignores dust lots;
  legacy: no unknown trade_id close on a position that only holds other bots' (outside) quantity.
Invariants (checked after each settled step): I1 Binance qty == model sum per position; I2 Binance change per step
== expected; I3 executor lots == model per trade_id (and no phantom lot; I3q trade.quantity == Binance);
I4 every trade_id closed by signal has its record(s) with the expected quantity and no unexpected lot records;
I5 realized_total == sum(closed pnl); I6 no _lot_attempts pending 17 s after order.status is back;
I7 no TP/SL of a trade_id closed by its own signal stays live; plus settle timeouts (deadlock) and restart history.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import random
import time
from decimal import Decimal, ROUND_HALF_EVEN
from pathlib import Path

import pytest

import executor.trading as trading_mod
from executor.service import ExecutorService
from tests.conftest import make_settings, wait_for
from tests.fake_binance import _Err

OUT = Path(os.environ["FUZZ_OUT"]) if os.environ.get("FUZZ_OUT") else None   # trazas JSON por semilla
if OUT is not None:
    OUT.mkdir(parents=True, exist_ok=True)

SYMS = {"ETHUSDT": Decimal("0.001"), "GRIDUSDT": Decimal("0.1")}
QRANGE = {"ETHUSDT": (7, 30), "GRIDUSDT": (1, 12)}
BOTS = ["kline", "app25", "", ""]


def is_open_of(cid, b, tid):
    """Fill of the executor's open for (bot b, tid): clientOrderId X<tid>[:<bot>]-..."""
    return cid.rsplit("-", 1)[0] == trading_mod.lot_cid_prefix("X", trading_mod.lot_ref(BOTS[b], tid))
DIRS = ("LONG", "SHORT")
OUTSIDE = (-1, 0)
AVOID = os.environ.get("FUZZ_AVOID", "0") == "1"
OUTSIDE_POLICY = os.environ.get("FUZZ_OUTSIDE", "first")
STATELESS = os.environ.get("FUZZ_STATELESS", "0") == "1"   # restarts lose trades.json (Render ephemeral disk)
CLOSE_REASON = "BOTCLOSE"
LOST = "Timeout waiting for response from backend server."


def seeds() -> list[int]:
    raw = os.environ.get("FUZZ_SEEDS", "0-3,5,35,40")  # 5, 35, 40: regresiones (cierre tras -2022, close sin id ni dirección)
    out = []
    for part in raw.split(","):
        if "-" in part:
            a, b = part.split("-")
            out += list(range(int(a), int(b) + 1))
        elif part.strip():
            out.append(int(part))
    return out


def lk(bot: int, tid: int) -> str:
    if bot < 0:
        return "<outside>"
    name = BOTS[bot]
    return f"{name}:{tid}" if name else str(tid)


def fstep(sym) -> float:
    return float(SYMS[sym])


def rnd(q: float, sym: str) -> float:
    step = SYMS[sym]
    return float((Decimal(repr(q)) / step).quantize(Decimal(1), rounding=ROUND_HALF_EVEN) * step)


def pos(fake, sym, d) -> float:
    return round(abs(fake.positions.get((sym, d), {"amt": 0.0})["amt"]), 10)


class Model:
    def __init__(self):
        self.owners: dict[tuple, float] = {}       # (bot, tid, sym, dir) -> real qty ; OUTSIDE -> other bots
        self.assumed: dict[tuple, float] = {}
        self.real_sum: dict[tuple, float] = {}
        self.bot_sum: dict[tuple, float] = {}
        self.where: dict[tuple, tuple] = {}
        self.alive: set = set()
        self.dead: set = set()
        self.algo_owner: dict[int, tuple] = {}     # fake algoId -> ref that sent it with its open
        self.counters = {0: 1, 1: 1, 2: 1000, 3: 5000}
        self.unknown_counter = 9000

    def qty(self, ref) -> float:
        s, d = self.where.get(ref, (None, None))
        return self.owners.get((*ref, s, d), 0.0) if s else 0.0

    def lots(self, sym, d) -> dict:
        return {(b, t): q for (b, t, s, dd), q in self.owners.items()
                if s == sym and dd == d and q > 1e-12 and b >= 0}

    def outside(self, sym, d) -> float:
        return self.owners.get((*OUTSIDE, sym, d), 0.0)

    def total(self, sym, d) -> float:
        return sum(self.lots(sym, d).values()) + self.outside(sym, d)

    def new_tid(self, bot: int) -> int:
        tid = self.counters[bot]
        self.counters[bot] += 1
        return tid

    def mult(self, ref) -> float:
        r, b = self.real_sum.get(ref, 0.0), self.bot_sum.get(ref, 0.0)
        return r / b if r > 0 and b > 0 else 1.0

    def reduce(self, s, d, r, P, first=None, policy="prorata"):
        """Binance reduced (s, d) by r from P outside the executor (other bot, TP/SL, ADL)."""
        if r >= P - 1e-12:
            for k in list(self.owners):
                if k[2] == s and k[3] == d:
                    self.owners[k] = 0.0
            for k in list(self.assumed):
                if k[2] == s and k[3] == d:
                    self.assumed[k] = 0.0
            return
        if first is not None:
            key = (*first, s, d)
            take = min(r, self.owners.get(key, 0.0))
            if take > 0:
                self.owners[key] -= take
                r -= take
        if r <= 1e-12:
            return
        if policy == "first":
            ok = (*OUTSIDE, s, d)
            take = min(r, self.owners.get(ok, 0.0))
            if take > 0:
                self.owners[ok] -= take
                r -= take
            if r <= 1e-12:
                return
        keys = [k for k, v in self.owners.items() if k[2] == s and k[3] == d and v > 1e-12]
        total = sum(self.owners[k] for k in keys)
        if total <= 0:
            return
        share = min(1.0, r / total)
        for k in keys:
            self.owners[k] *= (1 - share)


@pytest.mark.parametrize("stateless", [False, True], ids=["con_trades_json", "sin_trades_json"])
@pytest.mark.parametrize("seed", seeds())
async def test_fuzz_lots(seed, stateless, fake, tmp_path, snapshot_file, monkeypatch):
    monkeypatch.setattr(sys.modules[__name__], "STATELESS", stateless)
    monkeypatch.setattr(trading_mod, "UNKNOWN_OPEN_CHECKS", (0.6, 1.2, 2.5))
    monkeypatch.setattr(trading_mod, "CLOSE_RETRY_DELAYS", (0.0, 1.5, 4.0))
    rng = random.Random(seed)
    lag_rng = random.Random(seed * 7919 + 1)
    trace: list = []
    fill_log: list = []          # every fill in fake (orders + algo triggers), in Binance order
    place_log: list = []         # every order placed through fake._place
    lag_modes = ["none", "fifo", "fifo"] + ([] if AVOID else ["otu_lag"])
    lag_mode = os.environ.get("FUZZ_LAG") or rng.choice(lag_modes)
    result = {"seed": seed, "avoid": AVOID, "lag": lag_mode, "policy": OUTSIDE_POLICY, "violations": [],
              "trace": trace}

    # ── fake Binance instrumentation ────────────────────────────────────────
    orig_apply = fake._apply_fill

    def apply_fill(order, price, qty):
        key = (order["symbol"], order["positionSide"])
        before = abs(fake.positions.get(key, {"amt": 0.0})["amt"])
        fill_log.append({"cid": order.get("clientOrderId", ""), "sym": order["symbol"], "ps": order["positionSide"],
                         "side": order["side"], "qty": qty, "ot": order.get("origType", order["type"]),
                         "before": round(before, 10)})
        return orig_apply(order, price, qty)

    fake._apply_fill = apply_fill

    thin = {"left": 0}
    hooks: list = []             # one-shot callables run when the matching open order is placed
    orig_place = fake._place

    def place(p):
        cid = p.get("newClientOrderId", "")
        for h in list(hooks):
            if cid.startswith(h["prefix"]) and not fake._is_closing(p):
                hooks.remove(h)
                h["fn"]()
        closing = fake._is_closing(p) and cid.startswith(("C", "K"))
        if thin["left"] > 0 and closing:
            if p["type"] == "MARKET":
                raise _Err(-4131, "The counterparty's best price does not meet the PERCENT_PRICE filter limit.")
            if p["type"] == "LIMIT" and p.get("timeInForce") == "IOC":
                thin["left"] -= 1
                q = Decimal(p["quantity"])
                step = SYMS.get(p["symbol"], Decimal("1"))
                half = (q / 2 / step).quantize(Decimal(1)) * step
                if half > 0:
                    res = orig_place({**p, "quantity": str(half)})
                    res.update({"origQty": str(q), "status": "EXPIRED"})
                    place_log.append(cid)
                    return res
        res = orig_place(p)
        place_log.append(cid)
        return res

    fake._place = place

    status_down = {"on": False, "task": None}
    orig_dispatch = fake._dispatch

    def dispatch(method, p):
        if method == "order.status" and status_down["on"]:
            raise _Err(-1001, "Internal error; unable to process your request.")
        return orig_dispatch(method, p)

    fake._dispatch = dispatch

    lag = {"last": 0.0}

    async def lagged_push(event):
        conns = set(fake.user_ws)
        kind = event.get("e")
        loop = asyncio.get_running_loop()
        d = 0.0
        if lag_mode == "fifo":
            target = max(loop.time() + lag_rng.uniform(0.1, 0.5), lag["last"] + 0.002)
            lag["last"] = target
            d = target - loop.time()
        elif lag_mode == "au_reorder" and kind == "ACCOUNT_UPDATE":
            d = lag_rng.uniform(0.1, 0.5)
        elif lag_mode == "otu_lag" and kind == "ORDER_TRADE_UPDATE":
            d = lag_rng.uniform(0.1, 0.5)
        if d > 0:
            await asyncio.sleep(d)
        text = json.dumps({"stream": "lk", "data": event})
        for ws in conns:
            if ws in fake.user_ws:
                try:
                    await ws.send_str(text)
                except Exception:
                    pass

    fake._push = lagged_push

    svc = ExecutorService(make_settings(fake, tmp_path))
    await svc.start()
    await wait_for(lambda: svc.user.conn.connected and svc.trades.ready.is_set())
    m = Model()
    n_steps = rng.randint(int(os.environ.get("FUZZ_STEPS_MIN", 10)), int(os.environ.get("FUZZ_STEPS_MAX", 18)))
    restart_at = rng.randint(3, n_steps - 2) if STATELESS or rng.random() < 0.5 else -1
    seq = [0]

    def busy() -> bool:
        t = svc.trades
        return (any(not x.done() for x in t._tasks) or bool(t._reconciling) or bool(t._closing)
                or bool(t._pending_external) or any(not x.done() for x in fake._tasks)
                or (status_down["task"] is not None and not status_down["task"].done()))

    async def settle(extra: float = 0.35) -> list:
        errs = []
        deadline = time.time() + 90
        for _ in range(2):
            while time.time() < deadline and busy():
                await asyncio.sleep(0.05)
            await asyncio.sleep(extra)
        if busy():
            errs.append("SETTLE timeout 90s: executor still busy (closing=%s reconciling=%s tasks=%d)" % (
                svc.trades._closing, svc.trades._reconciling, sum(1 for x in svc.trades._tasks if not x.done())))
        # I6: unconfirmed lot closes must be settled once order.status is back (service checks every 15 s)
        if svc.trades._lot_attempts and not status_down["on"]:
            t0 = time.time()
            while svc.trades._lot_attempts and time.time() - t0 < 17:
                await asyncio.sleep(0.2)
            while busy() and time.time() < deadline:
                await asyncio.sleep(0.05)
            await asyncio.sleep(extra)
            if svc.trades._lot_attempts:
                errs.append(f"I6 _lot_attempts still pending 17s after order.status is back: "
                            f"{[(k, v.get('qty'), v.get('active')) for k, v in svc.trades._lot_attempts.items()]}")
        return errs

    stateless_done = [False]

    async def restart():
        nonlocal svc
        closed_before, realized_before = len(svc.trades.closed), svc.trades.realized_total
        await svc.stop()
        if STATELESS:
            f = tmp_path / "data" / "trades.json"
            if f.exists():
                f.unlink()
            stateless_done[0] = True
        svc = ExecutorService(make_settings(fake, tmp_path))
        await svc.start()
        await wait_for(lambda: svc.user.conn.connected and svc.trades.ready.is_set(), timeout=15)
        await wait_for(lambda: not svc.trades._rebuilding, timeout=15)
        if STATELESS:
            return len(svc.trades.closed), svc.trades.realized_total
        return closed_before, realized_before

    def check_global():
        t = svc.trades
        errs = []
        for sym in SYMS:
            step = fstep(sym)
            for d in DIRS:
                real = pos(fake, sym, d)
                exp = m.total(sym, d)
                if abs(real - exp) > step * 1.01:
                    errs.append(f"I1 {sym} {d}: Binance {real} != expected {exp:.6f} "
                                f"(lots {m.lots(sym, d)}, outside {m.outside(sym, d):.6f})")
                trade = t.get_trade(sym, d)
                if m.lots(sym, d) and real > 0:
                    if trade is None:
                        errs.append(f"I3 {sym} {d}: Binance has {real} with lots but executor has no trade")
                        continue
                    if abs(trade.quantity - real) > 1e-9 and not trade.order_assumed:
                        errs.append(f"I3q {sym} {d}: trade.quantity {trade.quantity} != Binance {real}")
                if trade is not None and real > 0:
                    eff = trade.effective_lots(real)
                    exp_lots = m.lots(sym, d)
                    for (b, tid), q in exp_lots.items():
                        got = eff.get(lk(b, tid), 0.0)
                        if abs(got - q) > step * 1.01:
                            errs.append(f"I3 {sym} {d}: lot {lk(b, tid)} executor {got:.6f} != expected {q:.6f}")
                    keys = {lk(b, tid) for (b, tid) in exp_lots}
                    for k, v in eff.items():
                        if k not in keys and v > step * 1.01:
                            errs.append(f"I3 {sym} {d}: phantom lot {k}={v:.6f} in executor")
                assumed_here = any(v > 0 for k, v in m.assumed.items() if k[2] == sym and k[3] == d)
                if real == 0 and trade is not None and not assumed_here and not trade.order_assumed:
                    errs.append(f"I3 {sym} {d}: Binance flat but executor keeps trade qty={trade.quantity} "
                                f"lots={trade.lots} assumed={trade.assumed_lots}")
        tot = sum(r.pnl_usdt for r in t.closed)
        if abs(t.realized_total - tot) > 1e-6:
            errs.append(f"I5 realized_total {t.realized_total:.6f} != sum closed pnl {tot:.6f}")
        # TP/SL of a trade_id closed by its own signal must not stay live
        for aid, a in fake.algos.items():
            ref = m.algo_owner.get(aid)
            if ref is not None and ref in m.dead and getattr(m, "closed_by_signal", set()) and \
                    ref in m.closed_by_signal:
                errs.append(f"I7 {a['type']} {a['symbol']} {a['positionSide']} of closed {lk(*ref)} still live "
                            f"(qty {a.get('quantity')})")
        return errs

    m.closed_by_signal = set()

    def open_sig(b, tid, sym, d, q, prot=None):
        sig = {"action": "open", "trade_id": tid, "symbol": sym, "direction": d, "price": fake.marks[sym],
               "quantity": q}
        if prot:
            sig.update(prot)
        if BOTS[b]:
            sig["bot"] = BOTS[b]
        return sig

    def close_sig(b, tid, sym, d, q=None):
        sig = {"action": "close", "trade_id": tid, "reason": CLOSE_REASON,
               "close_price": fake.marks.get(sym or "ETHUSDT")}
        if sym:
            sig["symbol"] = sym
        if d:
            sig["direction"] = d
        if q:
            sig["quantity"] = q
        if BOTS[b]:
            sig["bot"] = BOTS[b]
        return sig

    def pick_open(allow_prot=True):
        allow_prot = allow_prot and not STATELESS
        alive = [r for r in m.alive if r not in m.dead]
        if alive and rng.random() < 0.35:
            b, tid = rng.choice(sorted(alive))
            sym, d = m.where[(b, tid)]
        else:
            b = rng.randrange(4)
            tid = m.new_tid(b)
            sym, d = rng.choice(list(SYMS)), rng.choice(DIRS)
        lo, hi = QRANGE[sym]
        q = float(Decimal(rng.randint(lo, hi)) * SYMS[sym])
        prot = None
        if allow_prot and rng.random() < 0.3:
            mk = fake.marks[sym]
            up, dn = round(mk * rng.choice([1.006, 1.012, 1.02]), 2), round(mk * rng.choice([0.994, 0.988, 0.98]), 2)
            kind = rng.choice(["tp", "sl", "both"])
            prot = {}
            if kind in ("tp", "both"):
                prot["tp"] = up if d == "LONG" else dn
            if kind in ("sl", "both"):
                prot["sl"] = dn if d == "LONG" else up
        return ("open", b, tid, sym, d, q, prot)

    def closable():
        return sorted(r for r in m.alive if r not in m.dead and m.qty(r) > 1e-12)

    def make_close(ref, allow_q=True):
        b, tid = ref
        s, d = m.where[ref]
        q = None
        if allow_q and rng.random() < 0.3:
            q = float(Decimal(rng.randint(1, QRANGE[s][1])) * SYMS[s])
        return ("close", b, tid, s, d if (BOTS[b] or rng.random() < 0.8) else None, q)

    def expect_close(op, owners_now):
        _, b, tid, sym, d, q = op
        ref = (b, tid)
        where = m.where.get(ref)
        if where is None:
            return {}, 0.0, None
        s, dd = where
        if (sym and s != sym) or (d and dd != d):
            return {}, 0.0, None
        have = owners_now.get((b, tid, s, dd), 0.0)
        if have <= 1e-12:
            a = m.assumed.get((b, tid, s, dd), 0.0)
            return {}, 0.0, ("assumed", a) if a > 0 else None
        step = fstep(s)
        tol = step / 2
        whole = True
        amount = have
        if q:
            want = q * m.mult(ref)
            if want < have - tol:
                whole = False
                amount = rnd(want, s)
                if amount <= 0:
                    return {}, 0.0, None
        return {(s, dd): amount}, amount, ("whole" if whole else "partial", amount, s, dd)

    def apply_outside_fills(fills, owners, assumed=None):
        """Outside / TP-SL fills of this step, in Binance order, applied to ``owners`` (dict). An outside open on
        a position with assumed lots goes to those lots first (oldest first, up to their assumed qty): they
        become real (``assumed``: the assumed dict to use, m.assumed by default)."""
        assumed = m.assumed if assumed is None else assumed
        saved = m.owners
        m.owners = owners
        try:
            for f in fills:
                cid = f["cid"]
                if cid.startswith(("X", "C", "K")):
                    continue
                s, d = f["sym"], f["ps"]
                if s not in SYMS:
                    continue
                opening = (f["side"] == "BUY") == (d == "LONG")
                if opening:
                    left = f["qty"]
                    for ak in list(assumed):
                        if ak[2] != s or ak[3] != d or assumed[ak] <= 1e-12 or left <= 1e-12:
                            continue
                        q = min(left, assumed[ak])
                        assumed[ak] -= q
                        m.owners[ak] = m.owners.get(ak, 0.0) + q
                        left -= q
                    if left > 1e-12:
                        k = (*OUTSIDE, s, d)
                        m.owners[k] = m.owners.get(k, 0.0) + left
                    continue
                first = None
                pol = OUTSIDE_POLICY
                if cid.startswith("algo"):
                    first = m.algo_owner.get(int(cid[4:]))
                    pol = "prorata"
                m.reduce(s, d, f["qty"], f["before"], first=first, policy=pol)
        finally:
            m.owners = saved

    async def run_step(i):
        kinds = ["open", "close", "group", "close_unknown", "close_all_sym", "mult", "outside", "outside_open",
                 "around_open", "inject2019", "open_unknown", "close_lost", "thin_close", "fire", "mark",
                 "group_restart", "inject1001", "close_lost_group", "protect"]
        weights = [5, 3, 1.2, 0.4, 0.3, 0.5, 0.6, 0.7, 0.8, 0.35, 0.3, 0.6, 0.6, 0.9, 0.2, 0.2, 0.2, 0.5, 0.5]
        if STATELESS:  # not recoverable from Binance's order history: assumed opens, TP/SL owners, multipliers
            pairs = [(k, (0.8 if k == "group_restart" else w)) for k, w in zip(kinds, weights)
                     if k not in ("inject2019", "protect", "mult", "fire")]
            kinds, weights = [k for k, _ in pairs], [w for _, w in pairs]
        kind = rng.choices(kinds, weights=weights)[0]
        if i == restart_at:
            kind = "restart"
        t = svc.trades
        n_closed0 = len(t.closed)
        log0 = len(fill_log)
        pos0 = {(s, d): pos(fake, s, d) for s in SYMS for d in DIRS}
        ops = []
        expected_red: dict = {}
        tol_mult: dict = {}

        if kind in ("close", "close_lost", "thin_close", "protect") and not closable():
            kind = "open"
        if kind in ("group", "group_restart", "close_lost_group") and len(closable()) < 2:
            kind = "open"
        if kind == "fire":
            live = [(aid, a) for aid, a in fake.algos.items() if a["symbol"] in SYMS
                    and pos(fake, a["symbol"], a["positionSide"]) > 0]
            if not live:
                kind = "open"

        if kind == "mark":
            sym = rng.choice(list(SYMS))
            newp = round(fake.marks[sym] * rng.choice([0.997, 1.003]), 2)
            kind = "fire"
            live = [(None, {"symbol": sym, "price": newp})]

        if kind == "restart":
            trace.append({"step": i, "kind": "restart"})
            cb, rb = await restart()
            errs = await settle()
            errs += check_global()
            if len(svc.trades.closed) != cb or abs(svc.trades.realized_total - rb) > 1e-9:
                errs.append(f"restart lost history: {cb}->{len(svc.trades.closed)} realized {rb}->"
                            f"{svc.trades.realized_total}")
            return errs

        if kind == "mult":
            f = rng.choice([1, 2, 3, 1.5, None])
            if f is None:
                svc.multiplier.configure({"enabled": False})
            else:
                svc.multiplier.configure({"enabled": True, "mode": "manual", "factor": f})
            trace.append({"step": i, "kind": "mult", "factor": f})
            return check_global()

        injected = None
        unknown_open = False
        if kind == "inject1001":
            fake.inject.extend([("order.place", -1001, "Internal error; unable to process your request.")] * 7)
            injected = "-1001x7"
            kind = rng.choice(["open", "close"]) if closable() else "open"
        elif kind == "inject2019":
            fake.inject.append(("order.place", -2019, "Margin is insufficient."))
            injected = "-2019"
            kind = "open"
        elif kind == "open_unknown":
            fake.inject_after.append(("order.place", -1007, LOST))
            fake.inject.extend([("order.status", -1001, "Internal error; unable to process your request.")] * 3)
            injected = "open:-1007+status-1001x3"
            unknown_open = True
            kind = "open"
        elif kind in ("close_lost", "close_lost_group"):
            dur = rng.choice([rng.uniform(0.3, 1.2), rng.uniform(2.0, 6.0), rng.uniform(8.0, 12.0)])
            if AVOID and kind == "close_lost_group":
                dur = min(dur, rng.uniform(2.0, 4.5))   # G7: a longer outage drops the other closes
            fake.inject_after.append(("order.place", -1007, LOST))
            injected = f"close:-1007+status down {dur:.1f}s"
            status_down["on"] = True

            async def back(dur=dur):
                await asyncio.sleep(dur)
                status_down["on"] = False
            status_down["task"] = asyncio.create_task(back())
            kind = "close" if kind == "close_lost" else "group_same"
        elif kind == "thin_close":
            thin["left"] = rng.randint(1, 3)
            injected = f"thin x{thin['left']}"
            kind = rng.choice(["close", "close"] + ([] if AVOID else ["close_all_sym"]))  # G5

        if kind == "fire":
            aid, a = rng.choice(live)
            sym = a["symbol"]
            newp = a.get("price") or float(a["triggerPrice"])
            trace.append({"step": i, "kind": "fire", "algo": aid, "owner": lk(*m.algo_owner[aid]) if aid in
                          m.algo_owner else None, "sym": sym, "price": newp,
                          "algos": {k: [v["type"], v["positionSide"], v["triggerPrice"], v.get("quantity")]
                                    for k, v in fake.algos.items()}})
            fake.set_mark(sym, newp)
            errs = await settle()
            fills = fill_log[log0:]
            apply_outside_fills(fills, m.owners)
            new_records = svc.trades.closed[n_closed0:]
            for r in new_records:
                if r.notes == "lote":
                    errs.append(f"I4 unexpected lot record on TP/SL fire tid={r.paper_trade_id} bot={r.bot!r} "
                                f"qty={r.quantity} status={r.status}")
            trace[-1]["fills"] = fills
            trace[-1]["records"] = [[r.paper_trade_id, r.bot, r.symbol, r.direction, r.quantity, r.status, r.notes]
                                    for r in new_records]
            trace[-1]["binance"] = {f"{s} {d}": pos(fake, s, d) for s in SYMS for d in DIRS if pos(fake, s, d)}
            trace[-1]["model"] = {f"{lk(b, tid)}@{s} {d}": round(q, 8) for (b, tid, s, d), q in m.owners.items()
                                  if q > 1e-12}
            return errs + check_global()

        if kind == "protect":
            ref = rng.choice(closable())
            b, tid = ref
            s, d = m.where[ref]
            mk = fake.marks[s]
            pk = rng.choice(["TP", "SL"])
            up, dn = round(mk * rng.choice([1.006, 1.012]), 2), round(mk * rng.choice([0.994, 0.988]), 2)
            trig = (up if d == "LONG" else dn) if pk == "TP" else (dn if d == "LONG" else up)
            sig = {"action": "open_tp" if pk == "TP" else "open_sl", "trade_id": tid, "symbol": s, "direction": d,
                   "trigger_price": trig}
            if BOTS[b]:
                sig["bot"] = BOTS[b]
            before = set(fake.algos)
            svc.trades.handle_signal(sig)
            errs = await settle()
            for aid in set(fake.algos) - before:
                m.algo_owner[aid] = ref
            for aid in list(m.algo_owner):
                if aid not in fake.algos:
                    m.algo_owner.pop(aid, None)
            trace.append({"step": i, "kind": "protect", "ref": lk(b, tid), "sym": s, "dir": d, "pk": pk,
                          "trigger": trig, "algos": {k: [v["type"], v["positionSide"], v["triggerPrice"],
                                                          v.get("quantity")] for k, v in fake.algos.items()}})
            return errs + check_global()

        owners_now = dict(m.owners)
        if kind == "open":
            ops.append(pick_open())
        elif kind == "around_open":
            o = pick_open(allow_prot=False)
            _, b, tid, s, d, q, prot = o
            seq[0] += 1
            n = int(round(pos0[(s, d)] / fstep(s)))
            if n >= 1 and rng.random() < 0.5:
                r = float(Decimal(rng.randint(1, n)) * SYMS[s])
                oo = ("outside", s, d, r)
            else:
                lo, hi = QRANGE[s]
                oo = ("outside_open", s, d, float(Decimal(rng.randint(lo, hi)) * SYMS[s]))
            cid = f"web_around{seq[0]}"
            hk = {"prefix": trading_mod.lot_cid_prefix("X", trading_mod.lot_ref(BOTS[o[1]], tid)) + "-", "fired": False}

            def fire_outside(oo=oo, cid=cid, hk=hk):
                hk["fired"] = True
                _, s2, d2, r2 = oo
                closing = oo[0] == "outside"
                side = ("SELL" if d2 == "LONG" else "BUY") if closing else ("BUY" if d2 == "LONG" else "SELL")
                orig_place({"symbol": s2, "side": side, "type": "MARKET", "positionSide": d2,
                            "quantity": str(Decimal(repr(r2))), "newClientOrderId": cid})
            hk["fn"] = fire_outside
            hooks.append(hk)
            ops.append(o)
            ops.append(("hooked", *oo, hk))
        elif kind == "close":
            ops.append(make_close(rng.choice(closable())))
        elif kind == "close_unknown":
            b = rng.randrange(4)
            if BOTS[b]:
                tid = m.new_tid(b)
            else:
                tid = m.unknown_counter
                m.unknown_counter += 1
            s, d = rng.choice(list(SYMS)), rng.choice(DIRS)
            if AVOID and pos0[(s, d)] > 0 and not m.lots(s, d):
                d = "LONG" if d == "SHORT" else "SHORT"   # legacy safety net (G?): position only of other bots
            if AVOID and pos0[(s, d)] > 0 and not m.lots(s, d):
                ops.append(pick_open())
                kind = "open"
            else:
                ops.append(("close", b, tid, s, d, None))
        elif kind == "close_all_sym":
            ops.append(("close_notid", rng.choice(list(SYMS)), rng.choice([*DIRS, None])))
        elif kind == "group_same":
            refs = closable()
            s0, d0 = m.where[rng.choice(refs)]
            same = [r for r in refs if m.where[r] == (s0, d0)]
            others = [r for r in refs if r not in same]
            chosen = rng.sample(same, min(len(same), rng.randint(1, 3)))
            if others and (len(chosen) < 2 or rng.random() < 0.3):
                chosen.append(rng.choice(others))
            for ref in chosen:
                ops.append(make_close(ref, allow_q=rng.random() < 0.5))
        elif kind in ("group", "group_restart"):
            refs = closable()
            chosen = rng.sample(refs, min(len(refs), rng.randint(2, 4)))
            for ref in chosen:
                ops.append(make_close(ref, allow_q=rng.random() < 0.7))
            if rng.random() < 0.5:
                o = pick_open(allow_prot=False)
                if (o[1], o[2]) not in chosen:
                    ops.insert(rng.randrange(len(ops) + 1), o)
            if rng.random() < 0.4 and kind == "group":
                s, d = m.where[chosen[0]]
                n = int(round(pos0[(s, d)] / fstep(s)))
                if n >= 2:
                    ops.insert(0, ("outside", s, d, float(Decimal(rng.randint(1, n - 1)) * SYMS[s])))
        elif kind == "outside":
            cands = [(s, d) for s in SYMS for d in DIRS if pos0[(s, d)] > 0]
            if not cands:
                ops.append(pick_open())
                kind = "open"
            else:
                s, d = rng.choice(cands)
                n = int(round(pos0[(s, d)] / fstep(s)))
                k = n if rng.random() < 0.15 else rng.randint(1, max(1, n - 1))
                ops.append(("outside", s, d, float(Decimal(k) * SYMS[s])))
        elif kind == "outside_open":
            s, d = rng.choice(list(SYMS)), rng.choice(DIRS)
            if rng.random() < 0.7 and closable():
                s, d = m.where[rng.choice(closable())]
            lo, hi = QRANGE[s]
            ops.append(("outside_open", s, d, float(Decimal(rng.randint(lo, hi)) * SYMS[s])))

        trace.append({"step": i, "kind": kind, "injected": injected, "lag": lag_mode,
                      "ops": [[x for x in op if not isinstance(x, dict) or "prefix" not in x] for op in ops]})

        # ── expectations: outside ops of the group happen first (dispatch order) ──
        sim = dict(owners_now)
        sim_assumed = dict(m.assumed)
        sim_pos = dict(pos0)
        for op in ops:
            if op[0] == "outside":
                _, s, d, r = op
                apply_outside_fills([{"cid": "web", "sym": s, "ps": d, "side": "SELL" if d == "LONG" else "BUY",
                                      "qty": r, "before": sim_pos[(s, d)]}], sim, sim_assumed)
                sim_pos[(s, d)] = max(0.0, sim_pos[(s, d)] - r)
                expected_red[(s, d)] = expected_red.get((s, d), 0.0) + r
                tol_mult[(s, d)] = tol_mult.get((s, d), 0) + 1
            elif op[0] == "outside_open":
                _, s, d, q = op
                apply_outside_fills([{"cid": "web", "sym": s, "ps": d, "side": "BUY" if d == "LONG" else "SELL",
                                      "qty": q, "before": sim_pos[(s, d)]}], sim, sim_assumed)
                sim_pos[(s, d)] += q
                expected_red[(s, d)] = expected_red.get((s, d), 0.0) - q
        close_expect = []
        for op in ops:
            if op[0] == "close":
                red, amount, info = expect_close(op, sim)
                for k, v in red.items():
                    expected_red[k] = expected_red.get(k, 0.0) + v
                    tol_mult[k] = tol_mult.get(k, 0) + 1
                close_expect.append((op, amount, info))
            elif op[0] == "close_notid":
                _, s, dsel = op
                for d in DIRS:
                    if dsel and d != dsel:
                        continue
                    if sim_pos[(s, d)] > 0:
                        expected_red[(s, d)] = expected_red.get((s, d), 0.0) + sim_pos[(s, d)]
                        tol_mult[(s, d)] = tol_mult.get((s, d), 0) + 1

        # ── dispatch ──
        t = svc.trades
        lag_gap = AVOID and lag_mode != "none" and any(op[0] == "outside" for op in ops) and \
            any(op[0] == "close" for op in ops)
        for n_op, op in enumerate(ops):
            if lag_gap and op[0] != "outside" and n_op > 0 and ops[n_op - 1][0] == "outside":
                await asyncio.sleep(0.7)   # G3: closes only after the outside fill's user-stream event
            if op[0] == "open":
                _, b, tid, s, d, q, prot = op
                t.handle_signal(open_sig(b, tid, s, d, q, prot))
            elif op[0] == "close":
                _, b, tid, s, d, q = op
                t.handle_signal(close_sig(b, tid, s, d, q))
            elif op[0] == "close_notid":
                _, s, d = op
                sig = {"action": "close", "symbol": s, "reason": "MAINCLOSE"}
                if d:
                    sig["direction"] = d
                t.handle_signal(sig)
            elif op[0] in ("outside", "outside_open"):
                _, s, d, r = op
                seq[0] += 1
                closing = op[0] == "outside"
                side = ("SELL" if d == "LONG" else "BUY") if closing else ("BUY" if d == "LONG" else "SELL")
                fake._place({"symbol": s, "side": side, "type": "MARKET", "positionSide": d,
                             "quantity": str(Decimal(repr(r))), "newClientOrderId": f"web_out{seq[0]}"})
        if kind == "group_restart":
            await asyncio.sleep(rng.choice([0, 0.05, 0.3]))
            cb, rb = await restart()
        errs = await settle()
        thin["left"] = 0
        fake.inject.clear()
        fake.inject_after.clear()
        hooks.clear()

        t = svc.trades
        new_records = t.closed[n_closed0:]
        fills = fill_log[log0:]

        # ── model: replay Binance fills of this step in order (outside first-class) ──
        # opens of the executor: attribute by clientOrderId prefix X<tid>-
        open_ops = [op for op in ops if op[0] == "open"]
        assumed_now = dict(m.assumed)
        for f in fills:
            cid = f["cid"]
            if cid.startswith("X"):
                continue
            if cid.startswith(("C", "K")):
                continue
            apply_outside_fills([f], m.owners)
        # executor opens (their position in the order matters only for outside reductions around them, which the
        # executor excludes by fill time; the model applied the outside fills above first only when they happened
        # first, so re-run in true order instead)
        if any(op[0] == "hooked" for op in ops) or open_ops:
            m.owners = dict(owners_now)
            m.assumed.clear()
            m.assumed.update(assumed_now)  # the outside opens of the first pass also realized assumed lots
            for f in fills:
                cid = f["cid"]
                if cid.startswith(("C", "K")):
                    continue
                if cid.startswith("X"):
                    for op in open_ops:
                        _, b, tid, s, d, q, prot = op
                        if is_open_of(cid, b, tid) and f["sym"] == s and f["ps"] == d and \
                                f["side"] == ("BUY" if d == "LONG" else "SELL"):
                            key = (b, tid, s, d)
                            m.owners[key] = m.owners.get(key, 0.0) + f["qty"]
                            break
                    continue
                apply_outside_fills([f], m.owners)
        for op in open_ops:
            _, b, tid, s, d, q, prot = op
            ref = (b, tid)
            got = sum(f["qty"] for f in fills if is_open_of(f["cid"], b, tid) and f["sym"] == s and f["ps"] == d
                      and f["side"] == ("BUY" if d == "LONG" else "SELL"))
            m.where[ref] = (s, d)
            m.alive.add(ref)
            key = (b, tid, s, d)
            if got > 0:
                m.real_sum[ref] = m.real_sum.get(ref, 0.0) + got
                m.bot_sum[ref] = m.bot_sum.get(ref, 0.0) + q
                expected_red[(s, d)] = expected_red.get((s, d), 0.0) - got
                tol_mult[(s, d)] = tol_mult.get((s, d), 0) + 1
                if prot:
                    for aid, a in fake.algos.items():
                        if a["symbol"] == s and a["positionSide"] == d and aid not in m.algo_owner:
                            m.algo_owner[aid] = ref
            else:
                tr = t.get_trade(s, d)
                a = tr.assumed_lots.get(lk(b, tid), 0.0) if tr else 0.0
                if a > m.assumed.get(key, 0.0):
                    m.assumed[key] = a
                if prot and m.owners.get(key, 0.0) > 0:  # assumed tranche of a lot with real qty: its TP/SL is placed
                    for aid, al in fake.algos.items():
                        if al["symbol"] == s and al["positionSide"] == d and aid not in m.algo_owner:
                            m.algo_owner[aid] = ref
            trace[-1].setdefault("open_fills", []).append([lk(b, tid), s, d, got])
        for op in ops:
            if op[0] == "hooked":
                _, kind_h, s, d, r, hk = op
                if not hk["fired"]:
                    continue
                if kind_h == "outside":
                    expected_red[(s, d)] = expected_red.get((s, d), 0.0) + r
                    tol_mult[(s, d)] = tol_mult.get((s, d), 0) + 1
                else:
                    expected_red[(s, d)] = expected_red.get((s, d), 0.0) - r

        for s in SYMS:
            for d in DIRS:
                real_red = pos0[(s, d)] - pos(fake, s, d)
                exp_red = expected_red.get((s, d), 0.0)
                n = max(1, tol_mult.get((s, d), 0))
                if abs(real_red - exp_red) > fstep(s) * 1.01 * n:
                    errs.append(f"I2 {s} {d}: Binance changed by {-real_red:+.6f}, expected {-exp_red:+.6f}")

        # closes (executor): expected amounts computed above on ``sim`` (state after the group's outside ops)
        matched = set()
        cur = sim
        for op, amount, info in close_expect:
            _, b, tid, s, d, q = op
            ref = (b, tid)
            recs = [r for r in new_records if r.paper_trade_id == tid and r.status == CLOSE_REASON
                    and (r.notes != "lote" or (r.bot or "") == BOTS[b])]
            for r in recs:
                matched.add(id(r))
            got_q = sum(r.quantity for r in recs)
            if info is None:
                if recs:
                    errs.append(f"I4 close {lk(b, tid)} expected no-op but records "
                                f"{[(r.quantity, r.symbol, r.direction) for r in recs]}")
                continue
            if info[0] == "assumed":
                k = (b, tid, *m.where[ref])
                m.assumed[k] = 0.0
                m.dead.add(ref)
                m.closed_by_signal.add(ref)
                if not any(r.order_assumed for r in recs):
                    errs.append(f"I4 assumed close {lk(b, tid)}: no order_assumed record "
                                f"(records {[(r.quantity, r.order_assumed) for r in recs]})")
                continue
            whole, amt, s2, d2 = info
            key = (b, tid, s2, d2)
            step = fstep(s2)
            if abs(got_q - amt) > step * 1.01:
                errs.append(f"I4 close {lk(b, tid)} {s2} {d2}: records qty {got_q} != expected {amt:.6f} "
                            f"(n={len(recs)}, signals={[(x['ok'], x['detail']) for x in t.signals(3, trade_id=tid)]})")
            if whole == "whole":
                if amt > step / 2:      # G8: a dust lot's close is a no-op and keeps its TP/SL
                    m.closed_by_signal.add(ref)
                m.owners[key] = 0.0
                m.assumed.pop(key, None)
                m.dead.add(ref)
            else:
                m.owners[key] = max(0.0, m.owners.get(key, 0.0) - amt)
                if m.owners[key] <= step / 2:
                    m.owners[key] = 0.0
                    m.dead.add(ref)
                    m.closed_by_signal.add(ref)
        for op in ops:
            if op[0] == "close_notid":
                _, s, dsel = op
                for d in DIRS:
                    if dsel and d != dsel:
                        continue
                    for k in list(m.owners):
                        if k[2] == s and k[3] == d:
                            m.owners[k] = 0.0
                    for k in list(m.assumed):
                        if k[2] == s and k[3] == d:
                            m.assumed[k] = 0.0
        for r in new_records:
            if id(r) in matched:
                continue
            closer = any(op[0] in ("close_notid", "outside", "hooked") for op in ops)
            if r.notes == "lote" and not closer:
                errs.append(f"I4 unexpected lot record tid={r.paper_trade_id} bot={r.bot!r} qty={r.quantity} "
                            f"{r.symbol} {r.direction} status={r.status}")
        for aid in list(m.algo_owner):
            if aid not in fake.algos:
                m.algo_owner.pop(aid, None)
        trace[-1]["fills"] = fills
        trace[-1]["records"] = [[r.paper_trade_id, r.bot, r.symbol, r.direction, r.quantity, r.status, r.notes,
                                 round(r.pnl_usdt, 6)] for r in new_records]
        trace[-1]["binance"] = {f"{s} {d}": pos(fake, s, d) for s in SYMS for d in DIRS if pos(fake, s, d)}
        trace[-1]["model"] = {f"{lk(b, tid)}@{s} {d}": round(q, 8) for (b, tid, s, d), q in m.owners.items()
                              if q > 1e-12}
        trace[-1]["exec_lots"] = {f"{tr.symbol} {tr.direction}": [dict(tr.lots), dict(tr.assumed_lots), tr.quantity]
                                  for tr in t.trades.values()}
        if kind == "group_restart" and len(t.closed) < cb:
            errs.append("group_restart lost history")
        return errs + check_global()

    try:
        for i in range(n_steps):
            errs = await run_step(i)
            if stateless_done[0]:  # records / PnL of the previous session are gone with trades.json
                errs = [e for e in errs if not e.startswith(("I4", "I5", "restart lost", "group_restart lost"))]
            if errs:
                result["violations"].append({"step": i, "errors": errs})
                break
    finally:
        try:
            await svc.stop()
        finally:
            if OUT is not None:
                (OUT / f"{seed}.json").write_text(json.dumps(result, indent=1, default=str))
    assert not result["violations"], json.dumps(result["violations"], indent=1)
