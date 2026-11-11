"""Reproducciones de la revisión adversarial (ronda 6): lo que no debe volver a pasar."""
import asyncio
import time


from executor.account import AccountState
from executor.service import ExecutorService
from tests.conftest import make_settings, wait_for


def _row(sym, ps, amt, entry=100.0):
    return {"symbol": sym, "positionSide": ps, "positionAmt": str(amt), "entryPrice": str(entry),
            "leverage": "5", "marginType": "cross"}


def test_snapshot_resurrects_position_closed_while_in_flight():
    """sync_account: snapshot pedido en t0, el user stream cierra la posición (pa=0) antes de que llegue
    la respuesta (vieja, aún con la posición). La posición cerrada vuelve como fantasma."""
    acct = AccountState(hedge_hint=True)
    acct.apply_positions_snapshot([_row("SOLUSDT", "LONG", 2)])
    asked = time.time()
    time.sleep(0.01)
    acct.apply_account_update({"e": "ACCOUNT_UPDATE", "a": {"P": [
        {"s": "SOLUSDT", "ps": "LONG", "pa": "0", "ep": "0"}]}})
    assert acct.position("SOLUSDT", "LONG") is None
    # La respuesta del snapshot (generada antes del cierre) llega ahora.
    acct.apply_positions_snapshot([_row("SOLUSDT", "LONG", 2)], as_of=asked)
    assert acct.position("SOLUSDT", "LONG") is None, "posición cerrada resucitada por un snapshot viejo"


async def test_sync_during_close_leaves_ghost_position(fake, tmp_path, snapshot_file):
    """Reconexión del user stream → sync_account. Mientras viaja account.position, una señal close cierra
    la posición. La respuesta vieja (con la posición) se aplica después: queda una posición fantasma que
    nadie corrige (el verify loop solo corre con el user stream caído) y que la siguiente apertura
    acredita como «sin dueño» en el libro de lotes."""
    svc = ExecutorService(make_settings(fake, tmp_path))
    await svc.start()
    try:
        await wait_for(lambda: svc.user.conn.connected and svc.trades.ready.is_set())
        await asyncio.sleep(2.5)
        st, body = await svc.trades.submit_signal({"action": "open", "symbol": "TESTUSDT", "direction": "LONG",
                                                   "trade_id": 1, "quantity": 20, "price": 0.5}, wait=True)
        assert body["ok"], body
        await wait_for(lambda: svc.core.account.position("TESTUSDT", "LONG") is not None)
        orig = svc.core.ws.positions
        gate = asyncio.Event()

        async def slow_positions(symbol=None):
            rows = await orig(symbol)
            if symbol is None:
                await gate.wait()  # la respuesta (vieja) llega tarde
            return rows

        svc.core.ws.positions = slow_positions
        sync = asyncio.create_task(svc.sync_account("user stream conectado"))
        await asyncio.sleep(0.1)
        st, body = await svc.trades.submit_signal({"action": "close", "symbol": "TESTUSDT", "direction": "LONG",
                                                   "trade_id": 1}, wait=True)
        assert body["ok"], body
        await wait_for(lambda: svc.core.account.position("TESTUSDT", "LONG") is None)
        await wait_for(lambda: svc.trades.get_trade("TESTUSDT", "LONG") is None)
        gate.set()
        await sync
        svc.core.ws.positions = orig
        await asyncio.sleep(3)
        ghost = svc.core.account.position("TESTUSDT", "LONG")
        st, body = await svc.trades.submit_signal({"action": "open", "symbol": "TESTUSDT", "direction": "LONG",
                                                   "trade_id": 2, "quantity": 20, "price": 0.5}, wait=True)
        await asyncio.sleep(1)
        trade = svc.trades.get_trade("TESTUSDT", "LONG")
        st, body = await svc.trades.submit_signal({"action": "close", "symbol": "TESTUSDT", "direction": "LONG",
                                                   "trade_id": 2}, wait=True)
        await asyncio.sleep(8)
        trade = svc.trades.get_trade("TESTUSDT", "LONG")
        assert ghost is None, f"posición fantasma: {ghost}"
    finally:
        await svc.stop()


async def test_exinfo_loop_dies_after_one_failure(fake, tmp_path, snapshot_file, monkeypatch):
    svc = ExecutorService(make_settings(fake, tmp_path, exchange_info_refresh_h=0.0001))
    calls = {"n": 0}

    async def refresh(reason):
        calls["n"] += 1
        if calls["n"] == 1:
            from executor.errors import BinanceAPIError
            raise BinanceAPIError(-1, "red caída")
        return {}

    svc.refresh_exchange_info = refresh
    svc.core.exinfo.age_hours = lambda: 99
    real_sleep = asyncio.sleep
    monkeypatch.setattr("executor.service.asyncio.sleep", lambda s: real_sleep(0.01))
    task = asyncio.create_task(svc._exinfo_loop())
    await real_sleep(0.3)
    alive = not task.done()
    task.cancel()
    assert alive and calls["n"] > 1, f"loop muerto tras {calls['n']} intento(s): {task.exception() if task.done() and not task.cancelled() else ''}"




async def test_signal_socket_retry_after_drop_gets_new_signal_id():
    """executor_client.SignalSocket.send: el signal_id se genera en una copia; si la conexión cae tras
    enviar, el reintento del llamador (mismo payload) lleva OTRO signal_id → el executor lo ejecuta dos veces."""
    import executor_client as ec
    sent = []

    class Conn:
        async def send(self, msg):
            sent.append(dict(msg))
            raise ConnectionError("caída tras enviar")

    sock = ec.SignalSocket("http://x", "s", timeout=0.2)

    async def fake_connect():
        sock._conn = Conn()
        sock._reader = asyncio.create_task(asyncio.sleep(10))
        return sock

    sock.connect = fake_connect
    payload = {"action": "open", "trade_id": 1, "symbol": "TESTUSDT", "direction": "LONG", "quantity": 20}
    for _ in range(2):
        try:
            await sock.send(payload)
        except ConnectionError:
            pass
    sock._reader.cancel()
    assert sent[0]["signal_id"] == sent[1]["signal_id"], (sent[0]["signal_id"], sent[1]["signal_id"])


def test_unreadable_store_is_overwritten_without_backup(tmp_path):
    """trades.json ilegible (escritura cortada, disco lleno): load() devuelve {} y el primer guardado lo
    pisa; el historial y los lotes guardados se pierden sin copia."""
    from executor.storage import JsonStore
    path = tmp_path / "trades.json"
    original = '{"open": [{"symbol": "SOLUSDT"}], "closed": [1, 2, 3]'  # JSON truncado
    path.write_text(original)
    store = JsonStore(path, lambda: {"open": [], "closed": []})
    assert store.load({}) == {}
    store.save_now()
    leftovers = [p.name for p in tmp_path.iterdir() if p.name != "trades.json"]
    assert leftovers or path.read_text() == original, "archivo original perdido sin copia"


async def test_ws_signal_client_drops_close_during_restart_window(open_service, live_url, fake):
    """app.py usa ExecutorSignalSocket por defecto. Durante un reinicio/deploy el executor responde 503
    «reintenta en unos segundos» (y la caída da error de conexión): el cliente cae a POST una vez, recibe 503
    y devuelve ok:false SIN reintentar. Un close enviado en esa ventana se pierde: la posición queda abierta
    en Binance aunque el bot la dé por cerrada (ExecutorBridge sí reintenta 503 con el mismo signal_id)."""
    from executor_ws_signal import ExecutorSignalSocket
    from tests.test_api import SECRET
    from tests.test_lots import SYM, amt
    base = {"trade_id": 1, "symbol": SYM, "direction": "LONG", "price": 3000, "quantity": 0.01}
    sock = await asyncio.to_thread(ExecutorSignalSocket, live_url, SECRET, logger=lambda m: None)
    try:
        reply = await asyncio.to_thread(sock.send, {"action": "open", **base})
        assert reply["ok"]
        await wait_for(lambda: amt(fake) == 0.01)
        open_service.trades.accepting = False  # executor reiniciándose (drain)
        asyncio.get_running_loop().call_later(1.0, setattr, open_service.trades, "accepting", True)
        reply = await asyncio.to_thread(sock.send, {"action": "close", "reason": "TP", **base})
        await asyncio.sleep(3)
        assert amt(fake) == 0, f"close perdido: posición sigue abierta ({amt(fake)}); respuesta {reply}"
    finally:
        await asyncio.to_thread(sock.close)
