"""executor_ws_signal.ExecutorSignalSocket: los bots envían por /ws/signal (con POST de respaldo)."""

import asyncio

from executor_ws_signal import ExecutorSignalSocket
from tests.conftest import wait_for
from tests.test_api import SECRET
from tests.test_lots import SYM, amt


def sig(action, tid, **extra):
    return {"action": action, "trade_id": tid, "symbol": SYM, "direction": "LONG", "price": 3000,
            "quantity": 0.01, "reason": "TP", **extra}


async def test_bot_signals_go_over_the_websocket(open_service, live_url, fake):
    sock = await asyncio.to_thread(ExecutorSignalSocket, live_url, SECRET, logger=lambda m: None)
    try:
        reply = await asyncio.to_thread(sock.send, sig("open", 1))
        assert reply["ok"] and sock.sent_ws == 1 and sock.sent_http == 0
        await wait_for(lambda: amt(fake) == 0.01)
        again = await asyncio.to_thread(sock.send, sig("open", 1, signal_id=reply["signal_id"]))
        assert again.get("duplicate")                    # mismo signal_id: no se ejecuta dos veces
        reply = await asyncio.to_thread(sock.send, sig("close", 1))
        assert reply["ok"] and sock.sent_ws == 3
        await wait_for(lambda: amt(fake) == 0)
    finally:
        await asyncio.to_thread(sock.close)


async def test_bot_identity_in_the_url_reaches_the_websocket(open_service, live_url, fake):
    sock = await asyncio.to_thread(ExecutorSignalSocket, live_url + "/b/kline", SECRET, logger=lambda m: None)
    try:
        await asyncio.to_thread(sock.send, sig("open", 5))
        await wait_for(lambda: open_service.trades.get_trade(SYM, "LONG") is not None
                       and "kline:5" in open_service.trades.get_trade(SYM, "LONG").lots)
    finally:
        await asyncio.to_thread(sock.close)


async def test_without_websocket_the_signal_goes_by_post(open_service, live_url, fake):
    sock = ExecutorSignalSocket(live_url, SECRET, logger=lambda m: None, connect=False)
    reply = await asyncio.to_thread(sock.send, sig("open", 2))
    assert reply["ok"] and sock.sent_http == 1 and sock.sent_ws == 0
    await wait_for(lambda: amt(fake) == 0.01)


async def test_wrong_secret_is_reported_not_retried_by_post(open_service, live_url, fake):
    sock = await asyncio.to_thread(ExecutorSignalSocket, live_url, "bad", logger=lambda m: None)
    try:
        reply = await asyncio.to_thread(sock.send, sig("open", 3))
        assert not reply["ok"] and reply.get("status") == 401 and sock.sent_http == 0
    finally:
        await asyncio.to_thread(sock.close)
