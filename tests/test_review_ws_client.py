"""Reproducciones de la revisión adversarial (ronda 6): lo que no debe volver a pasar."""
import asyncio
from executor_ws_signal import ExecutorSignalSocket
from tests.conftest import wait_for
from tests.test_api import SECRET
from tests.test_lots import SYM, amt


async def test_ws_reply_timeout_then_post_does_not_double_open(open_service, live_url, fake):
    sock = await asyncio.to_thread(ExecutorSignalSocket, live_url, SECRET, logger=lambda m: None)
    try:
        await asyncio.to_thread(sock._connected.wait, 3)
        sock.timeout = 0.0005                    # reply "lost": falls back to POST with the same signal_id
        reply = await asyncio.to_thread(sock.send, {"action": "open", "trade_id": 7, "symbol": SYM,
                                                    "direction": "LONG", "price": 3000, "quantity": 0.01})
        await asyncio.sleep(1.0)
        assert amt(fake) == 0.01
    finally:
        await asyncio.to_thread(sock.close)
