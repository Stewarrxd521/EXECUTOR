"""
Futures Executor — punto de entrada.

    python futures_executor.py

Recibe señales de app.py (POST /signal o WebSocket /ws/signal) y las ejecuta
en Binance USDⓈ-M Futures; incluye bots Grid y un dashboard estilo Binance
en tiempo real (GET /). Toda la lógica vive en el paquete ``executor/``.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from aiohttp import web

from executor import __version__
from executor.config import configure_logging, load_settings
from executor.service import ExecutorService
from executor.web.server import build_app

log = logging.getLogger("executor")


async def main() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    log.info("══════════════════════════════════════════════════════")
    log.info(" Futures Executor v%s — Binance USDⓈ-M [%s]", __version__, settings.env_label)
    log.info(" Órdenes/cuenta: WebSocket API · Precios: stream global")
    log.info("══════════════════════════════════════════════════════")

    service = ExecutorService(settings)
    app = build_app(service)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", settings.port)
    await site.start()
    log.info("Dashboard y API en http://0.0.0.0:%d", settings.port)

    await service.start()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):  # Windows
            pass
    await stop.wait()
    log.info("Deteniendo executor…")
    await service.stop()
    await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
