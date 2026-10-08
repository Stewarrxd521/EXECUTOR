"""ExecutorBridge tal cual está desplegado en app_25.py (líneas 30-172, sin cambios).

Las pruebas lo usan contra el executor real para garantizar compatibilidad.
"""
# flake8: noqa
from __future__ import annotations
import asyncio
import json
import threading
from typing import Optional


# ── ExecutorBridge (señales al Executor externo) ─────────────────────────────
import urllib.error
import urllib.request
from dataclasses import dataclass as _dataclass_eb


@_dataclass_eb
class _ExecutorSignalConfig:
    executor_url:   str = ""
    signal_secret:   str = "clave-secreta-aleatoria"
    poll_secs:       int = 5
    timeout_signal:  int = 8
    timeout_state:   int = 8


class ExecutorBridge:
    """Envía señales de apertura/cierre al Executor y consulta su estado."""

    def __init__(
        self,
        executor_url: str = "",
        signal_secret: str = "clave-secreta-aleatoria",
        poll_secs: int = 5,
        logger=None,
    ) -> None:
        self.config = _ExecutorSignalConfig(
            executor_url=executor_url.strip().rstrip("/"),
            signal_secret=signal_secret,
            poll_secs=int(poll_secs),
        )
        self.logger = logger or print

    def _log(self, message: str) -> None:
        try:
            self.logger(message)
        except Exception:
            pass

    def _build_signal_request(self, payload: dict) -> urllib.request.Request:
        body = json.dumps(payload).encode("utf-8")
        return urllib.request.Request(
            f"{self.config.executor_url}/signal",
            data=body,
            headers={
                "Content-Type": "application/json",
                "X-Signal-Secret": self.config.signal_secret,
            },
            method="POST",
        )

    def send_signal_sync(self, payload: dict) -> None:
        """Envía una señal al Executor. No lanza excepción: solo registra el error."""
        if not self.config.executor_url:
            return
        try:
            req = self._build_signal_request(payload)
            with urllib.request.urlopen(req, timeout=self.config.timeout_signal) as resp:
                resp.read()
                self._log(
                    f"[executor] ✓ señal enviada: {payload.get('action')} {payload.get('symbol')}"
                )
        except Exception as exc:
            self._log(
                f"[executor] error enviando {payload.get('action')} "
                f"{payload.get('symbol')}: {exc}"
            )

    async def send_signal_async(self, payload: dict) -> None:
        """Versión no bloqueante para usar desde el event loop."""
        await asyncio.to_thread(self.send_signal_sync, payload)

    def fetch_state_sync(self) -> Optional[dict]:
        """Lee /api/state del Executor."""
        if not self.config.executor_url:
            return None
        try:
            req = urllib.request.Request(
                f"{self.config.executor_url}/api/state", method="GET"
            )
            with urllib.request.urlopen(req, timeout=self.config.timeout_state) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception:
            return None

    def notify_open(
        self,
        trade_id: int,
        symbol: str,
        direction: str,
        price: float,
        quantity: float,
        notional: float = 0.0,
        level: float = 0.0,
    ) -> None:
        """Notifica apertura de posición al Executor sin bloquear el loop."""
        payload = {
            "action":    "open",
            "trade_id":  trade_id,
            "symbol":    symbol,
            "direction": direction,
            "price":     price,
            "quantity":  quantity,
            "notional":  notional,
            "level":     level,
        }
        self.notify_async(payload)

    def notify_close(
        self,
        trade_id: int,
        symbol: str,
        direction: str,
        reason: str,
        close_price: float,
        pnl: float = 0.0,
    ) -> None:
        """Notifica cierre de posición al Executor sin bloquear el loop."""
        payload = {
            "action":      "close",
            "trade_id":    trade_id,
            "symbol":      symbol,
            "direction":   direction,
            "reason":      reason,
            "close_price": close_price,
            "pnl":         pnl,
        }
        self.notify_async(payload)

    def notify_async(self, payload: dict) -> None:
        """Dispara el envío sin bloquear el event loop."""
        if not self.config.executor_url:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            threading.Thread(
                target=self.send_signal_sync,
                args=(payload,),
                daemon=True,
            ).start()
            return
        loop.create_task(self.send_signal_async(payload))
