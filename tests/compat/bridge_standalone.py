# ExecutorBridge tal cual está desplegado (executor_bridge_con_ejemplo_stewar.py, sin cambios).
# Las pruebas lo usan contra el executor real para garantizar compatibilidad.
# flake8: noqa
from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass
class ExecutorSignalConfig:
    executor_url: str = ""
    signal_secret: str = "clave-secreta-aleatoria"
    poll_secs: int = 5
    timeout_signal: int = 8
    timeout_state: int = 8


class ExecutorBridge:
    """
    Clase independiente para hablar con el Executor.

    Funciones:
    - Enviar señales de apertura/cierre.
    - Consultar /api/state del Executor.
    - Ejecutar polling periódico sin bloquear el loop principal.
    - Exponer helpers específicos para open/close.
    """

    def __init__(
        self,
        executor_url: str = "",
        signal_secret: str = "clave-secreta-aleatoria",
        poll_secs: int = 5,
        logger: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.config = ExecutorSignalConfig(
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

    def _build_signal_request(self, payload: dict[str, Any]) -> urllib.request.Request:
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

    def send_signal_sync(self, payload: dict[str, Any]) -> None:
        """
        Envía una señal al Executor.
        No lanza la excepción hacia arriba: solo registra el error.
        """
        if not self.config.executor_url:
            return

        try:
            req = self._build_signal_request(payload)
            with urllib.request.urlopen(req, timeout=self.config.timeout_signal) as resp:
                resp.read()
        except Exception as exc:
            self._log(
                f"[executor-signal] error enviando {payload.get('action')} "
                f"{payload.get('symbol')}: {exc}"
            )

    def fetch_state_sync(self) -> Optional[dict[str, Any]]:
        """Lee /api/state del Executor."""
        if not self.config.executor_url:
            return None

        try:
            req = urllib.request.Request(
                f"{self.config.executor_url}/api/state",
                method="GET",
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
    ) -> None:
        """Conveniencia para notificar aperturas."""
        self.send_signal_sync(
            {
                "action": "open",
                "trade_id": trade_id,
                "symbol": symbol,
                "direction": direction,
                "price": price,
                "quantity": quantity,
            }
        )

    def notify_close(
        self,
        trade_id: int,
        symbol: str,
        direction: str,
        reason: str,
        close_price: float,
    ) -> None:
        """Conveniencia para notificar cierres."""
        self.send_signal_sync(
            {
                "action": "close",
                "trade_id": trade_id,
                "symbol": symbol,
                "direction": direction,
                "reason": reason,
                "close_price": close_price,
            }
        )

    def notify_async(self, payload: dict[str, Any]) -> None:
        """
        Dispara el envío sin bloquear.
        Si no existe un loop activo, no hace nada.
        """
        if not self.config.executor_url:
            return
        try:
            asyncio.create_task(asyncio.to_thread(self.send_signal_sync, payload))
        except RuntimeError:
            # Ocurre si no hay loop asyncio activo.
            pass

    async def poll_state_loop(
        self,
        running: Callable[[], bool],
        on_state: Callable[[dict[str, Any]], None],
    ) -> None:
        """
        Loop de polling para consultar el estado del Executor.
        running(): debe devolver True mientras el bot siga activo.
        on_state(): callback para actualizar el estado compartido.
        """
        if not self.config.executor_url:
            self._log("[executor] EXECUTOR_URL no configurado — PnL real no disponible")
            return

        self._log(f"[executor] Polling de estado activo → {self.config.executor_url}")

        while running():
            try:
                data = await asyncio.to_thread(self.fetch_state_sync)
                if data is not None:
                    on_state(data)
            except asyncio.CancelledError:
                if not running():
                    break
            except Exception as exc:
                self._log(f"[executor] Error consultando estado: {exc}")

            try:
                await asyncio.sleep(self.config.poll_secs)
            except asyncio.CancelledError:
                if not running():
                    break


# =========================
# EJEMPLO DE USO COMPLETO
# =========================

if __name__ == "__main__":
    # 1) Crear el bridge con tu URL y tu secreto
    bridge = ExecutorBridge(
        executor_url=  "https://executorlong.onrender.com/", #"https://executor-5lu0.onrender.com",  #
        signal_secret="clave-secreta-aleatoria",
        poll_secs=5,
    )

    # 2) Enviar una apertura manualmente
    bridge.notify_open(
        trade_id=1,
        symbol="ZAMAUSDT",
        direction="LONG",
        price=1,
        quantity=5,
    )


    # 7) Ejemplo de polling continuo del Executor
    #    (solo para demostrar el uso del método async).
    async def demo_poll() -> None:
        running_flag = {"on": True}

        def running() -> bool:
            return running_flag["on"]

        def on_state(data: dict[str, Any]) -> None:
            print("Callback estado:", data)

        task = asyncio.create_task(bridge.poll_state_loop(running, on_state))
        await asyncio.sleep(10)
        running_flag["on"] = False
        await task

    # Descomenta para probar el polling real:
    # asyncio.run(demo_poll())
