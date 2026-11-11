"""Tareas en segundo plano que nadie espera.

``asyncio`` solo guarda una referencia débil a cada tarea: una lanzada con ``asyncio.create_task`` sin guardar
su resultado puede ser recogida por el recolector de basura a mitad de su trabajo, y si falla, su error solo
aparece al destruirla. ``spawn`` la mantiene viva hasta que termina y deja su error en el log.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Coroutine

log = logging.getLogger("executor.tasks")

_background: set[asyncio.Task] = set()


def _done(task: asyncio.Task) -> None:
    _background.discard(task)
    if not task.cancelled() and task.exception() is not None:
        log.error("Tarea en segundo plano %s falló", task.get_name(), exc_info=task.exception())


def spawn(coro: Coroutine, name: str = "") -> asyncio.Task:
    """Lanza ``coro`` en segundo plano, con referencia fuerte y registro de su error."""
    task = asyncio.create_task(coro, name=name or getattr(coro, "__qualname__", None))
    _background.add(task)
    task.add_done_callback(_done)
    return task
