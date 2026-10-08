"""Persistencia JSON atómica con guardado diferido (debounce)."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Callable, Optional

log = logging.getLogger("executor.storage")


class JsonStore:
    def __init__(self, path: Path, producer: Callable[[], Any], delay_s: float = 1.0):
        self.path = Path(path)
        self._producer = producer
        self._delay = delay_s
        self._task: Optional[asyncio.Task] = None

    def load(self, default: Any = None) -> Any:
        if not self.path.exists():
            return default
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception as exc:
            log.error("No se pudo leer %s (%s); se ignora", self.path, exc)
            return default

    def save_now(self) -> None:
        try:
            data = self._producer()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(prefix=self.path.name, dir=str(self.path.parent))
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    json.dump(data, fh, ensure_ascii=False, indent=1, default=str)
                os.replace(tmp, self.path)
            finally:
                if os.path.exists(tmp):
                    os.unlink(tmp)
        except Exception as exc:
            log.error("No se pudo guardar %s: %s", self.path, exc)

    def schedule(self) -> None:
        """Agenda un guardado; varias llamadas seguidas se agrupan en una."""
        if self._task is not None and not self._task.done():
            return
        try:
            self._task = asyncio.get_running_loop().create_task(self._delayed())
        except RuntimeError:
            self.save_now()

    async def _delayed(self) -> None:
        await asyncio.sleep(self._delay)
        self.save_now()
