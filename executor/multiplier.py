"""Multiplicador de operaciones.

Escala el tamaño (``quantity``, ``notional`` y ``margin``) de cada señal ``open``
que llega de los bots. Los cierres parciales se escalan con el mismo factor que
recibió la operación; los cierres totales cierran la posición real completa.

Modos
-----
* **manual**: factor fijo (x2, x3, x1.5…), activable y desactivable.
* **automático**: el factor sale del balance de la billetera con niveles de
  ``step`` USDT (por defecto 100) e histéresis para no saltar de nivel con
  cada pequeña variación del balance:

  =======  ===================  =====================================
  Nivel    Se activa con        Se mantiene mientras el balance esté
  =======  ===================  =====================================
  x1       < 200                hasta 199.99 (sube a x2 con 200)
  x2       ≥ 200                entre 100.01 y 299.99  [101-299]
  x3       ≥ 300                entre 200.01 y 399.99  [201-399]
  xN       ≥ N·100              entre (N-1)·100 y (N+1)·100 (sin incluir)
  =======  ===================  =====================================

  Es decir: sube a x(N+1) al llegar a (N+1)·step y baja a x(N-1) al caer a
  (N-1)·step o menos.
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import asdict, dataclass, field
from typing import Callable, Optional

from .precision import parse_bool, safe_float
from .storage import JsonStore

log = logging.getLogger("executor.multiplier")

MODES = ("manual", "auto")
SOURCES = {
    "wallet": "Saldo de la billetera (USDT)",
    "margin": "Saldo de margen (billetera + PnL no realizado)",
    "available": "Saldo disponible",
}
MAX_LIMIT = 100.0
_EPS = 1e-9


@dataclass
class MultiplierState:
    enabled: bool = False
    mode: str = "manual"
    factor: float = 1.0            # modo manual
    step_usdt: float = 100.0       # modo automático: tamaño de cada nivel
    max_factor: float = 10.0       # tope de seguridad (ambos modos)
    source: str = "wallet"         # qué balance usa el modo automático
    level: int = 0                 # nivel automático actual (0 = aún sin calcular)
    level_balance: float = 0.0     # balance con el que se fijó el nivel
    level_ts: float = 0.0
    updated_ts: float = 0.0
    history: list = field(default_factory=list)  # últimos cambios de nivel


def auto_level(current: int, balance: float, step: float, max_level: int) -> int:
    """Nivel automático con histéresis (ver la tabla del módulo)."""
    max_level = max(1, int(max_level))
    if balance <= 0 or step <= 0:
        return min(max(1, current or 1), max_level)
    level = current if current >= 1 else max(1, int(math.floor(balance / step + _EPS)))
    level = min(level, max_level)
    while level < max_level and balance >= (level + 1) * step - _EPS:
        level += 1
    while level > 1 and balance <= (level - 1) * step + _EPS:
        level -= 1
    return level


def level_band(level: int, step: float, max_level: int) -> dict:
    """Umbrales del nivel: con cuánto sube, con cuánto baja y el rango en que se mantiene."""
    up_at = (level + 1) * step if level < max_level else None
    down_at = (level - 1) * step if level > 1 else None
    return {
        "level": level,
        "up_at": up_at,                  # balance ≥ up_at → sube un nivel
        "down_at": down_at,              # balance ≤ down_at → baja un nivel
        "hold_min": down_at,             # se mantiene con balance > hold_min
        "hold_max": up_at,               # … y < hold_max
        "activates_at": level * step if level > 1 else 0.0,
    }


class Multiplier:
    """Estado y lógica del multiplicador (persistido en DATA_DIR/multiplier.json)."""

    def __init__(self, settings, balances: Callable[[], dict], on_change: Optional[Callable[[str, str], None]] = None):
        self.settings = settings
        self._balances = balances
        self._on_change = on_change
        self.state = self._from_env()
        self.store = JsonStore(settings.data_dir / "multiplier.json", self._serialize)

    # ── Configuración inicial (variables de entorno) y persistencia ─────────
    def _env_signature(self) -> dict:
        s = self.settings
        return {"mode": s.multiplier_mode, "factor": s.multiplier_factor, "step": s.multiplier_step_usdt,
                "max": s.multiplier_max, "source": s.multiplier_source}

    def _from_env(self) -> MultiplierState:
        s = self.settings

        def valid(value: float, default: float, low: float, high: float, name: str) -> float:
            if isinstance(value, (int, float)) and math.isfinite(value) and low <= value <= high:
                return float(value)
            log.warning("%s=%r no es válido (entre %g y %g); se usa %g", name, value, low, high, default)
            return default

        if s.multiplier_mode not in MODES + ("off", ""):
            log.warning("MULTIPLIER_MODE=%r no es válido (off, manual o auto); multiplicador desactivado",
                        s.multiplier_mode)
        mode = s.multiplier_mode if s.multiplier_mode in MODES else "manual"
        source = s.multiplier_source if s.multiplier_source in SOURCES else "wallet"
        max_factor = valid(s.multiplier_max, 10.0, 1.0, MAX_LIMIT, "MULTIPLIER_MAX")
        factor = valid(s.multiplier_factor, 1.0, 0.01, max_factor, "MULTIPLIER")
        step = valid(s.multiplier_step_usdt, 100.0, 1.0, 1e9, "MULTIPLIER_STEP_USDT")
        return MultiplierState(enabled=s.multiplier_mode in MODES, mode=mode, factor=factor, step_usdt=step,
                               max_factor=max_factor, source=source)

    def _serialize(self) -> dict:
        return {"state": asdict(self.state), "env": self._env_signature()}

    def load(self) -> None:
        data = self.store.load({}) or {}
        saved = data.get("state")
        if not isinstance(saved, dict):
            return
        if data.get("env") != self._env_signature():
            # Cambiaron las variables MULTIPLIER_* en el entorno: mandan ellas.
            log.info("Multiplicador: se usan las variables MULTIPLIER_* del entorno (cambiaron desde el último ajuste)")
            return
        names = set(MultiplierState.__dataclass_fields__)
        try:
            self.state = MultiplierState(**{k: v for k, v in saved.items() if k in names})
        except TypeError:
            return
        if self.state.enabled:
            log.info("Multiplicador restaurado: %s", self.describe())

    def save(self) -> None:
        self.store.schedule()

    # ── Balance y nivel automático ────────────────────────────────────────
    def balance(self) -> float:
        summary = self._balances() or {}
        key = {"wallet": "wallet", "margin": "margin_balance", "available": "available"}[self.state.source]
        return safe_float(summary.get(key))

    def refresh(self, reason: str = "") -> int:
        """Recalcula el nivel automático con el balance actual. Devuelve el nivel."""
        st = self.state
        summary = self._balances() or {}
        bal = self.balance()
        if bal <= 0 and not summary.get("last_sync"):
            # Balance aún desconocido (sin sincronizar): se conserva el nivel guardado, sin superar el máximo.
            capped = auto_level(st.level, 0.0, st.step_usdt, int(st.max_factor))
            if st.level and capped != st.level:
                st.level = capped
                self.save()
            return capped
        # Balance conocido: si es 0 o negativo el nivel baja a x1 (nunca se mantiene uno alto sin saldo).
        new = auto_level(st.level, bal, st.step_usdt, int(st.max_factor)) if bal > 0 else 1
        if new != st.level:
            old = st.level
            st.level, st.level_balance, st.level_ts = new, bal, time.time()
            st.history = (st.history + [{"ts": st.level_ts, "from": old, "to": new, "balance": round(bal, 4)}])[-20:]
            self.save()
            if old and st.enabled and st.mode == "auto":
                arrow = "▲" if new > old else "▼"
                msg = f"Multiplicador automático x{old} {arrow} x{new} (balance {bal:,.2f} USDT)"
                log.warning(msg)
                if self._on_change:
                    self._on_change(msg, "success" if new > old else "warning")
        return new

    # ── Factor efectivo ────────────────────────────────────────────────────
    def effective(self) -> float:
        st = self.state
        if not st.enabled:
            return 1.0
        if st.mode == "auto":
            return float(self.refresh("señal"))
        return float(min(st.factor, st.max_factor))

    def describe(self) -> str:
        st = self.state
        if not st.enabled:
            return "desactivado (x1)"
        if st.mode == "auto":
            return f"automático x{max(1, st.level or 1)} (niveles de {st.step_usdt:g} USDT)"
        return f"manual x{st.factor:g}"

    def view(self) -> dict:
        st = self.state
        bal = self.balance()
        max_level = int(st.max_factor)
        if st.enabled and st.mode == "auto":
            level = self.refresh("vista")  # el estado se consulta cada segundo: detecta los cambios de nivel
        else:
            # Vista previa: el nivel con el que arrancaría el automático con el balance actual.
            level = auto_level(0, bal, st.step_usdt, max_level) if bal > 0 else 1
        table = [{"level": n, "activates_at": n * st.step_usdt if n > 1 else 0.0,
                  "hold_min": (n - 1) * st.step_usdt if n > 1 else None,
                  "hold_max": (n + 1) * st.step_usdt if n < max_level else None}
                 for n in range(1, min(max_level, max(level + 3, 5)) + 1)]
        effective = float(level) if st.enabled and st.mode == "auto" else self.effective()
        return {
            **{k: v for k, v in asdict(st).items() if k != "history"},
            "effective": effective,
            "description": self.describe(),
            "balance": bal,
            "source_label": SOURCES.get(st.source, st.source),
            "auto": {**level_band(level, st.step_usdt, max_level), "table": table},
            "history": list(st.history)[-10:][::-1],
        }

    # ── Cambios desde el dashboard / API ────────────────────────────────────
    def configure(self, args: dict) -> dict:
        st = self.state
        new = MultiplierState(**asdict(st))
        if "mode" in args and args["mode"] not in (None, ""):
            mode = str(args["mode"]).lower().strip()
            mode = {"automatico": "auto", "automático": "auto", "automatic": "auto"}.get(mode, mode)
            if mode in ("off", "desactivado", "none"):
                new.enabled = False
            elif mode in MODES:
                new.mode = mode
            else:
                raise ValueError("mode debe ser manual, auto u off")
        if "enabled" in args and args["enabled"] not in (None, ""):
            new.enabled = parse_bool(args["enabled"], new.enabled)
        if "max_factor" in args or "max" in args:
            raw = args.get("max_factor", args.get("max"))
            if raw not in (None, ""):
                value = safe_float(raw)
                if not 1 <= value <= MAX_LIMIT:
                    raise ValueError(f"max debe estar entre 1 y {MAX_LIMIT:g}")
                new.max_factor = value
        if "factor" in args and args["factor"] not in (None, ""):
            raw = str(args["factor"]).lower().lstrip("x")
            value = safe_float(raw)
            if not 0 < value <= new.max_factor:
                raise ValueError(f"factor debe ser mayor que 0 y como máximo {new.max_factor:g} (max)")
            new.factor = value
        if any(k in args for k in ("step_usdt", "step")):
            raw = args.get("step_usdt", args.get("step"))
            if raw not in (None, ""):
                value = safe_float(raw)
                if value < 1:
                    raise ValueError("step_usdt debe ser al menos 1 USDT")
                if value != new.step_usdt:
                    new.level = 0  # con otro tamaño de nivel se recalcula desde cero
                new.step_usdt = value
        if any(k in args for k in ("source", "balance")):
            raw = str(args.get("source", args.get("balance")) or "").lower().strip()
            if raw:
                if raw not in SOURCES:
                    raise ValueError("source debe ser wallet, margin o available")
                if raw != new.source:
                    new.level = 0
                new.source = raw
        new.factor = min(new.factor, new.max_factor)
        new.level = min(new.level, int(new.max_factor))
        if (new.mode == "auto" and new.enabled and not st.enabled) or new.mode != st.mode:
            new.level = 0  # al activar el automático se parte del balance actual
        new.updated_ts = time.time()
        self.state = new
        if new.mode == "auto":
            self.refresh("ajuste")
        self.save()
        log.warning("Multiplicador: %s", self.describe())
        return self.view()

    # ── Aplicación a una señal ────────────────────────────────────────────
    @staticmethod
    def scale(values: dict, factor: float) -> dict:
        return {k: (v * factor if isinstance(v, (int, float)) and v > 0 else v) for k, v in values.items()}


__all__ = ["Multiplier", "MultiplierState", "auto_level", "level_band", "MODES", "SOURCES"]
