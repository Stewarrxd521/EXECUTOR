import asyncio
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from aiohttp.test_utils import TestServer  # noqa: E402

from executor.config import LEGACY_SIGNAL_SECRETS, Settings  # noqa: E402
from executor.service import ExecutorService  # noqa: E402
from executor.web.server import build_app  # noqa: E402
from tests.fake_binance import FakeBinance, exchange_info_payload  # noqa: E402


async def wait_for(cond, timeout: float = 6.0, interval: float = 0.05):
    """Espera hasta que ``cond()`` sea verdadero (o falla con timeout)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        await asyncio.sleep(interval)
    raise AssertionError("condición no alcanzada a tiempo")


def make_settings(fake: FakeBinance, tmp_path: Path, **over) -> Settings:
    s = Settings(
        api_key="key", api_secret="secret", signal_secret="sig", dashboard_token="dash",
        ws_api_url=f"{fake.ws}/ws-fapi/v1", stream_base_url=fake.ws, rest_url=fake.http,
        data_dir=tmp_path / "data", blocked_symbols=set(), signal_dedupe_ttl_s=0, leverage=5,
        high_price_threshold=1e12, exchange_info_bootstrap=False, seed_open_orders_rest=True,
        exchange_info_file=tmp_path / "sin_base.txt",
    )
    for k, v in over.items():
        setattr(s, k, v)
    return s


@pytest.fixture
async def fake():
    f = FakeBinance(hedge=True)
    await f.start()
    yield f
    await f.stop()


@pytest.fixture
def snapshot_file(tmp_path):
    """Snapshot de exchangeInfo con los símbolos del simulador."""
    from executor.exchange_info import build_snapshot, parse_exchange_info
    import json

    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    snap = build_snapshot(parse_exchange_info(exchange_info_payload()), "test")
    (data_dir / "exchange_info.json").write_text(json.dumps(snap))
    return data_dir / "exchange_info.json"


@pytest.fixture
async def service(fake, tmp_path, snapshot_file):
    svc = ExecutorService(make_settings(fake, tmp_path))
    await svc.start()
    await wait_for(lambda: svc.core.market.marks and svc.user.conn.connected)
    yield svc
    await svc.stop()


SECRET = "clave-secreta-aleatoria"


def legacy_settings(fake, tmp_path, **over):
    """Executor desplegado sin SIGNAL_SECRET ni DASHBOARD_TOKEN (como en Render)."""
    over.setdefault("dashboard_token", "")
    return make_settings(fake, tmp_path, signal_secret=LEGACY_SIGNAL_SECRETS[0],
                         signal_secrets=list(LEGACY_SIGNAL_SECRETS), signal_secret_is_default=True, **over)


@pytest.fixture
async def open_service(fake, tmp_path, snapshot_file):
    svc = ExecutorService(legacy_settings(fake, tmp_path))
    await svc.start()
    await wait_for(lambda: svc.core.market.marks and svc.user.conn.connected and svc.trades.ready.is_set())
    yield svc
    await svc.stop()


@pytest.fixture
async def live_url(open_service):
    """Servidor HTTP real (los bridges usan urllib en hilos)."""
    server = TestServer(build_app(open_service))
    await server.start_server()
    yield str(server.make_url("")).rstrip("/")
    await server.close()
