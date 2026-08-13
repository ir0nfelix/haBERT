import asyncio
import logging
from typing import Any

import httpx

from feed.helpers import get_random_headers
from feed.settings import settings

logger = logging.getLogger(__name__)

try:
    import ray

    RAY_AVAILABLE = True
except ImportError:
    ray = RAY_AVAILABLE = False


def init_ray(address: str | None = None) -> bool:
    if not RAY_AVAILABLE:
        logger.warning("Ray package is not installed. Scraper will use local fallback for gap sweeps.")
        return False

    if ray and ray.is_initialized():
        return True

    target_address = address or settings.RAY_ADDRESS
    try:
        ray.init(address=target_address, ignore_reinit_error=True)
        logger.info("Successfully initialized Ray connection to %s", target_address)
        return True
    except Exception as exc:
        logger.warning(
            "Could not connect to Ray cluster at %s: %s. Continuing with local gap sweeper fallback.",
            target_address,
            exc,
        )
        return False


def is_ray_initialized() -> bool:
    return bool(RAY_AVAILABLE and ray and ray.is_initialized())


if RAY_AVAILABLE:

    @ray.remote
    def ray_gap_sweeper(gap_start: int, gap_stop: int) -> list[int]:
        """Ray remote task executing asynchronous HEAD/GET checks over a missing gap range."""

        async def _sweep() -> list[int]:
            alive: list[int] = []
            limits = httpx.Limits(max_keepalive_connections=10, max_connections=25)
            proxy = settings.PROXY_SERVER_STR
            async with httpx.AsyncClient(timeout=8.0, limits=limits, trust_env=False, proxy=proxy) as client:
                for pub_id in range(gap_start, gap_stop):
                    url = f"{settings.HABR_BASE_API_URL.rstrip('/')}/{pub_id}/"
                    try:
                        resp = await client.head(url=url, headers=get_random_headers())
                        if resp.status_code in (200, 403):
                            alive.append(pub_id)
                    except Exception:
                        pass
            return alive

        return asyncio.run(_sweep())

else:

    def ray_gap_sweeper(*args: Any, **kwargs: Any) -> Any:  # type: ignore[misc]
        raise NotImplementedError("Ray package is not installed.")
