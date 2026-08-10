import asyncio
import json
import logging
import os
from pathlib import Path
import random
from typing import Any
from datetime import datetime, timezone

import aiofiles
import httpx
import stamina

from feed.schemas.habr_models import HabrPublicationDTO

logger = logging.getLogger(__name__)

# Create absolute path relative to this script's directory
DEFAULT_OUTPUT = str(Path(__file__).resolve().parent.parent / "data" / "habr_analytics.jsonl")

# Ray integration setup
try:
    import ray
    RAY_AVAILABLE = True
except ImportError:
    ray = None
    RAY_AVAILABLE = False


def init_ray(address: str = "ray://127.0.0.1:10001") -> bool:
    """Initialize connection to Ray cluster with graceful fallback."""
    if not RAY_AVAILABLE:
        logger.warning("Ray package is not installed. Scraper will use local fallback for gap sweeps.")
        return False
    try:
        ray.init(address=address, ignore_reinit_error=True)
        logger.info("Successfully initialized Ray connection to %s", address)
        return True
    except Exception as exc:
        logger.warning(
            "Could not connect to Ray cluster at %s: %s. Continuing with local gap sweeper fallback.",
            address,
            exc,
        )
        return False


class RateLimitException(Exception):
    """Exception raised to trigger stamina backoff on HTTP 429 and 503."""

    pass


class ScraperState:
    """Tracks error thresholds, consecutive 404s, known gap ranges, and controls emergency stop."""

    def __init__(self, max_fatal_errors: int = 25, max_consecutive_not_found: int = 100) -> None:
        self.fatal_errors_count: int = 0
        self.consecutive_not_found: int = 0
        self.stop_event: asyncio.Event = asyncio.Event()
        self.max_fatal_errors: int = max_fatal_errors
        self.max_consecutive_not_found: int = max_consecutive_not_found
        self.known_gaps: list[tuple[int, int]] = []
        self.is_probing: bool = False

    def register_fatal_error(self, reason: str) -> None:
        """Increment fatal error counter and trigger stop_event if threshold reached."""
        self.fatal_errors_count += 1
        logger.warning(
            "Fatal error registered (%d/%d): %s",
            self.fatal_errors_count,
            self.max_fatal_errors,
            reason,
        )
        if self.fatal_errors_count >= self.max_fatal_errors:
            logger.error("Max fatal errors threshold reached! Triggering emergency stop.")
            self.stop_event.set()

    def register_not_found(self) -> None:
        """Increment consecutive 404 NOT_FOUND counter and trigger stop_event if threshold reached."""
        self.consecutive_not_found += 1
        if self.consecutive_not_found >= self.max_consecutive_not_found:
            logger.error(
                "Consecutive NOT_FOUND threshold reached (%d/%d)!",
                self.consecutive_not_found,
                self.max_consecutive_not_found,
            )
            self.stop_event.set()

    def reset_not_found(self) -> None:
        """Reset consecutive 404 NOT_FOUND counter upon successful publication hit or gap jump."""
        self.consecutive_not_found = 0
        self.stop_event.clear()


async def file_writer(queue: asyncio.Queue, file_path: str) -> None:
    """Async background worker task consuming JSON payloads from queue and appending to JSONL file."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    async with aiofiles.open(file_path, mode="a", encoding="utf-8") as f:
        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break
            line = json.dumps(item, ensure_ascii=False) + "\n"
            await f.write(line)
            await f.flush()
            queue.task_done()


def get_random_headers() -> dict[str, str]:
    """Генерирует случайный, максимально реалистичный профиль браузера для обхода WAF."""
    browser_profiles = [
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
        },
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"macOS"',
        },
        {
            "User-Agent": "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "Sec-Ch-Ua-Mobile": "?1",
            "Sec-Ch-Ua-Platform": '"Android"',
        },
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
        },
    ]

    profile = random.choice(browser_profiles)
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Priority": "u=1, i",
        "Cache-Control": "max-age=0",
        "Referer": "https://habr.com/",
        "Origin": "https://habr.com/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Connection": "keep-alive",
    }
    headers.update(profile)
    return headers


# Phase 4: Ray Worker Logic
if RAY_AVAILABLE:

    @ray.remote
    def ray_gap_sweeper(gap_start: int, gap_stop: int) -> list[int]:
        """Ray remote task executing asynchronous HEAD/GET checks over a missing gap range."""

        async def _sweep() -> list[int]:
            alive: list[int] = []
            limits = httpx.Limits(max_keepalive_connections=10, max_connections=25)
            async with httpx.AsyncClient(timeout=8.0, limits=limits, trust_env=False) as client:
                for pub_id in range(gap_start, gap_stop):
                    url = f"https://habr.com/kek/v2/articles/{pub_id}/"
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


async def fallback_gap_sweeper(gap_start: int, gap_stop: int) -> list[int]:
    """In-process async fallback sweeper when Ray is not connected."""
    alive: list[int] = []
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=25)
    async with httpx.AsyncClient(timeout=8.0, limits=limits, trust_env=False) as client:
        for pub_id in range(gap_start, gap_stop):
            url = f"https://habr.com/kek/v2/articles/{pub_id}/"
            try:
                resp = await client.head(url=url, headers=get_random_headers())
                if resp.status_code in (200, 403):
                    alive.append(pub_id)
            except Exception:
                pass
    return alive


# Phase 5: The Watcher Bridge
async def gap_watcher(
    gap_start: int,
    gap_stop: int,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    state: ScraperState,
    result_queue: asyncio.Queue,
    use_ray: bool = True,
) -> None:
    """Asynchronously awaits Ray gap sweep task and spawns harvest tasks for alive IDs."""
    logger.info("Gap watcher launched for range (%d, %d).", gap_start, gap_stop)
    alive_ids: list[int] = []

    if use_ray and RAY_AVAILABLE and ray.is_initialized():
        try:
            ray_future = ray_gap_sweeper.remote(gap_start, gap_stop)
            alive_ids = await asyncio.to_thread(ray.get, ray_future)
        except Exception as exc:
            logger.warning(
                "Ray sweep failed for range (%d, %d): %s. Falling back to local sweep.",
                gap_start,
                gap_stop,
                exc,
            )
            alive_ids = await fallback_gap_sweeper(gap_start, gap_stop)
    else:
        logger.info("Ray not connected. Running local gap sweep for range (%d, %d).", gap_start, gap_stop)
        alive_ids = await fallback_gap_sweeper(gap_start, gap_stop)

    logger.info(
        "Gap sweep for range (%d, %d) completed. Found %d alive IDs. Spawning harvest tasks...",
        gap_start,
        gap_stop,
        len(alive_ids),
    )
    for alive_id in alive_ids:
        asyncio.create_task(
            fetch_habr_publication(client, alive_id, semaphore, state, result_queue)
        )


# Phase 2: Probing Helper
async def probe_gap_right_edge(
    client: httpx.AsyncClient,
    gap_start: int,
    max_probe_distance: int = 50000,
) -> int | None:
    """Probes forward with exponential/incremental jumps (+300, +500, +700, +1000, +2000, +5000...) to find gap_stop."""
    jump_multipliers = [300, 500, 700, 1000, 2000, 5000, 10000, 20000, 50000]

    for jump in jump_multipliers:
        if jump > max_probe_distance:
            break
        probe_target = gap_start + jump
        logger.info("Probing for right edge at gap_start + %d (ID %d)...", jump, probe_target)

        # Check local window of up to 30 IDs around probe_target
        local_hit = None
        for test_id in range(probe_target, probe_target + 30):
            url = f"https://habr.com/kek/v2/articles/{test_id}/"
            try:
                resp = await client.head(url=url, headers=get_random_headers())
                if resp.status_code in (200, 403):
                    local_hit = test_id
                    break
            except Exception:
                pass

        if local_hit is not None:
            logger.info("Right edge hit found at pub_id=%d!", local_hit)
            return local_hit

    return None


async def fetch_habr_publication(
    client: httpx.AsyncClient,
    pub_id: int,
    semaphore: asyncio.Semaphore,
    state: ScraperState,
    queue: asyncio.Queue,
) -> None:
    """Fetch publication metadata for a single pub_id with rate limiting and retry handling (Phase 1)."""
    if state.stop_event.is_set():
        return

    url = f"https://habr.com/kek/v2/articles/{pub_id}/"

    async with semaphore:
        if state.stop_event.is_set():
            return

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        try:
            async for attempt in stamina.retry_context(
                on=(httpx.NetworkError, httpx.TimeoutException, RateLimitException),
                attempts=5,
                wait_initial=2.0,
                wait_max=15.0,
            ):
                with attempt:
                    response = await client.get(url=url, headers=get_random_headers())

                    if response.status_code in (429, 503):
                        logger.warning(
                            "Rate limited (HTTP %d) on pub_id=%d. Backing off...",
                            response.status_code,
                            pub_id,
                        )
                        raise RateLimitException(f"HTTP {response.status_code}")

        except (httpx.NetworkError, httpx.TimeoutException, RateLimitException) as exc:
            state.register_fatal_error(f"Failed after retries for pub_id={pub_id}: {exc}")
            record = {
                "timestamp": timestamp,
                "pub_id": pub_id,
                "http_status": 0,
                "error_code": "NETWORK_OR_RATE_LIMIT",
                "post_type": None,
                "is_seo": False,
                "article_score": 0,
                "author_score": 0,
                "hubs": [],
                "tags": [],
            }
            await queue.put(record)
            return
        except Exception as exc:
            state.register_fatal_error(f"Unexpected exception for pub_id={pub_id}: {exc}")
            record = {
                "timestamp": timestamp,
                "pub_id": pub_id,
                "http_status": 0,
                "error_code": "UNEXPECTED_ERROR",
                "post_type": None,
                "is_seo": False,
                "article_score": 0,
                "author_score": 0,
                "hubs": [],
                "tags": [],
            }
            await queue.put(record)
            return

        if response.status_code == 200:
            try:
                dto = HabrPublicationDTO.model_validate(response.json())
                state.reset_not_found()
                record = {
                    "timestamp": timestamp,
                    "pub_id": pub_id,
                    "http_status": 200,
                    "error_code": None,
                    "post_type": dto.post_type,
                    "is_seo": dto.author.is_seo,
                    "article_score": dto.statistics.score,
                    "author_score": dto.author.score_stats.score if dto.author.score_stats else 0,
                    "hubs": [hub.title for hub in dto.hubs],
                    "tags": dto.tags,
                }
            except Exception as exc:
                logger.error("Failed to parse pub_id=%d DTO: %s", pub_id, exc)
                record = {
                    "timestamp": timestamp,
                    "pub_id": pub_id,
                    "http_status": 200,
                    "error_code": "PARSE_ERROR",
                    "post_type": None,
                    "is_seo": False,
                    "article_score": 0,
                    "author_score": 0,
                    "hubs": [],
                    "tags": [],
                }
            await queue.put(record)

        elif response.status_code == 404:
            error_code = None
            try:
                data = response.json()
                error_code = (
                    data.get("meta", {}).get("errorCode")
                    or data.get("errorCode")
                    or data.get("error", {}).get("code")
                )
            except Exception:
                pass

            if error_code == "POST_TYPE_MISMATCH":
                # Micro-post (postType="post") - Safe skip
                record = {
                    "timestamp": timestamp,
                    "pub_id": pub_id,
                    "http_status": 404,
                    "error_code": "POST_TYPE_MISMATCH",
                    "post_type": "post",
                    "is_seo": False,
                    "article_score": 0,
                    "author_score": 0,
                    "hubs": [],
                    "tags": [],
                }
                await queue.put(record)

            elif error_code == "NOT_FOUND":
                # Explicitly deleted publication or potential horizon
                state.register_not_found()
                record = {
                    "timestamp": timestamp,
                    "pub_id": pub_id,
                    "http_status": 404,
                    "error_code": "NOT_FOUND",
                    "post_type": None,
                    "is_seo": False,
                    "article_score": 0,
                    "author_score": 0,
                    "hubs": [],
                    "tags": [],
                }
                await queue.put(record)

            else:
                # Unknown 404 error
                record = {
                    "timestamp": timestamp,
                    "pub_id": pub_id,
                    "http_status": 404,
                    "error_code": f"HTTP_404_{error_code or 'UNKNOWN'}",
                    "post_type": None,
                    "is_seo": False,
                    "article_score": 0,
                    "author_score": 0,
                    "hubs": [],
                    "tags": [],
                }
                await queue.put(record)

        elif response.status_code == 403:
            error_code = None
            try:
                data = response.json()
                error_code = (
                    data.get("meta", {}).get("errorCode")
                    or data.get("errorCode")
                    or data.get("error", {}).get("code")
                )
            except Exception:
                pass

            if error_code in ("IN_DRAFTS", "AUTHOR_INACTIVE"):
                # Safe business-logic skips
                record = {
                    "timestamp": timestamp,
                    "pub_id": pub_id,
                    "http_status": 403,
                    "error_code": error_code,
                    "post_type": None,
                    "is_seo": False,
                    "article_score": 0,
                    "author_score": 0,
                    "hubs": [],
                    "tags": [],
                }
                await queue.put(record)
            else:
                # Security Ban
                state.register_fatal_error(f"HTTP 403 (Security Ban) for pub_id={pub_id}")
                record = {
                    "timestamp": timestamp,
                    "pub_id": pub_id,
                    "http_status": 403,
                    "error_code": f"HTTP_403_{error_code or 'WAF'}",
                    "post_type": None,
                    "is_seo": False,
                    "article_score": 0,
                    "author_score": 0,
                    "hubs": [],
                    "tags": [],
                }
                await queue.put(record)

        else:
            state.register_fatal_error(f"HTTP {response.status_code} for pub_id={pub_id}")
            record = {
                "timestamp": timestamp,
                "pub_id": pub_id,
                "http_status": response.status_code,
                "error_code": f"HTTP_{response.status_code}",
                "post_type": None,
                "is_seo": False,
                "article_score": 0,
                "author_score": 0,
                "hubs": [],
                "tags": [],
            }
            await queue.put(record)


async def run_scraper(
    start_id: int = 560000,
    end_id: int = 560500,
    batch_size: int = 100,
    concurrency: int = 15,
    output_file: str = DEFAULT_OUTPUT,
    trust_env: bool = False,
    use_ray: bool = True,
) -> ScraperState:
    """Orchestrate state-machine scraper, gap probing, Ray delegation, and JSONL writer."""
    state = ScraperState()
    result_queue: asyncio.Queue = asyncio.Queue()
    pub_id_queue: asyncio.Queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(concurrency)

    writer_task = asyncio.create_task(file_writer(result_queue, output_file))

    limits = httpx.Limits(max_keepalive_connections=20, max_connections=30)
    async with httpx.AsyncClient(
        timeout=10.0,
        limits=limits,
        trust_env=trust_env,
    ) as client:
        current_id = start_id

        while current_id < end_id and not state.stop_event.is_set():
            # Phase 2 & 3: Gap Detection & Probing
            if state.consecutive_not_found >= state.max_consecutive_not_found:
                gap_start = current_id - state.max_consecutive_not_found
                logger.warning(
                    "Consecutive NOT_FOUND limit reached (%d). Initiating Gap Probing from gap_start=%d...",
                    state.consecutive_not_found,
                    gap_start,
                )
                state.is_probing = True

                gap_stop = await probe_gap_right_edge(client, gap_start)

                if gap_stop is not None:
                    logger.info(
                        "Gap right edge found! Range: (%d, %d). Delegating sweep to Ray and jumping current_id -> %d",
                        gap_start,
                        gap_stop,
                        gap_stop,
                    )
                    state.known_gaps.append((gap_start, gap_stop))
                    # Phase 3: Non-blocking Ray Watcher delegation
                    asyncio.create_task(
                        gap_watcher(
                            gap_start,
                            gap_stop,
                            client,
                            semaphore,
                            state,
                            result_queue,
                            use_ray=use_ray,
                        )
                    )
                    current_id = gap_stop
                    state.reset_not_found()
                    state.is_probing = False
                    continue
                else:
                    logger.error(
                        "Probing failed to find right edge within max distance. Horizon reached! Halting."
                    )
                    state.stop_event.set()
                    break

            # Phase 1: Normal Batch Generation
            batch_end = min(current_id + batch_size, end_id)
            tasks = [
                fetch_habr_publication(client, pub_id, semaphore, state, result_queue)
                for pub_id in range(current_id, batch_end)
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
            current_id = batch_end

    # Poison pill to gracefully terminate file writer
    await result_queue.put(None)
    await writer_task

    return state


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_ray()

    start_id = int(os.getenv("START_ID", "560000"))
    end_id = int(os.getenv("END_ID", "1060000"))
    batch_size = int(os.getenv("BATCH_SIZE", "500"))
    concurrency = int(os.getenv("CONCURRENCY", "15"))
    output_file = os.getenv("OUTPUT_FILE", DEFAULT_OUTPUT)

    asyncio.run(
        run_scraper(
            start_id=start_id,
            end_id=end_id,
            batch_size=batch_size,
            concurrency=concurrency,
            output_file=output_file,
        )
    )
