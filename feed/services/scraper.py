import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import aiofiles
import httpx
import stamina

from feed.helpers import get_data_path, get_random_headers, sanitize_proxy_url
from feed.ray import init_ray, is_ray_initialized, ray, ray_gap_sweeper
from feed.schemas.habr_models import HabrPublicationDTO
from feed.settings import RAW_SCRAPES_FILE, SCRAPER_LOG_FILE, settings

logger = logging.getLogger(__name__)


DEFAULT_OUTPUT = get_data_path(RAW_SCRAPES_FILE)


class RateLimitException(Exception):
    """Exception raised to trigger stamina backoff on HTTP 429 and 503."""

    pass


class ScraperState:
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
        """Increment consecutive 404 NOT_FOUND counter."""
        self.consecutive_not_found += 1
        if self.consecutive_not_found >= self.max_consecutive_not_found:
            logger.warning(
                "Consecutive NOT_FOUND threshold reached (%d/%d)!",
                self.consecutive_not_found,
                self.max_consecutive_not_found,
            )

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


async def fallback_gap_sweeper(gap_start: int, gap_stop: int) -> list[int]:
    """In-process async fallback sweeper when Ray is not connected."""
    alive: list[int] = []
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=25)
    proxy = settings.PROXY_SERVER_STR
    logger.info(
        "Running local gap sweep for range (%d, %d) [proxy=%s].",
        gap_start,
        gap_stop,
        sanitize_proxy_url(proxy),
    )
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

    if use_ray and is_ray_initialized():
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
            url = f"{settings.HABR_BASE_API_URL.rstrip('/')}/{test_id}/"
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
    if state.stop_event.is_set() or state.consecutive_not_found >= state.max_consecutive_not_found:
        return

    url = f"{settings.HABR_BASE_API_URL.rstrip('/')}/{pub_id}/"

    async with semaphore:
        if state.stop_event.is_set() or state.consecutive_not_found >= state.max_consecutive_not_found:
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
                            "Rate limited (HTTP %d) on pub_id=%d [proxy=%s]. Backing off...",
                            response.status_code,
                            pub_id,
                            sanitize_proxy_url(settings.PROXY_SERVER_STR),
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
                logger.warning(
                    "HTTP 403 (Security Ban) for pub_id=%d [proxy=%s]",
                    pub_id,
                    sanitize_proxy_url(settings.PROXY_SERVER_STR),
                )
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


async def scraper(
    start_id: int,
    end_id: int,
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

    active_proxy = settings.PROXY_SERVER_STR
    logger.info(
        "Starting scraper execution: range=(%d, %d), batch_size=%d, concurrency=%d, proxy=%s",
        start_id,
        end_id,
        batch_size,
        concurrency,
        sanitize_proxy_url(active_proxy),
    )
    limits = httpx.Limits(max_keepalive_connections=20, max_connections=30)
    async with httpx.AsyncClient(
        timeout=10.0,
        limits=limits,
        trust_env=trust_env,
        proxy=active_proxy,
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


# Alias for backward compatibility
run_scraper = scraper


if __name__ == "__main__":
    log_file = Path(get_data_path(SCRAPER_LOG_FILE))
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    if settings.USE_RAY:
        init_ray()

    asyncio.run(
        scraper(
            start_id=settings.START_ID,
            end_id=settings.END_ID,
            batch_size=settings.BATCH_SIZE,
            concurrency=settings.CONCURRENCY,
            output_file=settings.SCRAPER_OUTPUT_FILE or get_data_path(RAW_SCRAPES_FILE),
        )
    )
