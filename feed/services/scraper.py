"""Asynchronous background Habr API scraper with rate limiting and circuit breaker."""

import asyncio
import json
import logging
import os
import random
from datetime import datetime, timezone
from pathlib import Path

import aiofiles
import httpx
import stamina

from feed.schemas.habr_models import HabrPublicationDTO

logger = logging.getLogger(__name__)

# Create absolute path relative to this script's directory
DEFAULT_OUTPUT = str(Path(__file__).resolve().parent.parent / "data" / "habr_analytics.jsonl")


class RateLimitException(Exception):
    """Exception raised to trigger stamina backoff on HTTP 429 and 503."""
    pass


class ScraperState:
    """Tracks error thresholds and controls circuit breaker emergency stop event."""

    def __init__(self, max_fatal_errors: int = 25, max_consecutive_not_found: int = 300) -> None:
        self.fatal_errors_count: int = 0
        self.consecutive_not_found: int = 0
        self.stop_event: asyncio.Event = asyncio.Event()
        self.max_fatal_errors: int = max_fatal_errors
        self.max_consecutive_not_found: int = max_consecutive_not_found

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
                "Consecutive NOT_FOUND horizon reached (%d)! Triggering emergency stop.",
                self.consecutive_not_found,
            )
            self.stop_event.set()

    def reset_not_found(self) -> None:
        """Reset consecutive 404 NOT_FOUND counter upon successful publication hit."""
        self.consecutive_not_found = 0


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
        # 1. Десктопный Chrome на Windows
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
        },
        # 2. Десктопный Chrome на macOS
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"macOS"',
        },
        # 3. Мобильный Chrome на Android (отлично разбавляет трафик)
        {
            "User-Agent": "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "Sec-Ch-Ua-Mobile": "?1",
            "Sec-Ch-Ua-Platform": '"Android"',
        },
        # 4. Десктопный Firefox на Windows
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
        },
    ]

    profile = random.choice(browser_profiles)

    # Базовые заголовки, общие для всех современных браузеров при обращении к API
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

    # Накладываем выбранный профиль поверх базовых заголовков
    headers.update(profile)
    return headers


async def fetch_habr_publication(
    client: httpx.AsyncClient,
    pub_id: int,
    semaphore: asyncio.Semaphore,
    state: ScraperState,
    queue: asyncio.Queue,
) -> None:
    """Fetch publication metadata for a single pub_id with rate limiting and retry handling."""
    if state.stop_event.is_set():
        return

    url = f"https://habr.com/kek/v2/articles/{pub_id}/"

    async with semaphore:
        if state.stop_event.is_set():
            return

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        try:
            # Use async for to prevent blocking the event loop
            async for attempt in stamina.retry_context(
                on=(httpx.NetworkError, httpx.TimeoutException, RateLimitException),
                attempts=5,
                wait_initial=2.0,
                wait_max=15.0,
            ):
                with attempt:
                    response = await client.get(url=url, headers=get_random_headers())

                    # Trigger backoff on rate limits or server overloads
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
                # Explicitly deleted publication or actual horizon reached
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
                # Unknown 404 error. Do NOT increment consecutive_not_found to prevent false stops.
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
                # Safe business-logic skips. Do NOT trigger circuit breaker.
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
                # Actual WAF or unauthorized ban
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
            # 429 Rate Limit, 403 Forbidden, 5xx Server Error, etc.
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
) -> ScraperState:
    """Orchestrate background scraper tasks and JSONL writer."""
    state = ScraperState()
    queue: asyncio.Queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(concurrency)

    writer_task = asyncio.create_task(file_writer(queue, output_file))

    limits = httpx.Limits(max_keepalive_connections=20, max_connections=30)
    async with httpx.AsyncClient(
        timeout=10.0,
        limits=limits,
        trust_env=trust_env,
    ) as client:
        for batch_start in range(start_id, end_id, batch_size):
            if state.stop_event.is_set():
                logger.warning("Stop event active. Halting batch loop at ID %d.", batch_start)
                break

            batch_end = min(batch_start + batch_size, end_id)
            tasks = [
                fetch_habr_publication(client, pub_id, semaphore, state, queue)
                for pub_id in range(batch_start, batch_end)
            ]
            await asyncio.gather(*tasks, return_exceptions=True)

    # Poison pill to gracefully terminate file writer
    await queue.put(None)
    await writer_task

    return state


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
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
