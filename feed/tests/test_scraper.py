"""Unit tests for feed/services/scraper.py."""

import asyncio
import json
from pathlib import Path
import pytest
import httpx

from feed.services.scraper import (
    ScraperState,
    fetch_habr_publication,
    file_writer,
    run_scraper,
)


@pytest.mark.asyncio
async def test_scraper_state_thresholds() -> None:
    """Test ScraperState error counters and stop_event triggers."""
    state = ScraperState(max_fatal_errors=3, max_consecutive_not_found=5)
    assert not state.stop_event.is_set()

    state.register_fatal_error("Test error 1")
    state.register_fatal_error("Test error 2")
    assert not state.stop_event.is_set()

    state.register_fatal_error("Test error 3")
    assert state.stop_event.is_set()

    state_nf = ScraperState(max_consecutive_not_found=3)
    for _ in range(2):
        state_nf.register_not_found()
        assert not state_nf.stop_event.is_set()

    state_nf.register_not_found()
    assert state_nf.stop_event.is_set()

    state_nf.reset_not_found()
    assert state_nf.consecutive_not_found == 0


@pytest.mark.asyncio
async def test_file_writer(tmp_path: Path) -> None:
    """Test background file_writer task with asyncio.Queue."""
    output_file = tmp_path / "test_analytics.jsonl"
    queue: asyncio.Queue = asyncio.Queue()

    writer_task = asyncio.create_task(file_writer(queue, str(output_file)))

    record1 = {"pub_id": 1, "http_status": 200}
    record2 = {"pub_id": 2, "http_status": 404}

    await queue.put(record1)
    await queue.put(record2)
    await queue.put(None)  # Poison pill

    await writer_task

    with open(output_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    assert len(lines) == 2
    assert json.loads(lines[0]) == record1
    assert json.loads(lines[1]) == record2


@pytest.mark.asyncio
async def test_fetch_habr_publication_200_ok(raw_habr_news: dict) -> None:
    """Test fetching and parsing 200 OK publication."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=raw_habr_news)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        semaphore = asyncio.Semaphore(15)
        state = ScraperState()
        queue: asyncio.Queue = asyncio.Queue()

        await fetch_habr_publication(client, 1068052, semaphore, state, queue)

        record = await queue.get()
        assert record["pub_id"] == 1068052
        assert record["http_status"] == 200
        assert record["error_code"] is None
        assert record["post_type"] == "news"
        assert record["author_score"] == 1066
        assert record["article_score"] == 1
        assert "Искусственный интеллект" in record["hubs"]
        assert record["tags"] == ["Claude Opus 5", "rm -rf"]


@pytest.mark.asyncio
async def test_fetch_habr_publication_404_mismatch() -> None:
    """Test 404 POST_TYPE_MISMATCH micro-post handling."""
    payload = {
        "data": {"type": "post"},
        "error": {"code": "POST_TYPE_MISMATCH", "message": "Не совпадает тип публикации"},
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json=payload)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        semaphore = asyncio.Semaphore(15)
        state = ScraperState()
        queue: asyncio.Queue = asyncio.Queue()

        await fetch_habr_publication(client, 560001, semaphore, state, queue)

        record = await queue.get()
        assert record["pub_id"] == 560001
        assert record["http_status"] == 404
        assert record["error_code"] == "POST_TYPE_MISMATCH"
        assert record["post_type"] == "post"
        assert state.fatal_errors_count == 0
        assert state.consecutive_not_found == 0


@pytest.mark.asyncio
async def test_fetch_habr_publication_404_not_found() -> None:
    """Test 404 NOT_FOUND deleted publication handling."""
    payload = {"error": {"code": "NOT_FOUND", "message": "Not Found"}}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json=payload)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        semaphore = asyncio.Semaphore(15)
        state = ScraperState()
        queue: asyncio.Queue = asyncio.Queue()

        await fetch_habr_publication(client, 560002, semaphore, state, queue)

        record = await queue.get()
        assert record["pub_id"] == 560002
        assert record["http_status"] == 404
        assert record["error_code"] == "NOT_FOUND"
        assert state.consecutive_not_found == 1


@pytest.mark.asyncio
async def test_fetch_habr_publication_429_rate_limit() -> None:
    """Test 429 Rate Limit error triggering fatal error counter."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, json={"error": "Too Many Requests"})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        semaphore = asyncio.Semaphore(15)
        state = ScraperState()
        queue: asyncio.Queue = asyncio.Queue()

        await fetch_habr_publication(client, 560003, semaphore, state, queue)

        record = await queue.get()
        assert record["pub_id"] == 560003
        assert record["http_status"] == 429
        assert record["error_code"] == "HTTP_429"
        assert state.fatal_errors_count == 1
