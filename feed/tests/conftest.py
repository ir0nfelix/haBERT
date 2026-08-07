"""Pytest configuration and global fixtures for feed tests."""

import json
from pathlib import Path
import pytest


FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def raw_habr_article() -> dict:
    """Fixture returning raw JSON dict for article publication (ID 1041202)."""
    fixture_path = FIXTURES_DIR / "article_example.json"
    with open(fixture_path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def raw_habr_news() -> dict:
    """Fixture returning raw JSON dict for news publication (ID 1068052)."""
    fixture_path = FIXTURES_DIR / "new_example.json"
    with open(fixture_path, "r", encoding="utf-8") as f:
        return json.load(f)
