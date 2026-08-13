from pathlib import Path
from feed.helpers import get_data_path
from feed.settings import (
    DEFAULT_OUTPUT_BASE_DIR,
    HEADFRACTION_HTTP_STATUSES_FILE,
    HEADFRACTION_HUBS_FREQUENCY_FILE,
    HEADFRACTION_POST_TYPES_FILE,
    HEADFRACTION_SEO_FLAGS_FILE,
    HEADFRACTION_TAG_EDGES_FILE,
    HEADFRACTION_TAGS_FREQUENCY_FILE,
    RAW_SCRAPES_FILE,
    SCRAPER_LOG_FILE,
    settings,
)


def test_settings_default_values() -> None:
    """Test that Settings loads default values correctly."""
    assert settings.HABR_BASE_API_URL == "https://habr.com/kek/v2/articles/"
    assert settings.START_ID == 100
    assert settings.END_ID == 200
    assert settings.BATCH_SIZE == 500
    assert settings.CONCURRENCY == 15


def test_hardcoded_filenames() -> None:
    """Test hardcoded data file name constants."""
    assert RAW_SCRAPES_FILE == "raw_scraper.jsonl"
    assert HEADFRACTION_HTTP_STATUSES_FILE == "http_statuses.csv"
    assert HEADFRACTION_SEO_FLAGS_FILE == "seo_flags.csv"
    assert HEADFRACTION_POST_TYPES_FILE == "post_types.csv"
    assert HEADFRACTION_HUBS_FREQUENCY_FILE == "hubs_frequency.csv"
    assert HEADFRACTION_TAGS_FREQUENCY_FILE == "tags_frequency.csv"
    assert HEADFRACTION_TAG_EDGES_FILE == "hub_tag_edges.csv"
    assert SCRAPER_LOG_FILE == "scraper.log"


def test_get_data_path_default() -> None:
    """Test get_data_path with default output base directory."""
    path = get_data_path(RAW_SCRAPES_FILE)
    expected = str(Path(DEFAULT_OUTPUT_BASE_DIR) / "raw_scraper.jsonl")
    assert path == expected


def test_get_data_path_custom_dir(tmp_path: Path) -> None:
    """Test get_data_path with custom base directory."""
    path = get_data_path(HEADFRACTION_TAG_EDGES_FILE, tmp_path)
    assert path == str(tmp_path / "hub_tag_edges.csv")


def test_sanitize_proxy_url() -> None:
    """Test sanitize_proxy_url helper with various inputs."""
    from feed.helpers import sanitize_proxy_url

    assert sanitize_proxy_url(None) == "Disabled (Direct Connection)"
    assert sanitize_proxy_url("") == "Disabled (Direct Connection)"
    assert sanitize_proxy_url("   ") == "Disabled (Direct Connection)"
    assert (
        sanitize_proxy_url("http://user:secretpass@127.0.0.1:8080")
        == "http://user@127.0.0.1:8080"
    )
    assert sanitize_proxy_url("http://127.0.0.1:8080") == "http://127.0.0.1:8080"


def test_get_random_headers() -> None:
    """Test get_random_headers helper returns expected header structure."""
    from feed.helpers import get_random_headers
    from feed.settings import BROWSER_PROFILES

    assert len(BROWSER_PROFILES) >= 4
    headers = get_random_headers()
    assert "User-Agent" in headers
    assert "Accept" in headers
    assert headers["Referer"] == "https://habr.com/"
