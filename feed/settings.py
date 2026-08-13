from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from feed.helpers import get_data_path

DEFAULT_OUTPUT_BASE_DIR = str(Path(__file__).resolve().parent / "data")

RAW_SCRAPES_FILE = "raw_scraper.jsonl"
HEADFRACTION_HTTP_STATUSES_FILE = "http_statuses.csv"
HEADFRACTION_SEO_FLAGS_FILE = "seo_flags.csv"
HEADFRACTION_POST_TYPES_FILE = "post_types.csv"
HEADFRACTION_HUBS_FREQUENCY_FILE = "hubs_frequency.csv"
HEADFRACTION_TAGS_FREQUENCY_FILE = "tags_frequency.csv"
HEADFRACTION_TAG_EDGES_FILE = "hub_tag_edges.csv"
SCRAPER_LOG_FILE = "scraper.log"

HABR_BASE_API_URL: str = "https://habr.com/kek/v2/articles/"

BROWSER_PROFILES: list[dict[str, str]] = [
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


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    PROXY_SERVER_STR: Optional[str] = None
    START_ID: int = 100
    END_ID: int = 200
    BATCH_SIZE: int = 500
    CONCURRENCY: int = 15
    SCRAPER_OUTPUT_FILE: str = Field(default_factory=lambda: get_data_path(RAW_SCRAPES_FILE))
    USE_RAY: bool = True
    RAY_ADDRESS: str = "ray://127.0.0.1:10001"


settings = Settings()
