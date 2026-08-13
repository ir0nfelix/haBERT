from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_OUTPUT_BASE_DIR = str(Path(__file__).resolve().parent / "data")

RAW_SCRAPES_FILE = "raw_scraper.jsonl"
HEADFRACTION_HTTP_STATUSES_FILE = "http_statuses.csv"
HEADFRACTION_SEO_FLAGS_FILE = "seo_flags.csv"
HEADFRACTION_POST_TYPES_FILE = "post_types.csv"
HEADFRACTION_HUBS_FREQUENCY_FILE = "hubs_frequency.csv"
HEADFRACTION_TAGS_FREQUENCY_FILE = "tags_frequency.csv"
HEADFRACTION_TAG_EDGES_FILE = "hub_tag_edges.csv"
SCRAPER_LOG_FILE = "scraper.log"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    HABR_BASE_API_URL: str = "https://habr.com/kek/v2/articles/"
    PROXY_SERVER_STR: Optional[str] = None
    START_ID: int = 100
    END_ID: int = 200
    BATCH_SIZE: int = 500
    CONCURRENCY: int = 15
    OUTPUT_FILE: Optional[str] = None


settings = Settings()

