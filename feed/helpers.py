import random
from pathlib import Path
from typing import Union
from urllib.parse import urlparse

from feed.settings import BROWSER_PROFILES, DEFAULT_OUTPUT_BASE_DIR


def get_data_path(filename: str, base_dir: Union[str, Path, None] = None) -> str:
    target_dir = Path(base_dir) if base_dir else Path(DEFAULT_OUTPUT_BASE_DIR)
    return str(target_dir / filename)


def sanitize_proxy_url(proxy: str | None) -> str:
    """Returns a safe, sanitized string representation of a proxy URL (masking credentials)."""
    if not proxy or not proxy.strip():
        return "Disabled (Direct Connection)"
    try:
        parsed = urlparse(proxy.strip())
        if parsed.hostname:
            port_str = f":{parsed.port}" if parsed.port else ""
            user_str = f"{parsed.username}@" if parsed.username else ""
            return f"{parsed.scheme or 'http'}://{user_str}{parsed.hostname}{port_str}"
    except Exception:
        pass
    return "Enabled"


def get_random_headers() -> dict[str, str]:
    profile = random.choice(BROWSER_PROFILES)
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
