from pathlib import Path
from typing import Union
from urllib.parse import urlparse

from feed.settings import DEFAULT_OUTPUT_BASE_DIR


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
