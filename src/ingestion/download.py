from __future__ import annotations

from pathlib import Path

import requests

from src.utils.io_utils import ensure_dir
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


def download_file(url: str, destination: str) -> str:
    ensure_dir(str(Path(destination).parent))
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    with open(destination, "wb") as f:
        f.write(response.content)
    logger.info("Downloaded %s to %s", url, destination)
    return destination
