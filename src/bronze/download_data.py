"""Download raw yellow-trip parquet files and the zone lookup CSV."""

import logging
import shutil
import urllib.error
import urllib.request
from pathlib import Path

from src.common.config import (
    EXTRA_MONTH_JSON,
    EXTRA_MONTH_PG,
    MONTHS,
    RAW_DIR,
    TRIP_URL_TEMPLATE,
    ZONE_LOOKUP_URL,
)

logger = logging.getLogger(__name__)

USER_AGENT = "TripFlowLakehouse/1.0"
MAX_ATTEMPTS = 3
_CHUNK_BYTES = 1024 * 1024


def _targets() -> list[tuple[str, Path]]:
    files = [
        (TRIP_URL_TEMPLATE.format(month=month), RAW_DIR / f"yellow_tripdata_{month}.parquet")
        for month in MONTHS
    ]
    # April is the Postgres sample month, not part of the parquet MONTHS backfill.
    files.append(
        (TRIP_URL_TEMPLATE.format(month=EXTRA_MONTH_PG), RAW_DIR / f"yellow_tripdata_{EXTRA_MONTH_PG}.parquet")
    )
    # May is the JSON sample month, not part of the parquet MONTHS backfill.
    files.append(
        (TRIP_URL_TEMPLATE.format(month=EXTRA_MONTH_JSON), RAW_DIR / f"yellow_tripdata_{EXTRA_MONTH_JSON}.parquet")
    )
    files.append((ZONE_LOOKUP_URL, RAW_DIR / "taxi_zone_lookup.csv"))
    return files


def _download(url: str, destination: Path) -> None:
    """Save url to destination. A file already on disk is left as-is."""
    if destination.exists():
        logger.info("skipping %s (%s bytes)", destination.name, destination.stat().st_size)
        return

    destination.parent.mkdir(parents=True, exist_ok=True)
    # Keep the incomplete body beside the real name so a crash cannot look complete.
    temporary = destination.with_name(destination.name + ".part")
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_error: Exception | None = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            logger.info("downloading %s (attempt %s/%s)", destination.name, attempt, MAX_ATTEMPTS)
            with urllib.request.urlopen(request, timeout=120) as response, temporary.open("wb") as handle:
                expected = response.headers.get("Content-Length")
                shutil.copyfileobj(response, handle, length=_CHUNK_BYTES)
            actual = temporary.stat().st_size
            if expected is not None and actual != int(expected):
                raise OSError(f"incomplete download ({actual} of {expected} bytes)")
            temporary.replace(destination)
            logger.info("saved %s (%s bytes)", destination.name, destination.stat().st_size)
            return
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = exc
            logger.warning("download failed for %s: %s", destination.name, exc)
            if temporary.exists():
                temporary.unlink()

    raise RuntimeError(f"failed to download {url}") from last_error


def download_data() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for url, destination in _targets():
        _download(url, destination)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    download_data()


if __name__ == "__main__":
    main()
