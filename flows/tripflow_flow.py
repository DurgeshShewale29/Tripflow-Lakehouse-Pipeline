"""Run the TripFlow pipeline locally, one Spark step per process."""

import os


# Local ephemeral API when no Prefect server or cloud account is configured.
os.environ["PREFECT_SERVER_EPHEMERAL_ENABLED"] = "true"
os.environ["DO_NOT_TRACK"] = "1"
os.environ["PREFECT_SERVER_ANALYTICS_ENABLED"] = "false"

import argparse
import subprocess
import sys
import time
from collections import deque
from pathlib import Path

from prefect import flow, get_run_logger, task

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_TAIL = 20


def _run_module(module: str, args: list[str]) -> None:
    """Run module with this interpreter. Keep only the last lines for failures."""
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    process = subprocess.Popen(
        [sys.executable, "-m", module, *args],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    tail: deque[str] = deque(maxlen=OUTPUT_TAIL)
    assert process.stdout is not None
    for line in process.stdout:
        tail.append(line.rstrip("\n"))
    return_code = process.wait()
    if return_code != 0:
        detail = "\n".join(tail)
        raise RuntimeError(f"{module} exited with code {return_code}\n{detail}")


@task(retries=2, retry_delay_seconds=10)
def run_module(name: str, module: str, args: list[str] | None = None) -> float:
    logger = get_run_logger()
    started = time.perf_counter()
    error: Exception | None = None
    try:
        _run_module(module, args or [])
    except Exception as exc:
        error = exc
    duration = time.perf_counter() - started
    logger.info("%s duration_s=%.1f", name, duration)
    if error is not None:
        raise error
    return duration


@flow(name="tripflow-pipeline")
def tripflow_pipeline(skip_download: bool = False, months: list[str] | None = None) -> None:
    logger = get_run_logger()
    started = time.perf_counter()
    summary: list[tuple[str, float]] = []

    steps: list[tuple[str, str, list[str] | None]] = []
    if not skip_download:
        steps.append(("download_data", "src.bronze.download_data", None))
    steps.append(("ingest_zones", "src.bronze.ingest_zones", None))
    steps.append(("ingest_trips", "src.bronze.ingest_trips", None))
    steps.append(("build_silver", "src.silver.build_silver", None))
    gold_args = ["--months", *months] if months else None
    steps.append(("build_gold", "src.gold.build_gold", gold_args))

    for name, module, args in steps:
        summary.append((name, run_module(name, module, args)))

    logger.info("total duration_s=%.1f", time.perf_counter() - started)
    for name, duration in summary:
        logger.info("step=%s duration_s=%.1f", name, duration)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the TripFlow pipeline locally.")
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--months", nargs="+")
    args = parser.parse_args()
    tripflow_pipeline(skip_download=args.skip_download, months=args.months)


if __name__ == "__main__":
    main()


# Schedule example, not enabled by default:
# tripflow_pipeline.serve(cron="0 2 * * *")
