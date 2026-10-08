"""Path constants for the lakehouse layers, relative to the project root."""

from pathlib import Path

# src/common/config.py -> parents[2] is the project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
QUARANTINE_DIR = PROJECT_ROOT / "data" / "quarantine"
