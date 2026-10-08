"""Path constants for the lakehouse layers, relative to the project root."""

from pathlib import Path

# src/common/config.py -> parents[2] is the project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
QUARANTINE_DIR = PROJECT_ROOT / "data" / "quarantine"

# Add a "YYYY-MM" entry to extend the backfill.
MONTHS = ["2024-01", "2024-02", "2024-03"]

TRIP_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

BRONZE_TRIPS = BRONZE_DIR / "trips"
BRONZE_ZONES = BRONZE_DIR / "zones"

SILVER_TRIPS = SILVER_DIR / "trips"
SILVER_RECON = SILVER_DIR / "recon"
QUARANTINE_TRIPS = QUARANTINE_DIR / "trips"

GOLD_REVENUE_ZONE_HOUR = GOLD_DIR / "revenue_by_zone_hour"
GOLD_DAILY_SUMMARY = GOLD_DIR / "daily_summary"
GOLD_PAYMENT_BOROUGH = GOLD_DIR / "payment_borough"
