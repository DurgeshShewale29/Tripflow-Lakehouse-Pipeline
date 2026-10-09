"""PostgreSQL connection settings from the environment or a local .env file."""

import os
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from src.common.config import PROJECT_ROOT

_SETTINGS = ("PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASSWORD")


def load_project_env() -> None:
    """Load project-root .env values that are not already set."""
    path = PROJECT_ROOT / ".env"
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def jdbc_options() -> dict[str, str]:
    """Spark JDBC options. The password is not URL-encoded."""
    load_project_env()
    missing = [key for key in _SETTINGS if not os.environ.get(key)]
    if missing:
        raise RuntimeError(f"missing Postgres settings: {', '.join(missing)}")
    host = os.environ["PG_HOST"]
    port = os.environ["PG_PORT"]
    database = os.environ["PG_DB"]
    return {
        "url": f"jdbc:postgresql://{host}:{port}/{database}",
        "user": os.environ["PG_USER"],
        "password": os.environ["PG_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }


def get_engine() -> Engine:
    load_project_env()
    missing = [key for key in _SETTINGS if not os.environ.get(key)]
    if missing:
        raise RuntimeError(f"missing Postgres settings: {', '.join(missing)}")
    user = quote_plus(os.environ["PG_USER"])
    password = quote_plus(os.environ["PG_PASSWORD"])
    host = os.environ["PG_HOST"]
    port = os.environ["PG_PORT"]
    database = os.environ["PG_DB"]
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)
