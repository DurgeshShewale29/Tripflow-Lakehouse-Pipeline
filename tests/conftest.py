"""Shared Spark fixture for the local test session."""

import pytest

from src.common.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("tripflow-tests", testing=True)
    yield session
    session.stop()
