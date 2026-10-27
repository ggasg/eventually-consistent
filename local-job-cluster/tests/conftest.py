import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

FOLDER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(FOLDER / "code" / "app"))


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield session
    session.stop()
