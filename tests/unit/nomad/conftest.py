import logging
from datetime import datetime
import os
import sys
from pathlib import Path

import pytest
from airflow.configuration import conf
from airflow.models.taskinstance import TaskInstance
from airflow.utils.types import DagRunType

UNITTEST_ROOT = Path(__file__).resolve().parent
TEST_CONFIG_PATH = UNITTEST_ROOT / "config"
TEST_DATA_PATH = UNITTEST_ROOT / "data"

logger = logging.getLogger(__name__)

sys.path.append(
    os.environ.get("AIRFLOW_SOURCES", os.environ.get("PWD", ".") + "airflow")
    + "/airflow-core/tests"
)

DATE_VAL = (2016, 1, 1)
DEFAULT_DATE = datetime(*DATE_VAL)


def pytest_configure(config):
    """Ahead of ***ANYTHING*** running, AIRFLOW_HOME/airflow.cfg must be generated"""
    if AIRFLOW_SOURCES := os.environ.get("AIRFLOW_SOURCES"):
        sys.path.append(AIRFLOW_SOURCES + "airflow-core/tests")
        sys.path.append(AIRFLOW_SOURCES + "airflow-core/tests/unit")
        sys.path.append(AIRFLOW_SOURCES + "airflow-core/src")
        logger.info(f"Active PYTHONPATH: {sys.path}")


@pytest.fixture(scope="session", autouse=True)
def load_airflow_config():
    conf.read_file(open(f"{TEST_CONFIG_PATH}/unit_tests.cfg"))


@pytest.fixture(autouse=True)
def mock_nomad_client(mocker):
    """Mock the Nomad client to avoid real connections during unit tests."""
    return mocker.patch("airflow.providers.nomad.manager.nomad.Nomad", autospec=True).return_value


@pytest.fixture
def unittest_root():
    return UNITTEST_ROOT


@pytest.fixture
def test_datadir():
    return TEST_DATA_PATH


@pytest.fixture
def taskinstance(create_task_instance) -> TaskInstance:
    return create_task_instance(
        dag_id="dag",
        task_id="task",
        run_type=DagRunType.SCHEDULED,
        logical_date=DEFAULT_DATE,
    )
