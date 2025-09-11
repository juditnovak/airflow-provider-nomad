# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
import os
import sys
from pathlib import Path

import pytest
from airflow.configuration import conf

UNITTEST_ROOT = Path(__file__).resolve().parent
TEST_CONFIG_PATH = UNITTEST_ROOT / "config"
TEST_DATA_PATH = UNITTEST_ROOT / "data"

logger = logging.getLogger(__name__)

sys.path.append(
    os.environ.get("AIRFLOW_SOURCES", os.environ.get("PWD", ".") + "airflow")
    + "/airflow-core/tests"
)


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
    return mocker.patch(
        "airflow.providers.nomad.job_manager.nomad.Nomad", autospec=True
    ).return_value


@pytest.fixture
def unittest_root():
    return UNITTEST_ROOT


@pytest.fixture
def test_datadir():
    return TEST_DATA_PATH
