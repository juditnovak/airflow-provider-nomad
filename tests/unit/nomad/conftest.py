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

from __future__ import annotations

from pathlib import Path

import pytest
from airflow.configuration import conf

DATA_FILE_DIRECTORY = Path(__file__).resolve().parent / "data_files"
CONFIG_DIRECTORY = Path(__file__).resolve().parent / "config"


import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))

@pytest.fixture(scope="session", autouse=True)
def load_airflow_config():
    conf.read_file(open(f"{CONFIG_DIRECTORY}/unit_tests.cfg"))


@pytest.fixture(autouse=True)
def mock_nomad_client(mocker):
    """Mock the Nomad client to avoid real connections during unit tests."""
    return mocker.patch(
        "airflow.providers.nomad.executors.nomad_executor.nomad.Nomad", autospec=True
    )
