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

import os
import subprocess
import time
from pathlib import Path

import pytest
from airflow.configuration import conf

CONFIG_DIRECTORY = Path(__file__).resolve().parent / "config"
SCRIPTS_DIRECTORY = Path(__file__).resolve().parent / "scripts"


@pytest.fixture(scope="session", autouse=True)
def load_airflow_config():
    conf.read_file(open(f"{CONFIG_DIRECTORY}/unit_tests.cfg"))


@pytest.fixture(autouse=True)
def nomad_agent():
    path = os.environ["PATH"]
    dir_path = os.path.dirname(os.path.realpath(__file__))

    daemon = subprocess.Popen(
        [
            "sudo",
            "env",
            f"PATH={path}",
            "nomad",
            "agent",
            "-dev",
            "-config",
            dir_path / CONFIG_DIRECTORY / Path("nomad_client.hcl"),
        ]
    )

    time.sleep(5)  # wait for the agent to start

    subprocess.run(
        [
            dir_path / SCRIPTS_DIRECTORY / Path("create_dynamic_logs_volume.sh"),
            dir_path / CONFIG_DIRECTORY / Path("volume_dynamic_logs.json"),
        ]
    )

    print(f"Started Nomad agent (PID: {daemon.pid})")
    yield daemon

    print(f"Stopping Nomad agent (PID: {daemon.pid})")
    daemon.terminate()
