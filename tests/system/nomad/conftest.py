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

import netifaces
import logging
import os
import select
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest
from airflow.configuration import conf

logger = logging.getLogger(__name__)


SYSTEST_ROOT = Path(__file__).resolve().parent
TEST_CONFIG_DIRECTORY = SYSTEST_ROOT / "config"
SERVER_ROOT = SYSTEST_ROOT / "server"
SERVER_LOGS = SERVER_ROOT / Path("logs")

NOMAD_CONFIG_DIRECTORY = TEST_CONFIG_DIRECTORY / "nomad"
SCRIPTS_DIRECTORY = SYSTEST_ROOT / "scripts"

#
# Airflow's pytest module is enforcing to have AIRFLOW_HOME set, however it allows
# for the user to override defaults with OS the AIRFLOW_HOME environment variable.
# Since Nomad tests require specific settings, re-generate airflow.cfg, etc.
# we try to prevent fellow developers not to have their environment altered unexpectedly.
#
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
if AIRFLOW_HOME and AIRFLOW_HOME != str(SERVER_ROOT / "airflow_home"):
    logger.warning(
        "*************************************************************************"
    )
    logger.warning("Using existing AIRFLOW_HOME: %s", AIRFLOW_HOME)
    logger.warning(
        "The tests are modifying airflow.cfg in this location (resetting DB, etc.)"
    )
    logger.warning(
        f"(Recommended setting: AIRFLOW_HOME={str(SERVER_ROOT / 'airflow_home')})"
    )
    logger.warning(
        "*************************************************************************"
    )
    logger.warning("")
    print("Are you sure? (Waiting 10 seconds...)")
    i, o, e = select.select([sys.stdin], [], [], 10)
    if sys.stdin.readline().strip().lower() not in ["y", "Y", "yes"]:
        exit(0)
elif not AIRFLOW_HOME:
    AIRFLOW_HOME = str(SERVER_ROOT / "airflow_home")

AIRFLOW_HOME_PATH = Path(AIRFLOW_HOME)

sys.path.append(
    os.environ.get("AIRFLOW_SOURCES", os.environ.get("PWD", ".") + "airflow")
    + "/airflow-core/tests"
)


def update_template(file_path: Path, replacements: dict[str, str]) -> None:
    """Update placeholders in a template file with actual values."""
    infile_path = f"{file_path}.template"
    outfile_path = file_path

    with open(infile_path, "r") as file:
        content = file.read()
    for placeholder, actual in replacements.items():
        content = content.replace(placeholder, actual)
    with open(outfile_path, "w") as file:
        file.write(content)
    logger.debug("Rendered template for %s", file_path)



@pytest.fixture(autouse=True, scope="session")
def nomad_runner_config():
    iface_env_Var = "TEST_AIRFLOW_API_SERVER_IP"
    iface = os.environ.get(iface_env_Var, "eth0")

    ip = netifaces.ifaddresses(iface)[netifaces.AF_INET][0]['addr']
    if not ip:
        logger.error("Could not determine IP address for interface %s", iface)
        logger.error("(Note: default is eth0, configurable via ENV VAR %s)", iface_env_Var)
        exit(1)

    nomad_runner_config = TEST_CONFIG_DIRECTORY / Path("airflow.cfg")
    update_template(nomad_runner_config, {"<API_IP>": ip},
    )


@pytest.fixture(autouse=True, scope="session")
def nomad_agent():
    path = os.environ["PATH"]
    systest_root = os.path.dirname(os.path.realpath(__file__))

    # Prepare necessary directories
    SERVER_LOGS.mkdir(parents=True, exist_ok=True)

    # Update configuration templates
    # airflow_server_config = systest_root / AIRFLOW_CONFIG_DIRECTORY / Path("airflow.cfg")
    nomad_client_config = (
        systest_root / NOMAD_CONFIG_DIRECTORY / Path("nomad_client.hcl")
    )
    nomad_log = SERVER_LOGS / Path("nomad_agent.log")

    # update_template(airflow_server_config, {"<AIRFLOW_TEST_HOME>": systest_root})
    update_template(nomad_client_config, {"<SYS_TEST_ROOT>": systest_root})

    # Start Nomad agent (dev mode)
    daemon = subprocess.Popen(
        [
            "sudo",
            "env",
            f"PATH={path}",
            "nomad",
            "agent",
            "-dev",
            "-config",
            nomad_client_config,
        ],
        stdout=open(nomad_log, "w"),
        stderr=subprocess.STDOUT,
    )

    print(f"Starting Nomad agent (PID: {daemon.pid})")
    time.sleep(5)  # let the agent start up

    # Create dynamic logs volume for Nomad client
    subprocess.run(
        [
            systest_root / SCRIPTS_DIRECTORY / Path("create_dynamic_logs_volume.sh"),
            systest_root / NOMAD_CONFIG_DIRECTORY / Path("volume_dynamic_logs.json"),
        ],
    )

    yield daemon

    print(f"Stopping Nomad agent (PID: {daemon.pid})")
    daemon.terminate()


@pytest.fixture(scope="session", autouse=True)
def airflow_test_servces_config(nomad_agent):
    airflow_unit_test_config = (
        SYSTEST_ROOT / TEST_CONFIG_DIRECTORY / Path("unit_tests.cfg")
    )
    update_template(
        airflow_unit_test_config,
        {
            "<SYSTEST_ROOT>": str(SYSTEST_ROOT),
            "<SYSTEST_AIRFLOW_HOME>": str(AIRFLOW_HOME_PATH),
        },
    )

    # Load System Test configuration
    conf.read_file(open(f"{TEST_CONFIG_DIRECTORY}/unit_tests.cfg"))
    logger.debug("Airflow config loaded for system tests")

    return airflow_unit_test_config


@pytest.fixture(scope="session", autouse=True)
def airflow_test_servces_setup(airflow_test_servces_config):
    airflow_cfg = AIRFLOW_HOME_PATH / Path("airflow.cfg")
    logger.debug(f"Setting up Airflow test services using {airflow_cfg}")
    try:
        airflow_cfg.symlink_to(airflow_test_servces_config)
    except FileExistsError:
        if not airflow_cfg.is_symlink():
            backup_path = str(airflow_cfg) + f".bak.{time.strftime('%Y%m%d-%H%M%S')}"
            shutil.move(str(airflow_cfg), backup_path)
            logger.warning(f"Moved {airflow_cfg} to backup file")
            airflow_cfg.symlink_to(airflow_test_servces_config)
        else:
            logger.warning("File {airflow_cfg} was a symlink that's now deleted.")
            os.unlink(airflow_cfg)
            airflow_cfg.symlink_to(airflow_test_servces_config)


# @pytest.fixture(scope="session", autouse=True)
# def airflow_test_servces_api_start(airflow_test_servces_config):
#     # webapi_log = SERVER_LOGS / Path("airflow_api.log")
#     daemon = subprocess.Popen(
#         ["uv", "run", "--active", "airflow", "api-server"],
#         # stderr=subprocess.STDOUT,
#     )
#
#     print(f"Starting Airflow API (PID: {daemon.pid})")
#     time.sleep(10)
#
#     yield daemon
#
#     print(f"Stopping Airflow web API (PID: {daemon.pid})")
#     daemon.terminate()
