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
import shutil
import subprocess
import sys
import time
from pathlib import Path

import netifaces
import pytest
from airflow.configuration import conf

from .utils import check_service_available, say_yes_or_no, stream_subprocess_output, update_template

logger = logging.getLogger(__name__)


# Test global setup

SYSTEST_ROOT = Path(__file__).resolve().parent
TEST_CONFIG_PATH = SYSTEST_ROOT / "config"
SCRIPTS_PATH = SYSTEST_ROOT / "scripts"

# Runner setup and workspace
NOMAD_RUNNER_ROOT = SYSTEST_ROOT / "runner"
NOMAD_CONFIG_PATH = NOMAD_RUNNER_ROOT / "config"
NOMAD_RUNNER_LOGS = NOMAD_RUNNER_ROOT / Path("logs")
NOMAD_SCRIPTS_PATH = NOMAD_RUNNER_ROOT / "scripts"

# Servers setup and workspace
AIRFLOW_SERVICES_ROOT = SYSTEST_ROOT / "server"

#
# Airflow's pytest module is enforcing to have AIRFLOW_HOME set, however it allows
# for the user to override defaults with OS the AIRFLOW_HOME environment variable.
# Since Nomad tests require specific settings, re-generate airflow.cfg, etc.
# we try to prevent fellow developers not to have their environment altered unexpectedly.
#
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
if AIRFLOW_HOME and AIRFLOW_HOME != str(AIRFLOW_SERVICES_ROOT / "airflow_home"):
    print("*************************************************************************")
    print("*                   !!!!!!   WARNING  !!!!!!!!                          *")
    print("*                                                                       *")
    print(f"* Using existing AIRFLOW_HOME: {AIRFLOW_HOME}*")
    print("* The tests are modifying airflow.cfg in this location (reset DB, etc.) ")
    print(f"* (Recommended setting: AIRFLOW_HOME={str(AIRFLOW_SERVICES_ROOT / 'airflow_home')})")
    print("*                                                                       *")
    print("*************************************************************************")
    print("Are you sure you want to continue?")
    print("")

    try:
        userinput = say_yes_or_no()
    except Exception as err:
        print(
            f"An issue with AIRFLOW_HOME was detected and no interactive decision could be processed ({err})."
        )
        exit(1)

    if not userinput:
        print("\nExiting...")
        exit(0)

elif not AIRFLOW_HOME:
    AIRFLOW_HOME = str(AIRFLOW_SERVICES_ROOT / "airflow_home")

AIRFLOW_HOME_PATH = Path(AIRFLOW_HOME)

sys.path.append(
    os.environ.get("AIRFLOW_SOURCES", os.environ.get("PWD", ".") + "airflow")
    + "/airflow-core/tests"
)

##############################################################################
# Test environment
##############################################################################

TEST_ENV_API_IP_NETIFACE = "TEST_AIRFLOW_API_IP_NETIFACE"
TEST_ENV_API_HOST = "TEST_AIRFLOW_API_HOST"


def pytest_addoption(parser):
    # Whether to run a Nomad agent
    agent_help = "Don't start a Nomad agent. (Typically: when running one manually.)"
    parser.addoption("--no-nomad-agent", action="store_false", dest="nomad_agent", help=agent_help)
    parser.addoption(
        "--nomad-agent",
        action="store_true",
        dest="nomad_agent",
        default=True,
        help=agent_help,
    )

    # Local network interface to use for Airflow IP
    parser.addoption(
        "--api-ip-netiface",
        action="store",
        default="eth0",
        help=f"Automatically use IP address bound to this network interface. (Alternative: {TEST_ENV_API_IP_NETIFACE}) IMPORTANT: Nomad runners can't use localhost/127.0.0.1, as they run in Docker. They need a different IP/hostname the Airflow API.",
    )

    # Airflow API IP/hostname
    parser.addoption(
        "--airflow-api-host",
        action="store",
        help="Hostname or IP of the Airflow API server. (Alternative: {TEST_ENV_API_HOST}) See also --api-ip-netiface",
    )


@pytest.fixture(scope="session")
def option_nomad_agent(request):
    return request.config.getoption("nomad_agent")


@pytest.fixture(scope="session")
def option_airflow_api_host(request):
    return request.config.getoption("airflow_api_host")


@pytest.fixture(scope="session")
def option_airflow_api_ip_netiface(request):
    return request.config.getoption("api_ip_netiface")


def pytest_configure(config):
    """Ahead of ***ANYTHING*** running, AIRFLOW_HOME/airflow.cfg must be generated"""
    unit_test_conf = airflow_test_servces_config()
    airflow_test_servces_setup(unit_test_conf)


##############################################################################
# Services setup (server/runner)
##############################################################################


@pytest.fixture(autouse=True, scope="session")
def nomad_runner_config(option_airflow_api_ip_netiface, option_airflow_api_host):
    """The runners need a different airflow.cfg than the servers.
    Reason: different local environment (example: dag-file = /opt/airflow/dags)
    """
    addr = os.environ.get(TEST_ENV_API_HOST, option_airflow_api_host)

    if not addr:
        iface = os.environ.get(TEST_ENV_API_IP_NETIFACE, option_airflow_api_ip_netiface)
        addr = netifaces.ifaddresses(iface)[netifaces.AF_INET][0]["addr"]

        if not addr:
            logger.error("Could not determine IP address for interface %s", iface)
            logger.error(
                "(Note: default is eth0, configurable via ENV VAR %s)",
                TEST_ENV_API_IP_NETIFACE,
            )
            exit(1)

    if not check_service_available(addr, 8080):
        logger.error("Airflow web API is is not available at http:{ip}:8080.")
        logger.error(
            "Airflow services (except the ones targeted by the tests) should be started manually."
        )
        exit(1)

    nomad_runner_config = NOMAD_CONFIG_PATH / Path("airflow.cfg")
    update_template(nomad_runner_config, {"<API_IP>": addr})


@pytest.fixture(autouse=True, scope="session")
def nomad_agent(nomad_runner_config, option_nomad_agent):
    log_volume_creation_script = str(NOMAD_SCRIPTS_PATH / Path("create_dynamic_logs_volume.sh"))
    log_volume_creation_hcl = str(NOMAD_CONFIG_PATH / Path("volume_dynamic_logs.json"))

    daemon = None
    nomad_log = ""
    if option_nomad_agent:
        path = os.environ["PATH"]

        # Prepare necessary directories
        NOMAD_RUNNER_LOGS.mkdir(parents=True, exist_ok=True)

        # Update configuration templates
        nomad_client_config = NOMAD_CONFIG_PATH / Path("nomad_client.hcl")
        nomad_log = NOMAD_RUNNER_LOGS / Path("nomad_agent.log")

        # update_template(airflow_server_config, {"<AIRFLOW_TEST_HOME>": systest_root})
        update_template(nomad_client_config, {"<SYS_TEST_ROOT>": str(SYSTEST_ROOT)})

        # Start Nomad agent (dev mode)
        logger.info("Starting Nomad agent with root privileges...")
        logger.warning("[NOTE: This 'sudo' call may mess up your terminal.]")
        daemon = None
        cmd = [
            "sudo",
            "env",
            f"PATH={path}",
            "nomad",
            "agent",
            "-dev",
            "-config",
            nomad_client_config,
        ]
        try:
            daemon = subprocess.Popen(
                cmd,
                universal_newlines=True,
                stdout=open(nomad_log, "w"),
                stderr=open(nomad_log, "w"),
            )
        except subprocess.CalledProcessError as err:
            logging.error("Starting Nomad agent failed (%s)", str(err))

        if daemon:
            logger.info(f"Nomad agent started (PID: {str(daemon.pid)})")

        for i in range(3):
            time.sleep(5)
            try:
                for line in stream_subprocess_output(
                    [log_volume_creation_script, log_volume_creation_hcl]
                ):
                    logger.info(line.strip())
            except subprocess.CalledProcessError as err:
                if i < 2:
                    logger.error("Could not create dynamic logs volume: %s. Retrying...", err)
                else:
                    logger.error("Dynamic logs volume createion failed. Exciting...")
                    exit(1)
            else:
                break
    else:
        logger.warning("Keep in mind that dynamic log volumes are necessary. See ")

    timeout = 30
    if not check_service_available("127.0.0.1", 4646, timeout=timeout):
        logger.error(
            "Nomad agent unavailable within %d seconds. (See logs at: %s)",
            timeout,
            nomad_log,
        )
        logger.error(
            "Alternatively you can run Nomad agent manually, and use the --no-nomad-agent parameter."
        )
        if daemon:
            daemon.terminate()
        exit(1)

    yield daemon

    if daemon:
        logger.info(f"Stopping Nomad agent (PID: {daemon.pid})")
        daemon.terminate()


# Though not fixtures, still rather left here for readability


def airflow_test_servces_config():
    airflow_unit_test_config = SYSTEST_ROOT / TEST_CONFIG_PATH / Path("unit_tests.cfg")
    update_template(
        airflow_unit_test_config,
        {
            "<SYSTEST_ROOT>": str(SYSTEST_ROOT),
            "<SYSTEST_AIRFLOW_HOME>": str(AIRFLOW_HOME_PATH),
        },
    )

    # Load System Test configuration
    conf.read_file(open(f"{TEST_CONFIG_PATH}/unit_tests.cfg"))
    logger.debug("Airflow config loaded for system tests")
    logger.info(f"The executor is: {conf.get('core', 'executor')}")

    return airflow_unit_test_config


def airflow_test_servces_setup(airflow_test_servces_config):
    airflow_cfg = AIRFLOW_HOME_PATH / Path("airflow.cfg")
    logger.info(f"Setting up Airflow test services using {airflow_cfg}")
    logger.warning(
        f"IMPORTANT: Any Airflow services already running may NOT be using updated {airflow_cfg} yet!"
    )
    logger.warning(
        f"NOTE: Run '{SCRIPTS_PATH}/init_airflow_cfg.sh' before starting up Airflow services"
    )
    try:
        airflow_cfg.symlink_to(airflow_test_servces_config)
    except FileExistsError:
        if not airflow_cfg.is_symlink():
            backup_path = str(airflow_cfg) + f".bak.{time.strftime('%Y%m%d-%H%M%S')}"
            shutil.move(str(airflow_cfg), backup_path)
            logger.warning(f"Moved old {airflow_cfg} to {backup_path}")
            airflow_cfg.symlink_to(airflow_test_servces_config)
        else:
            logger.warning("File {airflow_cfg} was a symlink that's now deleted.")
            os.unlink(airflow_cfg)
            airflow_cfg.symlink_to(airflow_test_servces_config)
