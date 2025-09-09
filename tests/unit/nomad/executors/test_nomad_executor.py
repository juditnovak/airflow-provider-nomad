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
from datetime import datetime
from unittest.mock import ANY

import pytest
from airflow.executors.workloads import ExecuteTask
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from nomad.api.exceptions import BaseNomadException
from tests_common.test_utils.config import conf_vars

from airflow.providers.nomad.executors.nomad_executor import NomadExecutor

EXECUTOR = "airflow.providers.nomad.executors.nomad_executor.NomadExecutor"

DATE_VAL = (2016, 1, 1)
DEFAULT_DATE = datetime(*DATE_VAL)


@pytest.fixture
def taskinstance(create_task_instance) -> TaskInstance:
    return create_task_instance(
        dag_id="dag",
        task_id="task",
        run_type=DagRunType.SCHEDULED,
        logical_date=DEFAULT_DATE,
    )


@conf_vars({})
def test_base_defaults():
    nomad_executor = NomadExecutor()
    assert nomad_executor
    assert nomad_executor.parallelism == 128
    assert nomad_executor.nomad_server_ip == "127.0.0.1"
    assert nomad_executor.cert_path == ""
    assert nomad_executor.key_path == ""
    assert nomad_executor.verify == ""
    assert nomad_executor.secure == False  # noqa: E712


@conf_vars(
    {
        ("nomad_executor", "server_ip"): "1.2.3.4",
        ("nomad_executor", "parallelism"): "2",
    }
)
def test_base_fallback_default_params():
    nomad_executor = NomadExecutor()
    assert nomad_executor
    assert nomad_executor.parallelism == 2
    assert nomad_executor.nomad_server_ip == "1.2.3.4"
    assert nomad_executor.cert_path == ""
    assert nomad_executor.key_path == ""
    assert nomad_executor.verify == ""
    assert nomad_executor.secure == False  # noqa: E712


@conf_vars(
    {
        ("nomad_executor", "server_ip"): "1.2.3.4",
        ("nomad_executor", "cert_path"): "certs_absolute_path/global-cli-nomad.pem",
        ("nomad_executor", "key_path"): "certs_absolute_path/global-cli-nomad-key.pem",
        ("nomad_executor", "verify"): "certs_absolute_path/nomad-agent-ca.pem",
        ("nomad_executor", "secure"): "true",
    }
)
def test_base_params_secure():
    nomad_executor = NomadExecutor()
    assert nomad_executor
    assert nomad_executor.nomad_server_ip == "1.2.3.4"
    assert nomad_executor.cert_path == "certs_absolute_path/global-cli-nomad.pem"
    assert nomad_executor.key_path == "certs_absolute_path/global-cli-nomad-key.pem"
    assert nomad_executor.verify == "certs_absolute_path/nomad-agent-ca.pem"
    assert nomad_executor.secure == True  # noqa: E712


def test_base_params_secure_verify_bool():
    with conf_vars({("nomad_executor", "verify"): "cacert_path"}):
        nomad_executor = NomadExecutor()
        assert nomad_executor.verify == "cacert_path"

    with conf_vars({("nomad_executor", "verify"): "true"}):
        nomad_executor = NomadExecutor()
        assert nomad_executor.verify == True  # noqa: E712

    with conf_vars({("nomad_executor", "verify"): "false"}):
        nomad_executor = NomadExecutor()
        assert nomad_executor.verify == False  # noqa: E712


def test_connect():
    """Connection to the Nomad cluster"""

    nomad_executor = NomadExecutor()
    nomad_executor.start()

    assert nomad_executor.nomad
    assert nomad_executor.nomad.agent.get_members()


@pytest.mark.skipif(NomadExecutor is None, reason="nomad_provider python package is not installed")
def test_sync_ok(mock_nomad_client, taskinstance):
    """ """

    nomad_executor = NomadExecutor()
    nomad_executor.start()
    task = ExecuteTask.make(taskinstance)

    try:
        nomad_executor.execute_async(key=taskinstance.key, queue=None, command=[task])
        nomad_executor.sync()

        # A job was registered
        assert mock_nomad_client.job.register_job.call_count == 1

        assert nomad_executor.task_queue.empty()
        assert nomad_executor.event_buffer[taskinstance.key][0] == State.QUEUED
    finally:
        nomad_executor.end()
        pass


@pytest.mark.skipif(NomadExecutor is None, reason="nomad_provider python package is not installed")
def test_sync_run_failed(mock_nomad_client, caplog, taskinstance):
    error = "Connection broken: ConnectionResetError(104, 'Connection reset by peer')"

    mock_nomad_client.job.register_job.side_effect = BaseNomadException(error)

    with caplog.at_level(logging.ERROR):
        nomad_executor = NomadExecutor()
        nomad_executor.start()
        task = ExecuteTask.make(taskinstance)
        try:
            nomad_executor.execute_async(key=taskinstance.key, queue=None, command=[task])
            nomad_executor.sync()

            assert any([error in record.message for record in caplog.records])
            assert nomad_executor.task_queue.empty()
            assert nomad_executor.event_buffer[taskinstance.key][0] == State.FAILED
        finally:
            nomad_executor.end()
            pass


@pytest.mark.parametrize("job_tpl", ["simple_job.json", "complex_job.json"])
@pytest.mark.skipif(NomadExecutor is None, reason="nomad_provider python package is not installed")
def test_sync_def_template(job_tpl, mock_nomad_client, test_datadir, taskinstance):
    with conf_vars({("nomad_executor", "default_job_template"): str(test_datadir / job_tpl)}):
        nomad_executor = NomadExecutor()
        nomad_executor.start()
        task = ExecuteTask.make(taskinstance)

        try:
            nomad_executor.execute_async(key=taskinstance.key, queue=None, command=[task])
            nomad_executor.sync()

            # A job was registered
            assert mock_nomad_client.job.register_job.call_count == 1
            assert nomad_executor.task_queue.empty()
            assert nomad_executor.event_buffer[taskinstance.key][0] == State.QUEUED
        finally:
            nomad_executor.end()
            pass


@pytest.mark.skipif(NomadExecutor is None, reason="nomad_provider python package is not installed")
@pytest.mark.usefixtures("mock_nomad_client")
def test_sync_def_template_hcl_secure(test_datadir, mocker, taskinstance):
    """Checking if access to Nomad APU outside of the nomad client Python library
    has cert info added
    """

    mock_requests_post = mocker.patch("airflow.providers.nomad.utils.requests.post")

    ca_cert = "/absolute/path/to/ca-cert.pem"
    client_key = "/absolute/path/to/client-key.pem"
    client_cert = "/absolute/path/to/client-cert.pem"
    with conf_vars(
        {
            ("nomad_executor", "default_job_template"): str(test_datadir / "simple_batch.hcl"),
            ("nomad_executor", "verify"): ca_cert,
            ("nomad_executor", "key_path"): client_key,
            ("nomad_executor", "cert_path"): client_cert,
        }
    ):
        nomad_executor = NomadExecutor()
        nomad_executor.start()
        task = ExecuteTask.make(taskinstance)

        try:
            nomad_executor.execute_async(key=taskinstance.key, queue=None, command=[task])
            nomad_executor.sync()

            # Secure parameters were appliet on call to Python requests
            mock_requests_post.assert_called_once_with(
                ANY, verify=ca_cert, cert=(client_cert, client_key), data=ANY
            )
        finally:
            nomad_executor.end()
            pass


@pytest.mark.skipif(NomadExecutor is None, reason="nomad_provider python package is not installed")
@pytest.mark.usefixtures("mock_nomad_client")
def test_sync_def_template_hcl_not_secure(test_datadir, mocker, taskinstance):
    """Checking if access to Nomad APU outside of the nomad client Python library
    has cert info added
    """

    mock_requests_post = mocker.patch("airflow.providers.nomad.utils.requests.post")

    with conf_vars(
        {
            ("nomad_executor", "default_job_template"): str(test_datadir / "simple_batch.hcl"),
            ("nomad_executor", "verify"): "false",
            ("nomad_executor", "key_path"): "",
            ("nomad_executor", "cert_path"): "",
        }
    ):
        nomad_executor = NomadExecutor()
        nomad_executor.start()
        task = ExecuteTask.make(taskinstance)

        try:
            nomad_executor.execute_async(key=taskinstance.key, queue=None, command=[task])
            nomad_executor.sync()

            # Secure parameters weren't appliet on call to Python requests
            mock_requests_post.assert_called_once_with(ANY, data=ANY, verify=False)
        finally:
            nomad_executor.end()
            pass
