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


import json
from time import sleep

from tests_common.test_utils.config import conf_vars

from airflow.providers.nomad.job_manager import NomadJobManager
from airflow.providers.nomad.models import (
    NomadJobAllocationInfo,
    NomadJobEvaluationInfo,
    NomadJobSubmission,
    NomadJobSummary,
)


@conf_vars({})
def test_base_defaults():
    nomad_job_mgr = NomadJobManager()
    assert nomad_job_mgr
    assert nomad_job_mgr.nomad_server_ip == "127.0.0.1"
    assert nomad_job_mgr.cert_path == ""
    assert nomad_job_mgr.key_path == ""
    assert nomad_job_mgr.verify == ""
    assert nomad_job_mgr.secure == False  # noqa: E712


@conf_vars(
    {
        ("nomad_executor", "server_ip"): "1.2.3.4",
    }
)
def test_base_fallback_default_params():
    nomad_job_mgr = NomadJobManager()
    assert nomad_job_mgr
    assert nomad_job_mgr.nomad_server_ip == "1.2.3.4"
    assert nomad_job_mgr.cert_path == ""
    assert nomad_job_mgr.key_path == ""
    assert nomad_job_mgr.verify == ""
    assert nomad_job_mgr.secure == False  # noqa: E712


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
    nomad_job_mgr = NomadJobManager()
    assert nomad_job_mgr
    assert nomad_job_mgr.nomad_server_ip == "1.2.3.4"
    assert nomad_job_mgr.cert_path == "certs_absolute_path/global-cli-nomad.pem"
    assert nomad_job_mgr.key_path == "certs_absolute_path/global-cli-nomad-key.pem"
    assert nomad_job_mgr.verify == "certs_absolute_path/nomad-agent-ca.pem"
    assert nomad_job_mgr.secure == True  # noqa: E712


def test_base_params_secure_verify_bool():
    with conf_vars({("nomad_executor", "verify"): "cacert_path"}):
        nomad_job_mgr = NomadJobManager()
        assert nomad_job_mgr.verify == "cacert_path"

    with conf_vars({("nomad_executor", "verify"): "true"}):
        nomad_job_mgr = NomadJobManager()
        assert nomad_job_mgr.verify == True  # noqa: E712

    with conf_vars({("nomad_executor", "verify"): "false"}):
        nomad_job_mgr = NomadJobManager()
        assert nomad_job_mgr.verify == False  # noqa: E712


def test_get_allocations(mock_nomad_client, test_datadir):
    nomad_job_mgr = NomadJobManager()
    nomad_job_mgr.initialize()

    job_alloc = test_datadir / "nomad_job_allocations.json"
    mock_nomad_client.job.get_allocations.return_value = json.loads(open(job_alloc).read())
    allocations = nomad_job_mgr.get_nomad_job_allocation_info("somejob")
    assert allocations
    assert all(isinstance(alloc, NomadJobAllocationInfo) for alloc in allocations)

    mock_nomad_client.job.get_allocations.return_value = {"wrong": "input"}
    assert not nomad_job_mgr.get_nomad_job_allocation_info("somejob")


def test_get_job_status(mock_nomad_client, test_datadir):
    nomad_job_mgr = NomadJobManager()
    nomad_job_mgr.initialize()

    job_alloc = test_datadir / "nomad_job_summary_running.json"
    mock_nomad_client.job.get_summary.return_value = json.loads(open(job_alloc).read())
    assert isinstance(nomad_job_mgr.get_nomad_job_summary("somejob"), NomadJobSummary)

    mock_nomad_client.job.get_summary.return_value = {"wrong": "input"}
    assert not nomad_job_mgr.get_nomad_job_summary("somejob")


def test_get_job_evaluations(mock_nomad_client, test_datadir):
    nomad_job_mgr = NomadJobManager()
    nomad_job_mgr.initialize()

    job_alloc = test_datadir / "nomad_job_evaluation.json"
    mock_nomad_client.job.get_evaluations.return_value = json.loads(open(job_alloc).read())
    assert all(
        isinstance(evalu, NomadJobEvaluationInfo)
        for evalu in nomad_job_mgr.get_nomad_evaluations_info("somejob")  # type: ignore[reportOperationalIterable]
    )

    mock_nomad_client.job.get_evaluations.return_value = {"wrong": "input"}
    assert not nomad_job_mgr.get_nomad_evaluations_info("somejob")


def test_get_job_submission(mock_nomad_client, test_datadir):
    nomad_job_mgr = NomadJobManager()
    nomad_job_mgr.initialize()

    job_alloc = test_datadir / "nomad_job_info.json"
    mock_nomad_client.job.get_job.return_value = json.loads(open(job_alloc).read())
    assert isinstance(nomad_job_mgr.get_nomad_job_submission_info("somejob"), NomadJobSubmission)

    mock_nomad_client.job.get_job.return_value = {"wrong": "input"}
    assert not nomad_job_mgr.get_nomad_job_submission_info("somejob")


@conf_vars({("nomad_executor", "alloc_pending_timeout"): "0"})
def test_remove_job_if_hanging_good_job(mock_nomad_client, test_datadir):
    """Job submission is all good, job is not to be killed"""
    nomad_job_mgr = NomadJobManager()
    nomad_job_mgr.initialize()

    file_path1 = test_datadir / "nomad_job_evaluation.json"
    file_path2 = test_datadir / "nomad_job_info.json"
    file_path3 = test_datadir / "nomad_job_allocations.json"
    mock_nomad_client.job.get_evaluations.return_value = json.loads(open(file_path1).read())
    mock_nomad_client.job.get_job.return_value = json.loads(open(file_path2).read())
    mock_nomad_client.job.get_allocations.return_value = json.loads(open(file_path3).read())
    mock_kill = mock_nomad_client.job.deregister_job

    assert nomad_job_mgr.remove_job_if_hanging("somejob") == (False, "")
    sleep(1)
    assert nomad_job_mgr.remove_job_if_hanging("somejob") == (False, "")
    mock_kill.assert_not_called()


@conf_vars({("nomad_executor", "alloc_pending_timeout"): "0"})
def test_remove_job_if_hanging_evaluation_timeout(mock_nomad_client, test_datadir):
    """Job allocation failed, and it's passing configured timeout"""
    nomad_job_mgr = NomadJobManager()
    nomad_job_mgr.initialize()

    file_path1 = test_datadir / "nomad_job_evaluation_failed.json"
    file_path2 = test_datadir / "nomad_job_info_pending.json"
    file_path3 = test_datadir / "nomad_job_allocations.json"
    mock_nomad_client.job.get_evaluations.return_value = json.loads(open(file_path1).read())
    mock_nomad_client.job.get_job.return_value = json.loads(open(file_path2).read())
    mock_nomad_client.job.get_allocations.return_value = json.loads(open(file_path3).read())
    mock_kill = mock_nomad_client.job.deregister_job

    assert nomad_job_mgr.remove_job_if_hanging("somejob") == (False, "")
    sleep(1)
    assert nomad_job_mgr.remove_job_if_hanging("somejob") == (
        True,
        "[\"{'missing compatible host volumes': 1}\"]",
    )
    mock_kill.assert_called_once()


@conf_vars({("nomad_executor", "alloc_pending_timeout"): "100"})
def test_remove_job_if_hanging_no_evaluation_timeout(mock_nomad_client, test_datadir):
    """Job allocation failed, but we've been within the timeout so the job isn't killed"""
    nomad_job_mgr = NomadJobManager()
    nomad_job_mgr.initialize()

    file_path1 = test_datadir / "nomad_job_evaluation_failed.json"
    file_path2 = test_datadir / "nomad_job_info_pending.json"
    file_path3 = test_datadir / "nomad_job_allocations.json"
    mock_nomad_client.job.get_evaluations.return_value = json.loads(open(file_path1).read())
    mock_nomad_client.job.get_job.return_value = json.loads(open(file_path2).read())
    mock_nomad_client.job.get_allocations.return_value = json.loads(open(file_path3).read())
    mock_kill = mock_nomad_client.job.deregister_job

    assert nomad_job_mgr.remove_job_if_hanging("somejob") == (False, "")
    sleep(1)
    assert nomad_job_mgr.remove_job_if_hanging("somejob") == (False, "")
    mock_kill.assert_not_called()


@conf_vars({("nomad_executor", "alloc_pending_timeout"): "0"})
def test_remove_job_if_hanging_alloc_failure(mock_nomad_client, test_datadir):
    """Job evaluation failed, Nomad status is 'dead', job is killed ultimately"""
    nomad_job_mgr = NomadJobManager()
    nomad_job_mgr.initialize()

    file_path1 = test_datadir / "nomad_job_evaluation.json"
    file_path2 = test_datadir / "nomad_job_info_dead.json"
    file_path3 = test_datadir / "nomad_job_allocations_pending.json"
    file_path4 = test_datadir / "nomad_job_summary_failed.json"
    mock_nomad_client.job.get_evaluations.return_value = json.loads(open(file_path1).read())
    mock_nomad_client.job.get_job.return_value = json.loads(open(file_path2).read())
    mock_nomad_client.job.get_allocations.return_value = json.loads(open(file_path3).read())
    mock_nomad_client.job.get_summary.return_value = json.loads(open(file_path4).read())
    mock_kill = mock_nomad_client.job.deregister_job

    assert nomad_job_mgr.remove_job_if_hanging("somejob") == (
        True,
        "[{'example_bash_operator_judit_new-runme_0': "
        "{'Driver Failure': [\"Failed to pull `novakjudi/af_nomad_test:latest`: "
        "Error response from daemon: pull access denied for novakjudi/af_nomad_test, "
        "repository does not exist or may require 'docker login': "
        'denied: requested access to the resource is denied", "Failed to pull '
        "`novakjudi/af_nomad_test:latest`: Error response from daemon: pull "
        "access denied for novakjudi/af_nomad_test, repository does not exist or may "
        "require 'docker login': denied: requested access to the resource is denied\"]}}]",
    )
    mock_kill.assert_called_once()
