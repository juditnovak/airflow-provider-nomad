import json
import logging
from datetime import datetime
from time import sleep

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


@conf_vars({})
def test_base_defaults():
    nomad_executor = NomadExecutor()
    assert nomad_executor
    assert nomad_executor.parallelism == 128


@conf_vars({("nomad_provider", "parallelism"): "2"})
def test_base_fallback_default_params():
    nomad_executor = NomadExecutor()
    assert nomad_executor
    assert nomad_executor.parallelism == 2


@pytest.fixture
def taskinstance(create_task_instance) -> TaskInstance:
    return create_task_instance(
        dag_id="dag",
        task_id="task",
        run_type=DagRunType.SCHEDULED,
        logical_date=DEFAULT_DATE,
    )


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


@pytest.mark.skipif(NomadExecutor is None, reason="nomad_provider python package is not installed")
def test_sync_nomad_allocation_ok(mock_nomad_client, taskinstance, test_datadir):
    # nomad job status == 'running'
    file_path1 = test_datadir / "nomad_job_info.json"
    with open(file_path1) as file1:
        mock_nomad_client.job.get_job.return_value = json.loads(file1.read())

    nomad_executor = NomadExecutor()
    nomad_executor.start()
    task = ExecuteTask.make(taskinstance)
    try:
        nomad_executor.execute_async(key=taskinstance.key, queue=None, command=[task])
        # Faking that the task is running
        nomad_executor.running.add(taskinstance.key)

        nomad_executor.sync()

        assert nomad_executor.task_queue.empty()
        assert nomad_executor.event_buffer[taskinstance.key][0] == State.QUEUED
    finally:
        nomad_executor.end()
        pass


@pytest.mark.skipif(NomadExecutor is None, reason="nomad_provider python package is not installed")
def test_sync_nomad_allocation_failing_timeout(
    mock_nomad_client, caplog, taskinstance, test_datadir
):
    error = {"missing compatible host volumes": 1}
    file_path1 = test_datadir / "nomad_job_evaluation_failed.json"
    file_path2 = test_datadir / "nomad_job_info_pending.json"
    with open(file_path1) as file1, open(file_path2) as file2:
        mock_nomad_client.job.get_evaluations.return_value = json.loads(file1.read())
        mock_nomad_client.job.get_job.return_value = json.loads(file2.read())

    with conf_vars({("nomad_provider", "alloc_pending_timeout"): "1"}):
        with caplog.at_level(logging.INFO):
            nomad_executor = NomadExecutor()
            nomad_executor.start()
            task = ExecuteTask.make(taskinstance)
            try:
                nomad_executor.execute_async(key=taskinstance.key, queue=None, command=[task])
                # Faking that the task is running
                nomad_executor.running.add(taskinstance.key)

                nomad_executor.sync()
                assert nomad_executor.task_queue.empty()
                assert nomad_executor.event_buffer[taskinstance.key][0] == State.QUEUED

                sleep(3)
                nomad_executor.sync()

                assert nomad_executor.task_queue.empty()
                assert nomad_executor.event_buffer[taskinstance.key][0] == State.FAILED
                assert any([str(error) in record.message for record in caplog.records])
            finally:
                nomad_executor.end()
                pass


@pytest.mark.skipif(NomadExecutor is None, reason="nomad_provider python package is not installed")
def test_sync_nomad_allocation_failing_within_timeout(
    mock_nomad_client, caplog, taskinstance, test_datadir
):
    error = {"missing compatible host volumes": 1}
    file_path1 = test_datadir / "nomad_job_evaluation_failed.json"
    file_path2 = test_datadir / "nomad_job_info_pending.json"
    with open(file_path1) as file1, open(file_path2) as file2:
        mock_nomad_client.job.get_evaluations.return_value = json.loads(file1.read())
        mock_nomad_client.job.get_job.return_value = json.loads(file2.read())

    with conf_vars({("nomad_provider", "alloc_pending_timeout"): "100"}):
        with caplog.at_level(logging.INFO):
            nomad_executor = NomadExecutor()
            nomad_executor.start()
            task = ExecuteTask.make(taskinstance)
            try:
                nomad_executor.execute_async(key=taskinstance.key, queue=None, command=[task])
                # Faking that the task is running
                nomad_executor.running.add(taskinstance.key)

                nomad_executor.sync()
                assert nomad_executor.task_queue.empty()
                assert nomad_executor.event_buffer[taskinstance.key][0] == State.QUEUED

                nomad_executor.sync()

                assert not any([str(error) in record.message for record in caplog.records])
                assert nomad_executor.task_queue.empty()
                assert nomad_executor.event_buffer[taskinstance.key][0] == State.QUEUED
            finally:
                nomad_executor.end()
                pass


@pytest.mark.skipif(NomadExecutor is None, reason="nomad_provider python package is not installed")
def test_sync_nomad_job_submission_fails(mock_nomad_client, caplog, taskinstance, test_datadir):
    error = "Error response from daemon: pull access denied for novakjudi/af_nomad_test"

    file_path1 = test_datadir / "nomad_job_allocations_pending.json"
    file_path2 = test_datadir / "nomad_job_summary_failed.json"
    file_path3 = test_datadir / "nomad_job_info_dead.json"
    with open(file_path1) as file1, open(file_path2) as file2, open(file_path3) as file3:
        mock_nomad_client.job.get_allocations.return_value = json.loads(file1.read())
        mock_nomad_client.job.get_summary.return_value = json.loads(file2.read())
        mock_nomad_client.job.get_job.return_value = json.loads(file3.read())

    with caplog.at_level(logging.INFO):
        nomad_executor = NomadExecutor()
        nomad_executor.start()
        task = ExecuteTask.make(taskinstance)
        try:
            nomad_executor.execute_async(key=taskinstance.key, queue=None, command=[task])
            # Faking that the task is running
            nomad_executor.running.add(taskinstance.key)

            nomad_executor.sync()

            assert nomad_executor.task_queue.empty()
            assert nomad_executor.event_buffer[taskinstance.key][0] == State.FAILED
            assert any([str(error) in record.message for record in caplog.records])
        finally:
            nomad_executor.end()
            pass


@pytest.mark.skipif(NomadExecutor is None, reason="nomad_provider python package is not installed")
def test_sync_nomad_job_submission_failed_but_running_now(
    mock_nomad_client, taskinstance, test_datadir
):
    file_path1 = test_datadir / "nomad_job_allocations_pending.json"
    file_path2 = test_datadir / "nomad_job_summary_running.json"
    file_path3 = test_datadir / "nomad_job_info_dead.json"
    with open(file_path1) as file1, open(file_path2) as file2, open(file_path3) as file3:
        mock_nomad_client.job.get_allocations.return_value = json.loads(file1.read())
        mock_nomad_client.job.get_summary.return_value = json.loads(file2.read())
        mock_nomad_client.job.get_job.return_value = json.loads(file3.read())

    nomad_executor = NomadExecutor()
    nomad_executor.start()
    task = ExecuteTask.make(taskinstance)
    try:
        nomad_executor.execute_async(key=taskinstance.key, queue=None, command=[task])
        # Faking that the task is running
        nomad_executor.running.add(taskinstance.key)

        nomad_executor.sync()

        assert nomad_executor.task_queue.empty()
        assert nomad_executor.event_buffer[taskinstance.key][0] == State.QUEUED
    finally:
        nomad_executor.end()
        pass


@pytest.mark.parametrize("job_tpl", ["simple_job.json", "complex_job.json"])
@pytest.mark.skipif(NomadExecutor is None, reason="nomad_provider python package is not installed")
def test_sync_def_template(job_tpl, mock_nomad_client, test_datadir, taskinstance):
    with conf_vars({("nomad_provider", "default_job_template"): str(test_datadir / job_tpl)}):
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
def test_sync_def_template_hcl(test_datadir, mock_nomad_client, taskinstance):
    with conf_vars(
        {("nomad_provider", "default_job_template"): str(test_datadir / "simple_job_batch.hcl")}
    ):
        file_path1 = test_datadir / "simple_batch_api_retval.json"
        with open(file_path1) as file1:
            mock_nomad_client.jobs.parse.return_value = json.loads(file1.read())

        nomad_executor = NomadExecutor()
        nomad_executor.start()
        task = ExecuteTask.make(taskinstance)

        try:
            nomad_executor.execute_async(key=taskinstance.key, queue=None, command=[task])
            nomad_executor.sync()

            assert mock_nomad_client.job.register_job.call_count == 1
            assert nomad_executor.task_queue.empty()
            assert nomad_executor.event_buffer[taskinstance.key][0] == State.QUEUED
        finally:
            nomad_executor.end()
            pass
