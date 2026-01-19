import copy
import json
import logging
from datetime import datetime
from queue import Empty
from time import sleep

import pytest
from airflow.executors.workloads import ExecuteTask, RunTrigger
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from nomad.api.exceptions import BaseNomadException
from sqlalchemy.orm import Session  # type: ignore[import-untyped]
from tests_common.test_utils.config import conf_vars

from airflow.providers.nomad.executors.nomad_executor import NomadExecutor
from airflow.providers.nomad.operators.job import NomadJobOperator
from airflow.providers.nomad.templates.job_template import (
    DEFAULT_IMAGE,
    DEFAULT_TASK_TEMPLATE,
    SDK_ENTRYPOINT,
)
from airflow.providers.nomad.models import NomadJobModel

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
    assert nomad_executor.nomad_mgr
    assert nomad_executor.serve_logs
    assert nomad_executor.supports_ad_hoc_ti_run
    assert nomad_executor.task_queue


def test_base_airflow2(mocker):
    mocker.patch(
        "airflow.providers.nomad.generic_interfaces.executor_interface.AIRFLOW_V_3_0_PLUS", False
    )
    with pytest.raises(RuntimeError) as err:
        NomadExecutor()
    assert "Nomad executor only available for Airflow > 3.0.0" in str(err.value)


@conf_vars({("nomad_provider", "parallelism"): "2"})
def test_base_fallback_default_params():
    nomad_executor = NomadExecutor()
    assert nomad_executor
    assert nomad_executor.parallelism == 2
    assert nomad_executor.nomad_mgr


def test_start(mocker):
    mock_mgr_init = mocker.patch("airflow.providers.nomad.manager.NomadManager.initialize")
    NomadExecutor().start()
    mock_mgr_init.assert_called_once()


def test_exec_async(taskinstance):
    nomad_executor = NomadExecutor()
    nomad_executor.execute_async(taskinstance.key, ["command"])
    item = nomad_executor.task_queue.get_nowait()
    assert (taskinstance.key, ["command"], None) == item


def test_queue_workload_ok(taskinstance):
    nomad_executor = NomadExecutor()
    task = ExecuteTask.make(taskinstance)
    nomad_executor.queue_workload(task, Session())
    assert task.ti.key in nomad_executor.queued_tasks


def test_queue_workload_bad_workload(caplog):
    nomad_executor = NomadExecutor()
    with caplog.at_level(logging.ERROR):
        nomad_executor.queue_workload(
            RunTrigger(id=1, ti=None, classpath="", encrypted_kwargs=""), Session()
        )
    assert "Workload of unsupported type:" in caplog.text
    # Nothing was added to the task queue
    with pytest.raises(Empty):
        nomad_executor.task_queue.get_nowait()


def test__process_workload_ok(taskinstance):
    """Normally internal functions may not be tested, but this one is kind of a "magic" one from BaseExecutor"""
    nomad_executor = NomadExecutor()
    task = ExecuteTask.make(taskinstance)

    # 1. Task is to be queued
    nomad_executor.queue_workload(task, Session())
    # 2. Queued workloads are processed
    nomad_executor._process_workloads([task])

    assert task.ti.key in nomad_executor.running
    assert task.ti.key not in nomad_executor.queued_tasks


def test__process_workload_bad_workload(caplog):
    """Normally internal functions may not be tested, but this one is kind of a "magic" one from BaseExecutor"""
    nomad_executor = NomadExecutor()
    running = copy.deepcopy(nomad_executor.running)
    queued = copy.deepcopy(nomad_executor.queued_tasks)
    with caplog.at_level(logging.ERROR):
        nomad_executor._process_workloads(
            [RunTrigger(id=1, ti=None, classpath="", encrypted_kwargs="")]
        )
    assert "Workload of unsupported type" in caplog.text
    # The task wasn't added to the run queue
    assert running == nomad_executor.running
    assert queued == nomad_executor.queued_tasks


@pytest.mark.parametrize(
    "state",
    [State.FAILED, State.SUCCESS, State.QUEUED, State.REMOVED, State.SCHEDULED, State.DEFERRED],
)
def test_queue_workload_set_ti_state_ok1(taskinstance, state):
    nomad_executor = NomadExecutor()
    task = ExecuteTask.make(taskinstance)

    # 1. Task is to be queued
    nomad_executor.queue_workload(task, Session())
    # 2. Queued workloads are processed
    nomad_executor._process_workloads([task])

    assert not nomad_executor.set_state(taskinstance.key, state)  # type: ignore [reportArgumentType]
    assert nomad_executor.event_buffer[taskinstance.key][0] == state
    assert task.ti.key not in nomad_executor.running


@pytest.mark.parametrize("state", [State.RUNNING, State.RESTARTING])
def test_queue_workload_set_ti_state_ok2(taskinstance, state):
    """The following state changes are ignored (to be adjusted by BaseOperator/scheduler)"""
    # NOTE: Is this surely the right thing to do...?
    nomad_executor = NomadExecutor()
    task = ExecuteTask.make(taskinstance)

    # 1. Task is to be queued
    nomad_executor.queue_workload(task, Session())
    # 2. Queued workloads are processed
    nomad_executor._process_workloads([task])

    assert not nomad_executor.set_state(taskinstance.key, state)  # type: ignore [reportArgumentType]
    assert nomad_executor.event_buffer[taskinstance.key][0] == State.QUEUED
    assert task.ti.key in nomad_executor.running


def test_queue_workload_set_ti_state_fails(taskinstance, caplog):
    nomad_executor = NomadExecutor()
    with caplog.at_level(logging.ERROR):
        assert not nomad_executor.set_state(taskinstance.key, None)  # type: ignore [reportArgumentType]
    assert nomad_executor.event_buffer[taskinstance.key][0] == State.FAILED
    assert "Unknown task state None, setting it as failed" in caplog.text


def test_workload_to_cmd(taskinstance):
    nomad_executor = NomadExecutor()
    task = ExecuteTask.make(taskinstance)
    assert nomad_executor.workload_to_command_args(task) == [task.model_dump_json()]


def test_run_task_bad_job(caplog):
    nomad_executor = NomadExecutor()
    nomad_executor.start()

    task_key = TaskInstanceKey.from_dict(
        {"dag_id": "dag_id", "task_id": "task_id", "run_id": "run_id"}
    )
    with caplog.at_level(logging.ERROR):
        assert not nomad_executor.run_task((task_key, ["wrong", "job"], None))

    assert "Workload of unsupported type" in caplog.text


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


def test_sync_exception1(mock_nomad_client, caplog):
    nomad_executor = NomadExecutor()
    nomad_executor.start()

    try:
        with caplog.at_level(logging.ERROR):
            # Ugly hack to simulate a bad task in the task queue
            nomad_executor.task_queue.put(("bad", "tuple"))  # type: ignore [reportArgumentType]
            nomad_executor.task_queue.task_done()

            nomad_executor.sync()

        # No job was registered
        assert mock_nomad_client.job.register_job.call_count == 0
        assert (
            "Failed to run task (task/command could be retrieved: (not enough values to unpack (expected 3, got 2)))"
        ) in caplog.text
    finally:
        nomad_executor.end()


def test_sync_exception2(mock_nomad_client, taskinstance, caplog):
    nomad_executor = NomadExecutor()
    nomad_executor.start()

    try:
        with caplog.at_level(logging.ERROR):
            nomad_executor.execute_async(key=taskinstance.key, queue=None, command=["badcommand"])
            nomad_executor.sync()

        # No job was registered
        assert mock_nomad_client.job.register_job.call_count == 0
        assert "Workload of unsupported type: 'badcommand'" in caplog.text
        assert nomad_executor.event_buffer[taskinstance.key][0] == State.FAILED
    finally:
        nomad_executor.end()


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


def test_sync_nomad_job_submission_failed_but_running_now(
    mock_nomad_client, taskinstance, test_datadir
):
    # Initial execution failed...
    file_path1 = test_datadir / "nomad_job_allocations_pending.json"
    # ...yet now there is a retry/re-allocation runnint
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

        # We simulate scenario for a task that's been started already
        nomad_executor.task_queue.get_nowait()
        nomad_executor.task_queue.task_done()

        nomad_executor.sync()

        assert nomad_executor.task_queue.empty()
        assert nomad_executor.event_buffer[taskinstance.key][0] == State.QUEUED
    finally:
        nomad_executor.end()


@pytest.mark.parametrize("job_tpl", ["simple_job.json", "complex_job.json"])
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


def test_prepare_job_template_wrong_setting_default_used(caplog):
    """In case of a wrong template setting, the default would be used"""
    with conf_vars({("nomad_provider", "default_job_template"): str("non-existent-file")}):
        nomad_executor = NomadExecutor()
        nomad_executor.start()
        task_key = TaskInstanceKey.from_dict(
            {"dag_id": "dag_id", "task_id": "task_id", "run_id": "run_id"}
        )

        try:
            with caplog.at_level(logging.ERROR):
                result_dict = nomad_executor.prepare_job_template(task_key, ["fake_command"])

            # The default template was used
            assert (
                result_dict["Job"]["TaskGroups"][0]["Name"]
                == DEFAULT_TASK_TEMPLATE["Job"]["TaskGroups"][0]["Name"]
            )
            assert (
                result_dict["Job"]["TaskGroups"][0]["Tasks"][0]["Env"]
                == DEFAULT_TASK_TEMPLATE["Job"]["TaskGroups"][0]["Tasks"][0]["Env"]
            )
            assert "Can't load or parse default job template" in caplog.text
        finally:
            nomad_executor.end()


def test_prepare_job_template_executor_config_defaults_enforced():
    executor_config = {
        "image": "python:3.12-alpine",
        "entrypoint": [],
        "args": ["date"],
    }
    result = {
        "image": "python:3.12-alpine",
        "entrypoint": SDK_ENTRYPOINT,
        "args": ["<command>"],
    }
    nomad_executor = NomadExecutor()
    command = "<command>"
    try:
        nomad_executor.start()
        task_key = TaskInstanceKey.from_dict(
            {"dag_id": "dag_id", "task_id": "task_id", "run_id": "run_id"}
        )

        result_dict = nomad_executor.prepare_job_template(task_key, [command], executor_config)
        assert result_dict["Job"]["TaskGroups"][0]["Tasks"][0]["Config"] == result
    finally:
        nomad_executor.end()


def test_prepare_job_template_executor_config_wrong_entrypoint(caplog):
    executor_config = {
        "entrypoint": ["wrong", "entrypoint"],
        "args": ["date"],
    }
    result = {
        "image": DEFAULT_IMAGE,
        "entrypoint": SDK_ENTRYPOINT,
        "args": ["<command>"],
    }
    nomad_executor = NomadExecutor()
    command = "<command>"
    try:
        nomad_executor.start()
        task_key = TaskInstanceKey.from_dict(
            {"dag_id": "dag_id", "task_id": "task_id", "run_id": "run_id"}
        )

        with caplog.at_level(logging.WARNING):
            result_dict = nomad_executor.prepare_job_template(task_key, [command], executor_config)

        assert (
            f"'entrypoint' should be used in a way that '{SDK_ENTRYPOINT} + <actual command>' will be used as 'args'"
            in caplog.text
        )
        assert result_dict["Job"]["TaskGroups"][0]["Tasks"][0]["Config"] == result
    finally:
        nomad_executor.end()


@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_prepare_job_template_executor_config_path(filename, test_datadir):
    nomad_executor = NomadExecutor()
    file_path = test_datadir / filename
    content = open(file_path).read()
    try:
        nomad_executor.start()
        task_key = TaskInstanceKey.from_dict(
            {"dag_id": "dag_id", "task_id": "task_id", "run_id": "run_id"}
        )

        result_dict = nomad_executor.prepare_job_template(
            task_key, ["cmd"], {"template_path": file_path}
        )

        content_validate = NomadJobModel.model_validate_json(content).Job.TaskGroups
        content_validate[0].Tasks[0].Config.args = SDK_ENTRYPOINT + ["cmd"]
        content_validate[0].Tasks[0].Name = "dag_id-task_id"
        assert result_dict["Job"]["TaskGroups"][0] == content_validate[0].model_dump(
            exclude_unset=True
        )
    finally:
        nomad_executor.end()


def test_prepare_job_template_executor_config_volumes():
    vol_data = {
        "test_cfg": {
            "AccessMode": "",
            "AttachmentMode": "",
            "MountOptions": None,
            "Name": "config",
            "PerAlloc": False,
            "ReadOnly": True,
            "Source": "config",
            "Sticky": False,
            "Type": "host",
        },
        "test_dags_folder": {
            "AccessMode": "",
            "AttachmentMode": "",
            "MountOptions": None,
            "Name": "dags",
            "PerAlloc": False,
            "ReadOnly": True,
            "Source": "dags",
            "Sticky": False,
            "Type": "host",
        },
        "test_logs_dest": {
            "AccessMode": "single-node-writer",
            "AttachmentMode": "file-system",
            "MountOptions": None,
            "Name": "logs",
            "PerAlloc": False,
            "ReadOnly": False,
            "Source": "airflow-logs",
            "Sticky": False,
            "Type": "host",
        },
    }
    vol_mounts_data = [
        {
            "Destination": "/opt/airflow/config",
            "PropagationMode": "private",
            "ReadOnly": True,
            "SELinuxLabel": "",
            "Volume": "test_cfg",
        },
        {
            "Destination": "/opt/airflow/dags",
            "PropagationMode": "private",
            "ReadOnly": True,
            "SELinuxLabel": "",
            "Volume": "test_dags_folder",
        },
        {
            "Destination": "/opt/airflow/logs",
            "PropagationMode": "private",
            "ReadOnly": False,
            "SELinuxLabel": "",
            "Volume": "test_logs_dest",
        },
    ]
    nomad_executor = NomadExecutor()

    try:
        nomad_executor.start()
        task_key = TaskInstanceKey.from_dict(
            {"dag_id": "dag_id", "task_id": "task_id", "run_id": "run_id"}
        )

        result_dict = nomad_executor.prepare_job_template(
            task_key, ["cmd"], {"volume_mounts": vol_mounts_data, "volumes": vol_data}
        )

        assert result_dict["Job"]["TaskGroups"][0]["Tasks"][0]["VolumeMounts"] == vol_mounts_data
        assert result_dict["Job"]["TaskGroups"][0]["Volumes"] == vol_data
    finally:
        nomad_executor.end()


@pytest.mark.usefixtures("mock_nomad_client")
def test_submit_same_nomad_job_twice(
    test_datadir, mock_nomad_client, create_task_instance_of_operator, caplog
):
    file_path1 = test_datadir / "nomad_job_summary_running.json"
    with open(file_path1) as file1:
        # The first job will be running by the 2nd time the query is issued
        mock_nomad_client.job.get_summary.side_effect = [None, json.loads(file1.read())]

    content_path = test_datadir / "simple_job.json"
    with open(content_path) as f:
        content = f.read()

    nomad_executor = NomadExecutor()
    nomad_executor.start()
    ti1 = create_task_instance_of_operator(
        NomadJobOperator, dag_id="d1", task_id="t1", template_content=content
    )
    ti2 = create_task_instance_of_operator(
        NomadJobOperator, dag_id="d2", task_id="t2", template_content=content
    )
    task1 = ExecuteTask.make(ti1)
    task2 = ExecuteTask.make(ti2)

    try:
        with caplog.at_level(logging.ERROR):
            nomad_executor.execute_async(key=ti1.key, queue=None, command=[task1])
            nomad_executor.execute_async(key=ti2.key, queue=None, command=[task2])
            nomad_executor.sync()

        assert mock_nomad_client.job.register_job.call_count == 1
        assert nomad_executor.task_queue.empty()
        assert nomad_executor.event_buffer[ti1.key][0] == State.QUEUED
        assert nomad_executor.event_buffer[ti2.key][0] == State.FAILED
        assert "Job d2-t2-test-0--1 already exists" in caplog.text
    finally:
        nomad_executor.end()
