import logging.config
from collections.abc import Callable
from datetime import datetime
from importlib import reload

import pendulum
import pytest
from airflow.executors import executor_loader
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunTriggeredByType, DagRunType
from tests_common.test_utils.compat import PythonOperator
from tests_common.test_utils.config import conf_vars

from airflow.providers.nomad.executors.nomad_executor import NomadExecutor
from airflow.providers.nomad.executors.nomad_log import NOMAD_LOG_CONFIG, NomadLogHandler
from airflow.providers.nomad.generic_interfaces.executor_log_handlers import ExecutorLogLinesHandler

DATE_VAL = (2016, 1, 1)
DEFAULT_DATE = datetime(*DATE_VAL)
TASK_LOGGER = "airflow.task"
LOGHANDLER_NAME = "nomad_log_handler"

EXECUTOR = "airflow.providers.nomad.executors.nomad_executor.NomadExecutor"
DAG_ID = "dag_test_log_handler"
TASK_ID = "task_test_log_handler"
RUN_ID = "test"
BUNDLE_NAME = "test"


def task_python_callable(ti):
    ti.task.log.info("This is a Python test job")


def submit_python_task(
    task_callable: Callable = task_python_callable, retry: int = 3
) -> TaskInstance:
    with DAG(DAG_ID, schedule=None, start_date=DEFAULT_DATE) as dag:
        task = PythonOperator(task_id=TASK_ID, python_callable=task_callable)

    dagrun_kwargs: dict = {
        "logical_date": DEFAULT_DATE,
        "run_after": DEFAULT_DATE,
        "triggered_by": DagRunTriggeredByType.TEST,
    }
    dag.sync_to_db()

    SerializedDagModel.write_dag(dag, bundle_name=BUNDLE_NAME)
    dagrun = dag.create_dagrun(
        run_id=RUN_ID,
        run_type=DagRunType.MANUAL,
        state=DagRunState.RUNNING,
        data_interval=dag.timetable.infer_manual_data_interval(
            run_after=pendulum.datetime(*DATE_VAL, tz="UTC")
        ),
        **dagrun_kwargs,
    )
    ti = TaskInstance(task=task, run_id=dagrun.run_id, dag_version_id=dagrun.created_dag_version_id)
    ti.try_number = retry
    ti.executor = EXECUTOR

    assert ti, "Couldn't create taskinstance"
    return ti


def clean_up():
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()


@pytest.fixture(scope="function", autouse=True)
def setup_teardown():
    # Normally this was already loaded from the config
    logging.config.dictConfig(NOMAD_LOG_CONFIG)
    logging.root.disabled = False
    clean_up()
    # We use file task handler by default.
    yield

    clean_up()


@pytest.mark.usefixtures("clean_executor_loader")
def test_get_task_log_task_running(create_task_instance, mocker):
    mock_nomad_get_task_log = mocker.patch(
        "airflow.providers.nomad.executors.nomad_executor.NomadExecutor.get_task_log"
    )

    mock_nomad_get_task_log.return_value = ([], [])
    ti = create_task_instance(
        dag_id="dag_for_testing_nomad_executor_log_read",
        task_id="task_for_testing_nomad_executor_log_read",
        run_type=DagRunType.SCHEDULED,
        logical_date=DEFAULT_DATE,
    )
    ti.state = TaskInstanceState.RUNNING
    ti.triggerer_job = None
    ti.executor = EXECUTOR
    with conf_vars({("core", "executor"): EXECUTOR}):
        reload(executor_loader)
        fth = ExecutorLogLinesHandler("")
        fth._read(ti=ti, try_number=2)
        mock_nomad_get_task_log.assert_called_once_with(ti, 2)


@pytest.mark.xfail(reason="Local caching not yet implemented")
@pytest.mark.usefixtures("clean_executor_loader")
def test_get_task_log_task_finished(create_task_instance, mocker):
    mock_nomad_get_task_log = mocker.patch(
        "airflow.providers.nomad.executors.nomad_executor.NomadExecutor.get_task_log"
    )

    mock_nomad_get_task_log.return_value = ([], [])
    executor_name = "airflow.providers.nomad.executors.nomad_executor.NomadExecutor"
    ti = create_task_instance(
        dag_id=DAG_ID,
        task_id="task_test+log_handler",
        run_type=DagRunType.SCHEDULED,
        logical_date=DEFAULT_DATE,
    )
    ti.state = TaskInstanceState.SUCCESS
    ti.triggerer_job = None
    ti.executor = executor_name
    with conf_vars({("core", "executor"): executor_name}):
        reload(executor_loader)
        fth = ExecutorLogLinesHandler("")
        fth._read(ti=ti, try_number=2)
        mock_nomad_get_task_log.assert_not_called()


@conf_vars({("core", "executor"): EXECUTOR})
def test_read_nomad_log_handler_ok(mocker, unittest_root):
    reload(executor_loader)
    attempts = 2

    fake_logfile = open(unittest_root / "data/logs.txt", "r").read()

    # Getting hold of the (already automatically mocked) Nomad client
    mock_client = mocker.patch(
        "airflow.providers.nomad.executors.nomad_executor.nomad.Nomad"
    ).return_value
    # We'll verify that the log request was targeting this task
    mock_allocations_request = mock_client.job.get_allocations
    # (Note: This value isn't used, but the mock above requires to have it defined)
    mock_allocations_request.return_value = [{"ID": "fake_UUID"}]
    # We fake the logfile output
    mock_client.client.cat.read_file.return_value = fake_logfile

    ti = submit_python_task()

    # "Hacking" the task into the logger's space
    assert ti.task, f"Taskinstance {ti} has no task"
    logger = ti.log
    ti.task.log.disabled = False

    file_handler: NomadLogHandler = next(
        (h for h in logger.handlers if h.name == LOGHANDLER_NAME), None
    )  # type: ignore

    ti.run(ignore_ti_state=True)
    ti.state = TaskInstanceState.RUNNING

    # clear executor_instances cache
    file_handler.executor_instances = {}

    logs = file_handler.read(ti, attempts)

    loglist = list(logs[0])

    # Checking if request to Nomad was for the right job
    jobid = NomadExecutor.job_id_from_taskinstance_key(ti.key)
    mock_allocations_request.assert_called_once_with(jobid)

    # Checking if logs have the expected content
    assert (
        "Attempting to fetch logs from task "
        f"TaskInstanceKey(dag_id='{DAG_ID}', task_id='{TASK_ID}', run_id='{RUN_ID}', try_number=0, map_index=-1)"
        f" through Nomad API (attempts: {attempts})"
    ) == loglist[0].model_extra["sources"][0]  # type: ignore

    assert ("Found logs for running job via Nomad API") == loglist[0].model_extra["sources"][1]  # type: ignore
    assert [line.event for line in loglist][2:] == fake_logfile.splitlines()


@conf_vars({("core", "executor"): EXECUTOR})
def test_read_nomad_log_handler_no_alloc(mocker):
    """The difference between this test and test_read_nomad_log_handler_ok
    is that here no allocation is returned (so we get an errror)
    """
    reload(executor_loader)
    attempts = 2

    # Getting hold of the (already automatically mocked) Nomad client
    mock_client = mocker.patch(
        "airflow.providers.nomad.executors.nomad_executor.nomad.Nomad"
    ).return_value
    # We'll verify that the log request was targeting this task
    mock_allocations_request = mock_client.job.get_allocations
    # (Note: This value isn't used, but the mock above requires to have it defined)
    mock_allocations_request.return_value = None

    ti = submit_python_task()

    # "Hacking" the task into the logger's space
    assert ti.task, f"Taskinstance {ti} has no task"
    logger = ti.log
    ti.task.log.disabled = False

    file_handler: NomadLogHandler = next(
        (h for h in logger.handlers if h.name == LOGHANDLER_NAME), None
    )  # type: ignore

    ti.run(ignore_ti_state=True)
    ti.state = TaskInstanceState.RUNNING

    # clear executor_instances cache
    file_handler.executor_instances = {}

    logs = file_handler.read(ti, attempts)

    loglist = list(logs[0])

    # Checking if request to Nomad was for the right job
    jobid = NomadExecutor.job_id_from_taskinstance_key(ti.key)
    mock_allocations_request.assert_called_once_with(jobid)

    # Checking if logs have the expected content
    assert (
        "Attempting to fetch logs from task "
        f"TaskInstanceKey(dag_id='{DAG_ID}', task_id='{TASK_ID}', run_id='{RUN_ID}', try_number=0, map_index=-1)"
        f" through Nomad API (attempts: {attempts})"
    ) == loglist[0].model_extra["sources"][0]  # type: ignore

    assert ("Unexpected result from Nomad API allocations query") == loglist[0].model_extra[  # type: ignore
        "sources"
    ][1]
    assert len(loglist) == 2


@conf_vars({("core", "executor"): EXECUTOR})
def test_read_nomad_log_handler_multi_alloc(mocker):
    """The difference between this test and test_read_nomad_log_handler_ok
    is that here multiple allocation are returned (so we get an errror)
    """
    reload(executor_loader)
    attempts = 2
    multi_alloc = [{"ID": "<UUID1>"}, {"ID": "<UUID2>"}]

    # Getting hold of the (already automatically mocked) Nomad client
    mock_client = mocker.patch(
        "airflow.providers.nomad.executors.nomad_executor.nomad.Nomad"
    ).return_value
    # We'll verify that the log request was targeting this task
    mock_allocations_request = mock_client.job.get_allocations
    # (Note: This value isn't used, but the mock above requires to have it defined)
    mock_allocations_request.return_value = multi_alloc

    ti = submit_python_task()

    # "Hacking" the task into the logger's space
    assert ti.task, f"Taskinstance {ti} has no task"
    logger = ti.log
    ti.task.log.disabled = False

    file_handler: NomadLogHandler = next(
        (h for h in logger.handlers if h.name == LOGHANDLER_NAME), None
    )  # type: ignore

    ti.run(ignore_ti_state=True)
    ti.state = TaskInstanceState.RUNNING

    # clear executor_instances cache
    file_handler.executor_instances = {}

    logs = file_handler.read(ti, attempts)

    loglist = list(logs[0])

    # Checking if request to Nomad was for the right job
    jobid = NomadExecutor.job_id_from_taskinstance_key(ti.key)
    mock_allocations_request.assert_called_once_with(jobid)

    # Checking if logs have the expected content
    assert (
        "Attempting to fetch logs from task "
        f"TaskInstanceKey(dag_id='{DAG_ID}', task_id='{TASK_ID}', run_id='{RUN_ID}', try_number=0, map_index=-1)"
        f" through Nomad API (attempts: {attempts})"
    ) == loglist[0].model_extra["sources"][0]  # type: ignore

    assert (
        f"Multiple allocations found found for dag_test_log_handler-task_test_log_handler-test-0--1/dag_test_log_handler-task_test_log_handler: {multi_alloc}"
    ) == loglist[0].model_extra[  # type: ignore
        "sources"
    ][1]
    assert len(loglist) == 2


@conf_vars({("core", "executor"): EXECUTOR})
def test_read_nomad_log_handler_no_alloc_id(mocker):
    """The difference between this test and test_read_nomad_log_handler_ok
    is that here the log retrieval raises an exception
    """
    reload(executor_loader)
    attempts = 2

    # Getting hold of the (already automatically mocked) Nomad client
    mock_client = mocker.patch(
        "airflow.providers.nomad.executors.nomad_executor.nomad.Nomad"
    ).return_value
    # We'll verify that the log request was targeting this task
    mock_allocations_request = mock_client.job.get_allocations
    # (Note: This value isn't used, but the mock above requires to have it defined)
    mock_allocations_request.side_effect = [{"wrong": "data"}]

    ti = submit_python_task()

    # "Hacking" the task into the logger's space
    assert ti.task, f"Taskinstance {ti} has no task"
    logger = ti.log
    ti.task.log.disabled = False

    file_handler: NomadLogHandler = next(
        (h for h in logger.handlers if h.name == LOGHANDLER_NAME), None
    )  # type: ignore

    ti.run(ignore_ti_state=True)
    ti.state = TaskInstanceState.RUNNING

    # clear executor_instances cache
    file_handler.executor_instances = {}

    logs = file_handler.read(ti, attempts)

    loglist = list(logs[0])

    # Checking if request to Nomad was for the right job
    jobid = NomadExecutor.job_id_from_taskinstance_key(ti.key)
    mock_allocations_request.assert_called_once_with(jobid)

    # Checking if logs have the expected content
    assert (
        "Attempting to fetch logs from task "
        f"TaskInstanceKey(dag_id='{DAG_ID}', task_id='{TASK_ID}', run_id='{RUN_ID}', try_number=0, map_index=-1)"
        f" through Nomad API (attempts: {attempts})"
    ) == loglist[0].model_extra["sources"][0]  # type: ignore

    assert ("Unexpected result from Nomad API allocations query") == loglist[0].model_extra[  # type: ignore
        "sources"
    ][1]
    assert len(loglist) == 2


@conf_vars({("core", "executor"): EXECUTOR})
def test_read_nomad_log_handler_log_retrieval_failse(mocker):
    """The difference between this test and test_read_nomad_log_handler_ok
    is that here the log retrieval raises an exception
    """
    reload(executor_loader)
    attempts = 2
    message = "Something bad happened"

    # Getting hold of the (already automatically mocked) Nomad client
    mock_client = mocker.patch(
        "airflow.providers.nomad.executors.nomad_executor.nomad.Nomad"
    ).return_value
    # We'll verify that the log request was targeting this task
    mock_allocations_request = mock_client.job.get_allocations
    # (Note: This value isn't used, but the mock above requires to have it defined)
    mock_allocations_request.side_effect = Exception(message)

    ti = submit_python_task()

    # "Hacking" the task into the logger's space
    assert ti.task, f"Taskinstance {ti} has no task"
    logger = ti.log
    ti.task.log.disabled = False

    file_handler: NomadLogHandler = next(
        (h for h in logger.handlers if h.name == LOGHANDLER_NAME), None
    )  # type: ignore

    ti.run(ignore_ti_state=True)
    ti.state = TaskInstanceState.RUNNING

    # clear executor_instances cache
    file_handler.executor_instances = {}

    logs = file_handler.read(ti, attempts)

    loglist = list(logs[0])

    # Checking if request to Nomad was for the right job
    jobid = NomadExecutor.job_id_from_taskinstance_key(ti.key)
    mock_allocations_request.assert_called_once_with(jobid)

    # Checking if logs have the expected content
    assert (
        "Attempting to fetch logs from task "
        f"TaskInstanceKey(dag_id='{DAG_ID}', task_id='{TASK_ID}', run_id='{RUN_ID}', try_number=0, map_index=-1)"
        f" through Nomad API (attempts: {attempts})"
    ) == loglist[0].model_extra["sources"][0]  # type: ignore

    assert (f"Reading logs failed: {message}") == loglist[0].model_extra[  # type: ignore
        "sources"
    ][1]
    assert len(loglist) == 2
