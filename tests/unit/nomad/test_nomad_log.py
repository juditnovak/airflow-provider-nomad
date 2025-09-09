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
from airflow.providers.nomad.generic_interfaces.executor_log_handlers import ExecutorLogLinesHandler
from airflow.providers.nomad.nomad_log import NOMAD_LOG_CONFIG

DATE_VAL = (2016, 1, 1)
DEFAULT_DATE = datetime(*DATE_VAL)
TASK_LOGGER = "airflow.task"
NOMAD_LOGHANDLER = "nomad_log_handler"
NOMAD_LOGGING_CONFIG = {
    ("logging", "task_log_reader"): NOMAD_LOGHANDLER,
    (
        "logging",
        "logging_config_class",
    ): "airflow.providers.nomad.executors.nomad_log.NOMAD_LOG_CONFIG",
}
AiRFLOW_LOGHANDLER = "task"
AIRFLOW_LOGGING_CONFIG = {
    ("logging", "task_log_reader"): AiRFLOW_LOGHANDLER,
    (
        "logging",
        "logging_config_class",
    ): "airflow.config_templates/airflow_local_settings.DEFAULT_LOGGING_CONFIG",
}

EXECUTOR = "airflow.providers.nomad.executors.nomad_executor.NomadExecutor"
DAG_ID = "dag_test_log_handler"
TASK_ID = "task_test_log_handler"
RUN_ID = "test"
BUNDLE_NAME = "test"
TRY_NUMBER = 3

##############################################################################
# Helper functions
##############################################################################


def ti_key_str(dag_id=DAG_ID, task_id=TASK_ID, run_id=RUN_ID, try_number=TRY_NUMBER, map_index=-1):
    # NOTE: The try number is always 0,
    return f"TaskInstanceKey(dag_id='{dag_id}', task_id='{task_id}', run_id='{run_id}', try_number={try_number}, map_index={map_index})"


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
    ti.executor = EXECUTOR

    ti.run(ignore_ti_state=True)
    ti.state = TaskInstanceState.RUNNING
    ti.try_number = retry

    assert ti, "Couldn't create taskinstance"
    return ti


def get_and_wipe_loghandler(
    ti: TaskInstance, handler_name: str = NOMAD_LOGHANDLER
) -> logging.Handler:
    logger = ti.log
    loghandler: logging.Handler = next((h for h in logger.handlers if h.name == handler_name), None)  # type: ignore
    # clear executor_instances cache
    loghandler.executor_instances = {}  # type: ignore[attr-defined]
    return loghandler


##############################################################################
# Setup/teardown
##############################################################################


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


##############################################################################
# Tests
##############################################################################


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
    with conf_vars({("core", "executor"): EXECUTOR, **NOMAD_LOGGING_CONFIG}):
        reload(executor_loader)
        fth = ExecutorLogLinesHandler()
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
    with conf_vars({("core", "executor"): EXECUTOR, **NOMAD_LOGGING_CONFIG}):
        reload(executor_loader)
        fth = ExecutorLogLinesHandler()
        fth._read(ti=ti, try_number=2)
        mock_nomad_get_task_log.assert_not_called()


# Nomad logging (OK)


@conf_vars({("core", "executor"): EXECUTOR, **NOMAD_LOGGING_CONFIG})
def test_nomad_log_ok(mocker, unittest_root):
    reload(executor_loader)
    fake_logfile = open(unittest_root / "data/task.log", "r").read()

    # Getting hold of the (already automatically mocked) Nomad client
    mock_client = mocker.patch(
        "airflow.providers.nomad.executors.nomad_executor.nomad.Nomad"
    ).return_value
    # We'll verify that the log request was targeting this task
    mock_allocations_request = mock_client.job.get_allocations
    # (Note: This value isn't used, but the mock above requires to have it defined)
    mock_allocations_request.return_value = [{"ID": "fake_UUID"}]
    # We fake the logfile output
    mock_client.client.cat.read_file.side_effect = [fake_logfile, None]

    ti = submit_python_task()

    # "Hacking" the task into the logger's space
    assert ti.task, f"Taskinstance {ti} has no task"
    ti.task.log.disabled = False

    loghandler = get_and_wipe_loghandler(ti)
    logs = loghandler.read(ti, TRY_NUMBER)  # type: ignore[reportAttributeAccessIssue]
    loglist = list(logs[0])

    # Checking if request to Nomad was for the right job
    jobid = NomadExecutor.job_id_from_taskinstance_key(ti.key)
    mock_allocations_request.assert_called_with(jobid)

    assert loglist[0].model_extra["sources"][0] == (  # type: ignore
        f"Attempting to fetch standard logs for task {ti_key_str()}"
        f" through Nomad API (attempts: {TRY_NUMBER})"
    )

    assert loglist[0].model_extra["sources"][1] == (  # type: ignore
        "Found standard logs for running job via Nomad API"
    )
    assert [line.event for line in loglist][2:] == fake_logfile.splitlines()


@conf_vars({("core", "executor"): EXECUTOR, **NOMAD_LOGGING_CONFIG})
def test_nomad_log_ok_with_stderr(mocker, unittest_root):
    reload(executor_loader)

    fake_logfile = open(unittest_root / "data/oneline_task.log", "r").read()
    fake_stderr = open(unittest_root / "data/err.log", "r").read()

    # Getting hold of the (already automatically mocked) Nomad client
    mock_client = mocker.patch(
        "airflow.providers.nomad.executors.nomad_executor.nomad.Nomad"
    ).return_value
    # We'll verify that the log request was targeting this task
    mock_allocations_request = mock_client.job.get_allocations
    # (Note: This value isn't used, but the mock above requires to have it defined)
    mock_allocations_request.return_value = [{"ID": "fake_UUID"}]
    # We fake the logfile output
    mock_client.client.cat.read_file.side_effect = [fake_logfile, fake_stderr]

    ti = submit_python_task()

    # "Hacking" the task into the logger's space
    assert ti.task, f"Taskinstance {ti} has no task"
    ti.task.log.disabled = False

    loghandler = get_and_wipe_loghandler(ti)
    logs = loghandler.read(ti, TRY_NUMBER)  # type: ignore[reportAttributeAccessIssue]
    loglist = list(logs[0])

    # Checking if request to Nomad was for the right job
    jobid = NomadExecutor.job_id_from_taskinstance_key(ti.key)
    mock_allocations_request.assert_called_with(jobid)

    # Checking if logs have the expected headers and content
    assert loglist[0].model_extra["sources"][0] == (  # type: ignore
        f"Attempting to fetch standard logs for task {ti_key_str()}"
        f" through Nomad API (attempts: {TRY_NUMBER})"
    )
    assert loglist[0].model_extra["sources"][1] == (  # type: ignore
        "Found standard logs for running job via Nomad API"
    )
    assert loglist[0].model_extra["sources"][2] == (  # type: ignore
        f"Attempting to fetch error logs for task {ti_key_str()}"
        f" through Nomad API (attempts: {TRY_NUMBER})"
    )
    assert loglist[0].model_extra["sources"][3] == (  # type: ignore
        "Found error logs for running job via Nomad API"
    )
    assert loglist[1].event == "::endgroup::"
    assert loglist[2].event == "::group::Task logs"
    assert loglist[3].event == fake_logfile.splitlines()[0]
    assert loglist[4].event == "::endgroup::"
    assert loglist[5].event == "::group::Errors outside of task execution"
    assert [line.event for line in loglist][6:-1] == fake_stderr.splitlines()
    assert loglist[-1].event == "::endgroup::"


# Airflow default logging (OK)


@conf_vars({("core", "executor"): EXECUTOR, **AIRFLOW_LOGGING_CONFIG})
def test_airflow_log_ok(mocker, unittest_root):
    reload(executor_loader)
    fake_logfile = open(unittest_root / "data/task.log", "r").read()

    # Getting hold of the (already automatically mocked) Nomad client
    mock_client = mocker.patch(
        "airflow.providers.nomad.executors.nomad_executor.nomad.Nomad"
    ).return_value
    # We'll verify that the log request was targeting this task
    mock_allocations_request = mock_client.job.get_allocations
    # (Note: This value isn't used, but the mock above requires to have it defined)
    mock_allocations_request.return_value = [{"ID": "fake_UUID"}]
    # We fake the logfile output
    mock_client.client.cat.read_file.side_effect = [fake_logfile, None]

    ti = submit_python_task()

    # "Hacking" the task into the logger's space
    assert ti.task, f"Taskinstance {ti} has no task"
    ti.task.log.disabled = False

    loghandler = get_and_wipe_loghandler(ti, AiRFLOW_LOGHANDLER)
    logs = loghandler.read(ti, TRY_NUMBER)  # type: ignore[reportAttributeAccessIssue]
    loglist = list(logs[0])

    # Checking if request to Nomad was for the right job
    jobid = NomadExecutor.job_id_from_taskinstance_key(ti.key)
    mock_allocations_request.assert_called_with(jobid)

    assert "Found standard logs for running job via Nomad API" in loglist[0].model_extra["sources"]
    assert loglist[1].event == "::endgroup::"
    assert [line.event for line in loglist][2:] == fake_logfile.splitlines()


@conf_vars({("core", "executor"): EXECUTOR, **AIRFLOW_LOGGING_CONFIG})
def test_airflow_log_ok_with_stderr(mocker, unittest_root):
    reload(executor_loader)
    fake_logfile = open(unittest_root / "data/oneline_task.log", "r").read()
    fake_stderr = open(unittest_root / "data/err.log", "r").read()
    # Getting hold of the (already automatically mocked) Nomad client
    mock_client = mocker.patch(
        "airflow.providers.nomad.executors.nomad_executor.nomad.Nomad"
    ).return_value
    # We'll verify that the log request was targeting this task
    mock_allocations_request = mock_client.job.get_allocations
    # (Note: This value isn't used, but the mock above requires to have it defined)
    mock_allocations_request.return_value = [{"ID": "fake_UUID"}]
    # We fake the logfile output
    mock_client.client.cat.read_file.side_effect = [fake_logfile, fake_stderr]

    ti = submit_python_task()

    # "Hacking" the task into the logger's space
    assert ti.task, f"Taskinstance {ti} has no task"
    ti.task.log.disabled = False

    loghandler = get_and_wipe_loghandler(ti, AiRFLOW_LOGHANDLER)
    logs = loghandler.read(ti, TRY_NUMBER)  # type: ignore[reportAttributeAccessIssue]
    loglist = list(logs[0])

    # Checking if request to Nomad was for the right job
    jobid = NomadExecutor.job_id_from_taskinstance_key(ti.key)
    mock_allocations_request.assert_called_with(jobid)

    # FileTaskHandler added it's 1s liner group
    # NOTE: The stdout before stderr order is specific to the test
    # Normally airflow.utils.log.FileTaskHandler merges logs by timestamp, resulting in errors
    # to be displayed when they happened within the rest of the output
    assert "Found standard logs for running job via Nomad API" in loglist[0].model_extra["sources"]
    assert loglist[1].event == "::endgroup::"
    assert loglist[2].event == fake_logfile.splitlines()[0]
    assert [line.event for line in loglist][3:] == fake_stderr.splitlines()


# Failure cases


@pytest.mark.parametrize(
    "handler, config",
    [(NOMAD_LOGHANDLER, NOMAD_LOGGING_CONFIG), (AiRFLOW_LOGHANDLER, AIRFLOW_LOGGING_CONFIG)],
)
def test_nomad_log_no_alloc(handler, config, mocker):
    """The difference between this test and test_nomad_log_ok
    is that here no allocation is returned (so we get an errror)
    """
    with conf_vars({("core", "executor"): EXECUTOR, **config}):
        reload(executor_loader)

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
        ti.task.log.disabled = False

        loghandler = get_and_wipe_loghandler(ti, handler)
        logs = loghandler.read(ti, TRY_NUMBER)  # type: ignore[reportAttributeAccessIssue]
        loglist = list(logs[0])

        # Checking if request to Nomad was for the right job
        jobid = NomadExecutor.job_id_from_taskinstance_key(ti.key)
        mock_allocations_request.assert_called_with(jobid)

        # Checking if logs have the expected content
        assert loglist[0].model_extra["sources"][1] == (  # type: ignore
            "Unexpected result from Nomad API allocations query"
        )
        assert len(loglist) == 2


@pytest.mark.parametrize(
    "handler, config",
    [(NOMAD_LOGHANDLER, NOMAD_LOGGING_CONFIG), (AiRFLOW_LOGHANDLER, AIRFLOW_LOGGING_CONFIG)],
)
def test_nomad_log_multi_alloc(handler, config, mocker):
    """The difference between this test and test_nomad_log_ok
    is that here multiple allocation are returned by Nomad API (so we get an errror)
    """
    with conf_vars({("core", "executor"): EXECUTOR, **config}):
        reload(executor_loader)
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
        ti.task.log.disabled = False

        loghandler = get_and_wipe_loghandler(ti, handler)
        logs = loghandler.read(ti, TRY_NUMBER)  # type: ignore[reportAttributeAccessIssue]
        loglist = list(logs[0])

        # Checking if request to Nomad was for the right job
        jobid = NomadExecutor.job_id_from_taskinstance_key(ti.key)
        mock_allocations_request.assert_called_with(jobid)

        # Checking if logs have the expected content
        assert loglist[0].model_extra["sources"][1] == (  # type: ignore
            f"Multiple allocations found found for {jobid}/{DAG_ID}-{TASK_ID}: {multi_alloc}"
        )
        assert len(loglist) == 2


@pytest.mark.parametrize(
    "handler, config",
    [(NOMAD_LOGHANDLER, NOMAD_LOGGING_CONFIG), (AiRFLOW_LOGHANDLER, AIRFLOW_LOGGING_CONFIG)],
)
def test_nomad_log_no_alloc_id(handler, config, mocker):
    """The difference between this test and test_nomad_log_ok
    is that wrong allocation data is returned from Nomad API (so we get an errror)
    """
    with conf_vars({("core", "executor"): EXECUTOR, **config}):
        reload(executor_loader)

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
        ti.task.log.disabled = False

        loghandler = get_and_wipe_loghandler(ti, handler)
        logs = loghandler.read(ti, TRY_NUMBER)  # type: ignore[reportAttributeAccessIssue]
        loglist = list(logs[0])

        # Checking if request to Nomad was for the right job
        jobid = NomadExecutor.job_id_from_taskinstance_key(ti.key)
        mock_allocations_request.assert_called_with(jobid)

        # Checking if logs have the expected content
        assert loglist[0].model_extra["sources"][1] == (  # type: ignore
            "Unexpected result from Nomad API allocations query"
        )
        assert len(loglist) == 2


@pytest.mark.parametrize(
    "handler, config",
    [(NOMAD_LOGHANDLER, NOMAD_LOGGING_CONFIG), (AiRFLOW_LOGHANDLER, AIRFLOW_LOGGING_CONFIG)],
)
def test_nomad_log_retrieval_false(handler, config, mocker):
    """The difference between this test and test_nomad_log_ok
    is that here the log retrieval raises an exception
    """
    with conf_vars({("core", "executor"): EXECUTOR, **config}):
        reload(executor_loader)
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
        ti.task.log.disabled = False

        loghandler = get_and_wipe_loghandler(ti, handler)
        logs = loghandler.read(ti, TRY_NUMBER)  # type: ignore[reportAttributeAccessIssue]
        loglist = list(logs[0])

        # Checking if request to Nomad was for the right job
        jobid = NomadExecutor.job_id_from_taskinstance_key(ti.key)
        mock_allocations_request.assert_called_with(jobid)

        # Checking if logs have the expected content
        assert loglist[0].model_extra["sources"][1] == (  # type: ignore
            f"Reading standard logs failed: {message}"
        )
        assert len(loglist) == 2
