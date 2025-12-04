import json
import logging.config
from collections.abc import Callable
from datetime import datetime
from importlib import reload

import pendulum
import pytest
from airflow.utils.log.log_reader import TaskLogReader
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.providers.nomad.log import NomadLogHandler
from airflow.executors import executor_loader
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunTriggeredByType, DagRunType
from tests_common.test_utils.compat import PythonOperator
from tests_common.test_utils.config import conf_vars

# Introduced and mandatory since Airflow 3.1
try:
    from tests_common.test_utils.dag import sync_dag_to_db
except ModuleNotFoundError:
    pass

from airflow.providers.nomad.generic_interfaces.executor_log_handlers import ExecutorLogLinesHandler
from airflow.providers.nomad.log import NOMAD_HANDLER_NAME

from airflow.providers.nomad.utils import job_id_from_taskinstance_key

DATE_VAL = (2016, 1, 1)
DEFAULT_DATE = datetime(*DATE_VAL)
TASK_LOGGER = "airflow.task"
NOMAD_LOGHANDLER = "nomad_log_handler"
NOMAD_LOGGING_CONFIG = {
    ("logging", "task_log_reader"): NOMAD_LOGHANDLER,
    (
        "logging",
        "logging_config_class",
    ): "airflow.providers.nomad.log.NOMAD_LOG_CONFIG",
}
AIRFLOW_LOGHANDLER = "task"
AIRFLOW_LOGGING_CONFIG = {
    ("logging", "task_log_reader"): AIRFLOW_LOGHANDLER,
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

    try:
        dag = sync_dag_to_db(dag)
    except NameError:
        dag.sync_to_db()
        from airflow.models.serialized_dag import SerializedDagModel

        SerializedDagModel.write_dag(dag, bundle_name=BUNDLE_NAME)

    # SerializedDagModel.write_dag(dag, bundle_name=BUNDLE_NAME)
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
) -> logging.Handler | None:
    # Adding nomad_log_handler to handlers in case it was missing
    # May need revision in the future
    if NOMAD_HANDLER_NAME not in logging.getLogger("airflow.task").handlers:
        handler = NomadLogHandler()
        logging.getLogger("airflow.task").addHandler(handler)
    if AIRFLOW_LOGHANDLER not in logging.getLogger("airflow.task").handlers:
        handler = FileTaskHandler("/dev/null")
        logging.getLogger("airflow.task").addHandler(handler)

    loghandler: logging.Handler | None = TaskLogReader().log_handler
    if not loghandler:
        logger = ti.log
        loghandler = next((h for h in logger.handlers if h.name == handler_name), None)  # type: ignore[union-attr, reportAttributeAccessIssue]

    loghandler.executor_instances = {}  # type: ignore[reportAttributeAccessIssue, union-attr]
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
    # logging.config.dictConfig(NOMAD_LOG_CONFIG)
    logging.root.disabled = False
    clean_up()
    # We use file task handler by default.
    yield

    clean_up()
    reload(executor_loader)


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
    executor_name = "airflow.providers.nomad.nomad_executor.NomadExecutor"
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
def test_nomad_log_ok(mocker, unittest_root, test_datadir):
    reload(executor_loader)

    fake_logfile = open(unittest_root / "data/task.log", "r").read()
    job_alloc = test_datadir / "nomad_job_allocations.json"

    # Getting hold of the (already automatically mocked) Nomad client
    mock_client = mocker.patch("airflow.providers.nomad.manager.nomad.Nomad").return_value
    # We'll verify that the log request was targeting this task
    mock_allocations_request = mock_client.job.get_allocations
    # (Note: This value isn't used, but the mock above requires to have it defined)
    mock_client.job.get_allocations.return_value = json.loads(open(job_alloc).read())
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
    jobid = job_id_from_taskinstance_key(ti.key)
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
def test_nomad_log_ok_with_stderr(mocker, unittest_root, test_datadir):
    reload(executor_loader)

    job_alloc = test_datadir / "nomad_job_allocations.json"
    fake_logfile = open(unittest_root / "data/oneline_task.log", "r").read()
    fake_stderr = open(unittest_root / "data/err.log", "r").read()

    # Getting hold of the (already automatically mocked) Nomad client
    mock_client = mocker.patch("airflow.providers.nomad.manager.nomad.Nomad").return_value
    # We'll verify that the log request was targeting this task
    mock_allocations_request = mock_client.job.get_allocations
    # (Note: This value isn't used, but the mock above requires to have it defined)
    mock_client.job.get_allocations.return_value = json.loads(open(job_alloc).read())
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
    jobid = job_id_from_taskinstance_key(ti.key)
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
def test_airflow_log_ok(mocker, unittest_root, test_datadir):
    reload(executor_loader)

    job_alloc = test_datadir / "nomad_job_allocations.json"
    fake_logfile = open(unittest_root / "data/task.log", "r").read()

    # Getting hold of the (already automatically mocked) Nomad client
    mock_client = mocker.patch("airflow.providers.nomad.manager.nomad.Nomad").return_value
    # We'll verify that the log request was targeting this task
    mock_allocations_request = mock_client.job.get_allocations
    # (Note: This value isn't used, but the mock above requires to have it defined)
    mock_client.job.get_allocations.return_value = json.loads(open(job_alloc).read())
    # We fake the logfile output
    mock_client.client.cat.read_file.side_effect = [fake_logfile, None]

    ti = submit_python_task()

    # "Hacking" the task into the logger's space
    assert ti.task, f"Taskinstance {ti} has no task"
    ti.task.log.disabled = False

    loghandler = get_and_wipe_loghandler(ti, AIRFLOW_LOGHANDLER)
    logs = loghandler.read(ti, TRY_NUMBER)  # type: ignore[reportAttributeAccessIssue]
    loglist = list(logs[0])

    # Checking if request to Nomad was for the right job
    jobid = job_id_from_taskinstance_key(ti.key)
    mock_allocations_request.assert_called_with(jobid)

    assert "Found standard logs for running job via Nomad API" in loglist[0].model_extra["sources"]
    assert loglist[1].event == "::endgroup::"
    assert [line.event for line in loglist][2:] == fake_logfile.splitlines()


@conf_vars({("core", "executor"): EXECUTOR, **AIRFLOW_LOGGING_CONFIG})
def test_airflow_log_ok_with_stderr(mocker, unittest_root, test_datadir):
    reload(executor_loader)

    job_alloc = test_datadir / "nomad_job_allocations.json"
    fake_logfile = open(unittest_root / "data/oneline_task.log", "r").read()
    fake_stderr = open(unittest_root / "data/err.log", "r").read()
    # Getting hold of the (already automatically mocked) Nomad client
    mock_client = mocker.patch("airflow.providers.nomad.manager.nomad.Nomad").return_value
    # We'll verify that the log request was targeting this task
    mock_allocations_request = mock_client.job.get_allocations
    # (Note: This value isn't used, but the mock above requires to have it defined)
    mock_client.job.get_allocations.return_value = json.loads(open(job_alloc).read())
    # We fake the logfile output
    mock_client.client.cat.read_file.side_effect = [fake_logfile, fake_stderr]

    ti = submit_python_task()

    # "Hacking" the task into the logger's space
    assert ti.task, f"Taskinstance {ti} has no task"
    ti.task.log.disabled = False

    loghandler = get_and_wipe_loghandler(ti, AIRFLOW_LOGHANDLER)
    logs = loghandler.read(ti, TRY_NUMBER)  # type: ignore[reportAttributeAccessIssue]
    loglist = list(logs[0])

    # Checking if request to Nomad was for the right job
    jobid = job_id_from_taskinstance_key(ti.key)
    mock_allocations_request.assert_called_with(jobid)

    # FileTaskHandler added it's 1s liner group
    # NOTE: The stdout before stderr order is specific to the test
    # Normally airflow.utils.log.FileTaskHandler merges logs by timestamp, resulting in errors
    # to be displayed when they happened within the rest of the output
    assert "Found standard logs for running job via Nomad API" in loglist[0].model_extra["sources"]
    assert loglist[1].event == "::endgroup::"
    assert loglist[2].event == fake_logfile.splitlines()[0]
    assert [line.event for line in loglist][3:] == fake_stderr.splitlines()


@pytest.mark.parametrize(
    "handler, config",
    [(NOMAD_LOGHANDLER, NOMAD_LOGGING_CONFIG), (AIRFLOW_LOGHANDLER, AIRFLOW_LOGGING_CONFIG)],
)
def test_nomad_side_error(handler, config, mocker, test_datadir):
    """The difference between this test and test_nomad_log_ok
    is that here no allocation is returned (so we get an errror)
    """

    with conf_vars({("core", "executor"): EXECUTOR, **config}):
        reload(executor_loader)

        # Getting hold of the (already automatically mocked) Nomad client
        mock_client = mocker.patch("airflow.providers.nomad.manager.nomad.Nomad").return_value
        mock_allocations_request = mock_client.job.get_allocations
        mock_allocations_request.return_value = [{"ID": "fake_UUID"}]
        mock_client.client.cat.read_file.side_effect = ["", ""]

        file_path1 = test_datadir / "nomad_job_evaluation.json"
        file_path2 = test_datadir / "nomad_job_info_pending.json"
        file_path3 = test_datadir / "nomad_job_allocations_pending.json"
        file_path4 = test_datadir / "nomad_job_summary_failed.json"
        with (
            open(file_path1) as file1,
            open(file_path2) as file2,
            open(file_path3) as file3,
            open(file_path4) as file4,
        ):
            eval_data = file1.read()
            alloc_data = file3.read()
            summary_data = file4.read()
            mock_client.job.get_job.return_value = json.loads(file2.read())
            mock_client.job.get_evaluations.return_value = json.loads(eval_data)
            mock_client.job.get_allocations.return_value = json.loads(alloc_data)
            mock_client.job.get_summary.return_value = json.loads(summary_data)

        ti = submit_python_task()

        # "Hacking" the task into the logger's space
        assert ti.task, f"Taskinstance {ti} has no task"
        ti.task.log.disabled = False

        loghandler = get_and_wipe_loghandler(ti, handler)
        logs = loghandler.read(ti, TRY_NUMBER)  # type: ignore[reportAttributeAccessIssue]
        loglist = list(logs[0])

        # Checking if request to Nomad was for the right job
        jobid = job_id_from_taskinstance_key(ti.key)
        mock_allocations_request.assert_called_with(jobid)

        # Checking if logs have the expected content
        assert loglist[2].event == (
            "No task logs found, but the following information was retrieved from Nomad:"
        )

        lastind = 3

        assert loglist[lastind].event == "Job summary:"
        startind = lastind + 1
        summary_lines = summary_data.splitlines()
        lastind = startind + len(summary_lines)
        assert [item.event for item in loglist][startind:lastind] == summary_lines

        assert loglist[lastind].event == "Job allocations info:"
        startind = lastind + 1
        alloc_lines = alloc_data.splitlines()
        lastind = startind + len(alloc_lines)
        assert [item.event for item in loglist][startind:lastind] == alloc_lines

        assert loglist[lastind].event == "Job evaluations:"
        startind = lastind + 1
        eval_lines = eval_data.splitlines()
        lastind = startind + len(eval_lines)
        assert [item.event for item in loglist][startind:lastind] == eval_lines


@pytest.mark.parametrize(
    "handler, config",
    [(NOMAD_LOGHANDLER, NOMAD_LOGGING_CONFIG), (AIRFLOW_LOGHANDLER, AIRFLOW_LOGGING_CONFIG)],
)
def test_nomad_log_multi_alloc(handler, config, mocker, test_datadir):
    """The difference between this test and test_nomad_log_ok
    is that here multiple allocation are returned by Nomad API (so we get an errror)
    """
    with conf_vars({("core", "executor"): EXECUTOR, **config}):
        reload(executor_loader)

        file_path = test_datadir / "nomad_job_multi_allocations.json"
        mock_client = mocker.patch("airflow.providers.nomad.manager.nomad.Nomad").return_value
        mock_allocations_request = mock_client.job.get_allocations
        mock_allocations_request.return_value = json.loads(open(file_path).read())
        mock_client.client.cat.read_file.side_effect = ["loglist1", "loglist2"]

        ti = submit_python_task()

        # "Hacking" the task into the logger's space
        assert ti.task, f"Taskinstance {ti} has no task"
        ti.task.log.disabled = False

        loghandler = get_and_wipe_loghandler(ti, handler)
        logs = loghandler.read(ti, TRY_NUMBER)  # type: ignore[reportAttributeAccessIssue]
        loglist = list(logs[0])

        # Checking if request to Nomad was for the right job
        jobid = job_id_from_taskinstance_key(ti.key)
        mock_allocations_request.assert_called_with(jobid)

        # Checking if logs have the expected content
        assert loglist[0].model_extra["sources"][1] == (  # type: ignore
            "Found standard logs for running job via Nomad API"
        )

        assert loglist[2].event == "Allocation ID 32ffc170-4cfd-2a63-5b8b-52981636a216:"
        assert loglist[3].event == "loglist1"
        assert loglist[4].event == "Allocation ID 32ffc170-4cfd-2a63-5b8b-52981636a226:"
        assert loglist[5].event == "loglist2"


# Failure cases


@pytest.mark.parametrize(
    "handler, config",
    [(NOMAD_LOGHANDLER, NOMAD_LOGGING_CONFIG), (AIRFLOW_LOGHANDLER, AIRFLOW_LOGGING_CONFIG)],
)
def test_nomad_log_no_alloc(handler, config, mocker, test_datadir):
    """The difference between this test and test_nomad_log_ok
    is that here no allocation is returned (so we get an errror)
    """
    with conf_vars({("core", "executor"): EXECUTOR, **config}):
        reload(executor_loader)

        # Getting hold of the (already automatically mocked) Nomad client
        mock_client = mocker.patch("airflow.providers.nomad.manager.nomad.Nomad").return_value
        # We'll verify that the log request was targeting this task
        mock_allocations_request = mock_client.job.get_allocations
        # (Note: This value isn't used, but the mock above requires to have it defined)
        mock_allocations_request.return_value = None

        # Ignore -- Nomad log retrieval mocked to reduce validation errors
        file_path1 = test_datadir / "nomad_job_evaluation.json"
        file_path2 = test_datadir / "nomad_job_info_pending.json"
        file_path3 = test_datadir / "nomad_job_summary_failed.json"
        with open(file_path1) as file1, open(file_path2) as file2, open(file_path3) as file3:
            mock_client.job.get_evaluations.return_value = json.loads(file1.read())
            mock_client.job.get_job.return_value = json.loads(file2.read())
            mock_client.job.get_summary.return_value = json.loads(file3.read())

        ti = submit_python_task()

        # "Hacking" the task into the logger's space
        assert ti.task, f"Taskinstance {ti} has no task"
        ti.task.log.disabled = False

        loghandler = get_and_wipe_loghandler(ti, handler)
        logs = loghandler.read(ti, TRY_NUMBER)  # type: ignore[reportAttributeAccessIssue]
        loglist = list(logs[0])

        # Checking if request to Nomad was for the right job
        jobid = job_id_from_taskinstance_key(ti.key)
        mock_allocations_request.assert_called_with(jobid)

        # Checking if logs have the expected content
        assert loglist[0].model_extra["sources"][1] == (  # type: ignore
            "Unexpected result from Nomad API allocations query"
        )


@pytest.mark.parametrize(
    "handler, config",
    [(NOMAD_LOGHANDLER, NOMAD_LOGGING_CONFIG), (AIRFLOW_LOGHANDLER, AIRFLOW_LOGGING_CONFIG)],
)
def test_nomad_log_no_alloc_id(handler, config, mocker, test_datadir):
    """The difference between this test and test_nomad_log_ok
    is that wrong allocation data is returned from Nomad API (so we get an errror)
    """
    with conf_vars({("core", "executor"): EXECUTOR, **config}):
        reload(executor_loader)

        # Getting hold of the (already automatically mocked) Nomad client
        mock_client = mocker.patch("airflow.providers.nomad.manager.nomad.Nomad").return_value
        # We'll verify that the log request was targeting this task
        mock_allocations_request = mock_client.job.get_allocations
        # (Note: This value isn't used, but the mock above requires to have it defined)
        mock_allocations_request.side_effect = [{"wrong": "data"}, {"wrong", "data"}]

        # Ignore -- Nomad log retrieval mocked to reduce validation errors
        file_path1 = test_datadir / "nomad_job_evaluation.json"
        file_path2 = test_datadir / "nomad_job_info_pending.json"
        file_path3 = test_datadir / "nomad_job_summary_failed.json"
        with open(file_path1) as file1, open(file_path2) as file2, open(file_path3) as file3:
            mock_client.job.get_evaluations.return_value = json.loads(file1.read())
            mock_client.job.get_job.return_value = json.loads(file2.read())
            mock_client.job.get_summary.return_value = json.loads(file3.read())

        ti = submit_python_task()

        # "Hacking" the task into the logger's space
        assert ti.task, f"Taskinstance {ti} has no task"
        ti.task.log.disabled = False

        loghandler = get_and_wipe_loghandler(ti, handler)
        logs = loghandler.read(ti, TRY_NUMBER)  # type: ignore[reportAttributeAccessIssue]
        loglist = list(logs[0])

        # Checking if request to Nomad was for the right job
        jobid = job_id_from_taskinstance_key(ti.key)
        mock_allocations_request.assert_called_with(jobid)

        # Checking if logs have the expected content
        assert loglist[0].model_extra["sources"][1] == (  # type: ignore
            "Unexpected result from Nomad API allocations query"
        )


@pytest.mark.parametrize(
    "handler, config",
    [(NOMAD_LOGHANDLER, NOMAD_LOGGING_CONFIG), (AIRFLOW_LOGHANDLER, AIRFLOW_LOGGING_CONFIG)],
)
def test_nomad_log_retrieval_false(handler, config, mocker, test_datadir):
    """The difference between this test and test_nomad_log_ok
    is that here the log retrieval raises an exception
    """
    with conf_vars({("core", "executor"): EXECUTOR, **config}):
        reload(executor_loader)

        message = "Something bad happened"

        # Getting hold of the (already automatically mocked) Nomad client
        mock_client = mocker.patch("airflow.providers.nomad.manager.nomad.Nomad").return_value
        # We'll verify that the log request was targeting this task
        mock_allocations_request = mock_client.job.get_allocations
        # (Note: This value isn't used, but the mock above requires to have it defined)
        mock_allocations_request.side_effect = [Exception(message), {"wrong", "data"}]

        # Ignore -- Nomad log retrieval mocked to reduce validation errors
        file_path1 = test_datadir / "nomad_job_evaluation.json"
        file_path2 = test_datadir / "nomad_job_info_pending.json"
        file_path3 = test_datadir / "nomad_job_summary_failed.json"
        with open(file_path1) as file1, open(file_path2) as file2, open(file_path3) as file3:
            mock_client.job.get_evaluations.return_value = json.loads(file1.read())
            mock_client.job.get_job.return_value = json.loads(file2.read())
            mock_client.job.get_summary.return_value = json.loads(file3.read())

        ti = submit_python_task()

        # "Hacking" the task into the logger's space
        assert ti.task, f"Taskinstance {ti} has no task"
        ti.task.log.disabled = False

        loghandler = get_and_wipe_loghandler(ti, handler)
        logs = loghandler.read(ti, TRY_NUMBER)  # type: ignore[reportAttributeAccessIssue]
        loglist = list(logs[0])

        # Checking if request to Nomad was for the right job
        jobid = job_id_from_taskinstance_key(ti.key)
        mock_allocations_request.assert_called_with(jobid)

        # Checking if logs have the expected content
        assert loglist[0].model_extra["sources"][1] == (  # type: ignore
            f"Reading standard logs failed: {message}"
        )
