import logging
from unittest.mock import MagicMock

from airflow.sdk import Context
from airflow.providers.nomad.decorators.python import (
    _NomadPythonTaskDecoratedOperator,
    nomad_python_task,
)


def test_nomad_python_task_decorator_args_warning(caplog):
    def my_task():
        return 3

    logging.captureWarnings(True)

    with caplog.at_level(logging.WARNING):
        _NomadPythonTaskDecoratedOperator(
            task_id="bla", multiple_outputs=True, python_callable=my_task
        )
    assert "`multiple_outputs=True` is not supported in @task.nomad tasks. Ignoring." in caplog.text

    with caplog.at_level(logging.WARNING):
        _NomadPythonTaskDecoratedOperator(task_id="bla", args="baaaaad", python_callable=my_task)
    assert (
        "Use 'args' for Nomad Decorator with caution. Note that the output of the decorated function is passed concatenated to 'args'"
        in caplog.text
    )
    logging.captureWarnings(False)


def test_nomad_python_task_decorator_task_id():
    def my_task():
        return "bla"

    decorated = nomad_python_task(my_task)

    assert decorated.kwargs["task_id"] == "my_task"  # type: ignore [reportAttributeAccessIssue]


def test_nomad_python_task_decorator_args(mocker):
    def my_task():
        return "bla"

    mocker.patch("airflow.providers.nomad.decorators.python.DecoratedOperator.execute")
    dec = _NomadPythonTaskDecoratedOperator(task_id="bla", args="baaaaad", python_callable=my_task)
    runtime_ti = MagicMock(
        task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
    )
    dec.execute(Context({"ti": runtime_ti}))
    assert dec.python_command == 'return "bla"\n'


def test_nomad_python_task_decorator_docstring1(mocker):
    def my_task():
        "with docstring"
        return "bla"

    mocker.patch("airflow.providers.nomad.decorators.python.DecoratedOperator.execute")
    dec = _NomadPythonTaskDecoratedOperator(task_id="bla", args="baaaaad", python_callable=my_task)
    runtime_ti = MagicMock(
        task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
    )
    dec.execute(Context({"ti": runtime_ti}))
    assert dec.python_command == 'return "bla"\n'


def test_nomad_python_task_decorator_docstring2(mocker):
    def my_task():
        "with docstring"
        return "bla"

    mocker.patch("airflow.providers.nomad.decorators.python.DecoratedOperator.execute")
    dec = _NomadPythonTaskDecoratedOperator(task_id="bla", args="baaaaad", python_callable=my_task)
    runtime_ti = MagicMock(
        task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
    )
    dec.execute(Context({"ti": runtime_ti}))
    assert dec.python_command == 'return "bla"\n'


def test_nomad_python_task_decorator_docstring3(mocker):
    def my_task():
        """with docstring'
        of multiple lines
        """
        return "bla"

    def my_task2():
        """with docstring'
        of multiple lines"""
        return "bla"

    mocker.patch("airflow.providers.nomad.decorators.python.DecoratedOperator.execute")
    dec = _NomadPythonTaskDecoratedOperator(task_id="bla", args="baaaaad", python_callable=my_task)
    runtime_ti = MagicMock(
        task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
    )
    dec.execute(Context({"ti": runtime_ti}))
    assert dec.python_command == 'return "bla"\n'

    dec = _NomadPythonTaskDecoratedOperator(task_id="bla", args="baaaaad", python_callable=my_task2)
    runtime_ti = MagicMock(
        task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
    )
    dec.execute(Context({"ti": runtime_ti}))
    assert dec.python_command == 'return "bla"\n'


def test_nomad_python_task_decorator_header_remove(mocker):
    @nomad_python_task()
    def my_task():
        return "bla"

    mocker.patch("airflow.providers.nomad.decorators.python.DecoratedOperator.execute")
    dec = _NomadPythonTaskDecoratedOperator(task_id="bla", args="baaaaad", python_callable=my_task)
    runtime_ti = MagicMock(
        task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
    )
    dec.execute(Context({"ti": runtime_ti}))
    assert dec.python_command == 'return "bla"\n'


def test_nomad_python_task_decorator_header_remove2(mocker):
    @nomad_python_task(param1="string", param2={"multi": "line", "python": "dict"})
    def my_task():
        return "bla"

    mocker.patch("airflow.providers.nomad.decorators.python.DecoratedOperator.execute")
    dec = _NomadPythonTaskDecoratedOperator(task_id="bla", args="baaaaad", python_callable=my_task)
    runtime_ti = MagicMock(
        task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
    )
    dec.execute(Context({"ti": runtime_ti}))
    assert dec.python_command == 'return "bla"\n'
