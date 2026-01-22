import pytest
import logging

from airflow.providers.nomad.decorators.task import _NomadTaskDecoratedOperator, nomad_task


def test_nomad_task_decorator_wrong_body():
    def my_task():
        return 3

    decorated = _NomadTaskDecoratedOperator(task_id="bla", python_callable=my_task)

    with pytest.raises(TypeError) as err:
        decorated.execute({})

    assert "The returned value from the TaskFlow callable must be a list of string(s)." in str(
        err.value
    )


def test_nomad_task_decorator_args_warning(caplog):
    def my_task():
        return 3

    logging.captureWarnings(True)

    with caplog.at_level(logging.WARNING):
        _NomadTaskDecoratedOperator(task_id="bla", multiple_outputs=True, python_callable=my_task)
    assert (
        "`multiple_outputs=True` is not supported in @task.nomad_task tasks. Ignoring."
        in caplog.text
    )

    with caplog.at_level(logging.WARNING):
        _NomadTaskDecoratedOperator(task_id="bla", args="baaaaad", python_callable=my_task)
    assert (
        "Use 'args' for Nomad Decorator with caution. Note that the output of the decorated function is passed concatenated to 'args'"
        in caplog.text
    )
    logging.captureWarnings(False)


def test_nomad_task_decorator_task_id():
    def my_task():
        return "bla"

    decorated = nomad_task(my_task)

    assert decorated.kwargs["task_id"] == "my_task"  # type: ignore [reportAttributeAccessIssue]
