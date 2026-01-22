import pytest
import logging

from airflow.providers.nomad.decorators.job import _NomadJobDecoratedOperator, nomad_job


def test_nomad_job_decorator_wrong_body():
    def my_task():
        return 3

    decorated = _NomadJobDecoratedOperator(task_id="bla", python_callable=my_task)

    with pytest.raises(TypeError) as err:
        decorated.execute({})

    assert "The returned value from the TaskFlow callable must be a string." in str(err.value)


def test_nomad_job_decorator_args_warning(caplog):
    def my_task():
        return 3

    logging.captureWarnings(True)

    with caplog.at_level(logging.WARNING):
        _NomadJobDecoratedOperator(task_id="bla", multiple_outputs=True, python_callable=my_task)
    assert (
        "`multiple_outputs=True` is not supported in @task.nomad_job tasks. Ignoring."
        in caplog.text
    )
    logging.captureWarnings(False)


def test_nomad_job_decorator_task_id():
    def my_task():
        return "bla"

    decorated = nomad_job(my_task)

    assert decorated.kwargs["task_id"] == "my_task"  # type: ignore [reportAttributeAccessIssue]
