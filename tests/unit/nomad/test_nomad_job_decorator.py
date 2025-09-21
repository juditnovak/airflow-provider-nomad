import pytest

from airflow.providers.nomad.decorators.job import nomad_job, _NomadJobDecoratedOperator


def test_nomad_job_decorator_wrong_body():
    def my_task():
        return 3

    decorated = _NomadJobDecoratedOperator(task_id="bla", python_callable=my_task)

    with pytest.raises(TypeError) as err:
        decorated.execute({})

    assert "The returned value from the TaskFlow callable must be a string." in str(err.value)


def test_nomad_job_decorator_wrong_body2():
    def my_task():
        return "bla"

    decorated = nomad_job(my_task)

    assert decorated.kwargs["task_id"] == "my_task"  # type: ignore [reportAttributeAccessIssue]
