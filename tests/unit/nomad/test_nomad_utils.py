import json
import logging

import pytest

from airflow.providers.nomad.exceptions import NomadValidationError
from airflow.providers.nomad.utils import (
    dict_to_lines,
    parse_json_job_template,
    run_id_short,
    validate_nomad_job,
    validate_nomad_job_json,
    job_id_from_taskinstance_key,
    job_short_id_from_taskinstance_key,
    job_id_from_taskinstance,
    job_short_id_from_taskinstance,
)


@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_validate_job(filename, test_datadir):
    file_path = test_datadir / filename
    with open(file_path) as file:
        assert validate_nomad_job(json.loads(file.read()))


def test_validate_job_fails():
    with pytest.raises(NomadValidationError):
        validate_nomad_job(json.loads(json.dumps({"wrong": "data"})))


@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_validate_job_json(filename, test_datadir):
    file_path = test_datadir / filename
    with open(file_path) as file:
        assert validate_nomad_job_json(file.read())


def test_validate_job_json_fails():
    with pytest.raises(NomadValidationError):
        validate_nomad_job_json(json.dumps({"wrong": "data"}))


@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_parse_json_job_template(filename, test_datadir):
    file_path = test_datadir / filename
    with open(file_path) as file:
        assert json.loads(file.read()) == parse_json_job_template(file_path).model_dump(  # type: ignore[reportOptionalMemberAccess]
            exclude_unset=True
        )


def test_parse_json_job_template_fails(test_datadir, caplog):
    file_path = test_datadir / "bla"
    error = f"Can't open file: {str(file_path)}"
    with caplog.at_level(logging.ERROR):
        assert parse_json_job_template(file_path) is None
        assert any([error in record.message for record in caplog.records])

    file_path = test_datadir / "simple_batch.hcl"
    assert parse_json_job_template(file_path) is None


def test_dict_to_lines():
    d = {"a": "b", "c": "d", "e": None, "f": 3}
    assert dict_to_lines(d) == [
        "{",
        '    "a": "b",',
        '    "c": "d",',
        '    "e": null,',
        '    "f": 3',
        "}",
    ]


def tests_run_id_short():
    assert (
        run_id_short("scheduled__2026-02-04T14:40:56.397651+00:00")
        == "scheduled__2026-02-04T14:40:56"
    )


def tests_job_id_from_taskinstance_key(taskinstance):
    dag_id, task_id, run_id, try_number, map_index = taskinstance.key
    assert (
        job_id_from_taskinstance_key(taskinstance.key)
        == f"{dag_id}-{task_id}-{run_id}-{try_number}-{map_index}"
    )


def tests_job_id_from_taskinstance(taskinstance):
    dag_id, task_id, run_id, try_number, map_index = taskinstance.key
    assert (
        job_id_from_taskinstance(taskinstance)
        == f"{dag_id}-{task_id}-{run_id}-{try_number}-{map_index}"
    )


def tests_job_short_id_from_taskinstance_key(taskinstance):
    dag_id, task_id, run_id, try_number, _ = taskinstance.key
    assert (
        job_short_id_from_taskinstance_key(taskinstance.key)
        == f"{dag_id}-{task_id}-{run_id_short(run_id)}-{try_number}"
    )


def tests_job_short_id_from_taskinstance(taskinstance):
    dag_id, task_id, run_id, try_number, _ = taskinstance.key
    assert (
        job_short_id_from_taskinstance(taskinstance)
        == f"{dag_id}-{task_id}-{run_id_short(run_id)}-{try_number}"
    )
