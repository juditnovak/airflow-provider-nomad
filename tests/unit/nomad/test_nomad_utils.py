# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json
import logging
import pytest

from airflow.providers.nomad.exceptions import NomadValidationError

from airflow.providers.nomad.utils import (
    dict_to_lines,
    parse_json_job_template,
    validate_nomad_job,
    validate_nomad_job_json,
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
    error = f"File '{str(file_path)}' not found"
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
