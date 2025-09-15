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
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from airflow.sdk import Context
from nomad.api.exceptions import BaseNomadException  # type: ignore[import-untyped]
from tests_common.test_utils.config import conf_vars

from airflow.providers.nomad.exceptions import NomadOperatorError
from airflow.providers.nomad.models import NomadJobModel
from airflow.providers.nomad.operators.nomad_task import NomadTaskOperator
from airflow.providers.nomad.templates.nomad_job_template import default_image


@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_nomad_task_operator_execute_ok(filename, test_datadir, mock_nomad_client):
    file_path = test_datadir / filename
    content = open(file_path).read()

    # The outer job will be running happily, the inner one fails
    file_path2 = test_datadir / "nomad_job_info_dead.json"
    mock_nomad_client.job.get_job.return_value = json.loads(open(file_path2).read())

    file_path3 = test_datadir / "nomad_job_allocations.json"
    file_path4 = test_datadir / "nomad_job_summary_success.json"
    file_path5 = test_datadir / "nomad_job_evaluation.json"
    mock_nomad_client.job.get_allocations.return_value = json.loads(open(file_path3).read())
    mock_nomad_client.job.get_summary.return_value = json.loads(open(file_path4).read())
    mock_nomad_client.job.get_evaluations.return_value = json.loads(open(file_path5).read())
    # The first read is on stderr, second is on stdout
    mock_nomad_client.client.cat.read_file.side_effect = [str({"Summary": 30}), ""]

    mock_job_register = mock_nomad_client.job.register_job
    context = Context(
        {
            "params": {"template_content": content},
            "ti": MagicMock(
                task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
            ),
        }
    )

    retval = NomadTaskOperator(task_id="task_id").execute(context)

    job_id = "nomad-task-dag_id-task_id-run_id-1--1"
    template = NomadJobModel.model_validate_json(content).model_dump(exclude_unset=True)
    template["Job"]["ID"] = job_id
    mock_job_register.assert_called_once_with(job_id, template)
    assert retval == str({"Summary": 30})


@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_nomad_task_operator_execute_ok_with_task_logs(
    filename, test_datadir, mock_nomad_client, caplog
):
    file_path = test_datadir / filename
    content = open(file_path).read()

    # The outer job will be running happily, the inner one fails
    file_path2 = test_datadir / "nomad_job_info_dead.json"
    mock_nomad_client.job.get_job.return_value = json.loads(open(file_path2).read())

    file_path3 = test_datadir / "nomad_job_allocations.json"
    file_path4 = test_datadir / "nomad_job_summary_success.json"
    file_path5 = test_datadir / "nomad_job_evaluation.json"
    mock_nomad_client.job.get_allocations.return_value = json.loads(open(file_path3).read())
    mock_nomad_client.job.get_summary.return_value = json.loads(open(file_path4).read())
    mock_nomad_client.job.get_evaluations.return_value = json.loads(open(file_path5).read())
    # The first read is on stderr, second is on stdout
    job_log = "Submitted task is saying hello"
    mock_nomad_client.client.cat.read_file.side_effect = [job_log, str({"Summary": 30}), ""]

    mock_job_register = mock_nomad_client.job.register_job
    context = Context(
        {
            "params": {"template_content": content},
            "ti": MagicMock(
                task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
            ),
        }
    )

    op = NomadTaskOperator(task_id="task_id", job_log_file="locallog.out")
    with caplog.at_level(logging.INFO):
        retval = op.execute(context)

        job_id = "nomad-task-dag_id-task_id-run_id-1--1"
        template = NomadJobModel.model_validate_json(content).model_dump(exclude_unset=True)
        template["Job"]["ID"] = job_id
        mock_job_register.assert_called_once_with(job_id, template)
        assert retval == str({"Summary": 30})
        assert any([job_log in record.message for record in caplog.records])


@conf_vars({("nomad_provider", "alloc_pending_timeout"): "0"})
@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_nomad_task_operator_execute_job_submission_fails(
    filename, test_datadir, mock_nomad_client
):
    file_path = test_datadir / filename
    content = open(file_path).read()

    mock_job_register = mock_nomad_client.job.register_job
    mock_job_register.side_effect = BaseNomadException("Job submission error")
    context = Context(
        {
            "params": {"template_content": content},
            "ti": MagicMock(
                task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
            ),
        }
    )
    # Job output
    mock_nomad_client.client.cat.read_file.side_effect = ["", ""]

    with pytest.raises(NomadOperatorError) as err:
        NomadTaskOperator(task_id="task_id").execute(context)

    job_id = "nomad-task-dag_id-task_id-run_id-1--1"
    template = NomadJobModel.model_validate_json(content).model_dump(exclude_unset=True)
    template["Job"]["ID"] = job_id
    mock_job_register.assert_called_once_with(job_id, template)
    assert str(err.value).startswith("Job submission failed")


@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_nomad_task_operator_execute_failed(filename, test_datadir, mock_nomad_client):
    file_path = test_datadir / filename
    content = open(file_path).read()

    # The outer job will be running happily, the inner one fails
    file_path2 = test_datadir / "nomad_job_info_dead.json"
    file_path3 = test_datadir / "nomad_job_allocations_pending.json"
    file_path4 = test_datadir / "nomad_job_summary_failed.json"
    mock_nomad_client.job.get_job.return_value = json.loads(open(file_path2).read())
    mock_nomad_client.job.get_allocations.return_value = json.loads(open(file_path3).read())
    mock_nomad_client.job.get_summary.return_value = json.loads(open(file_path4).read())
    # Job output
    mock_nomad_client.client.cat.read_file.side_effect = ["", ""]

    mock_job_register = mock_nomad_client.job.register_job
    context = Context(
        {
            "params": {"template_content": content},
            "ti": MagicMock(
                task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
            ),
        }
    )

    with pytest.raises(NomadOperatorError) as err:
        NomadTaskOperator(task_id="task_id").execute(context)

    job_id = "nomad-task-dag_id-task_id-run_id-1--1"
    template = NomadJobModel.model_validate_json(content).model_dump(exclude_unset=True)
    template["Job"]["ID"] = job_id
    mock_job_register.assert_called_once_with(job_id, template)
    assert str(err.value).startswith(f"Job summary:Job {job_id} got killed due to error")
    assert "Error response from daemon: pull access denied for novakjudi/af_nomad_test" in str(
        err.value
    )


def test_sanitize_logs():
    file_path = Path("alloc_id-task_name.log")
    try:
        log_content = "log\nfile\ncontent"
        logs = log_content
        assert log_content == NomadTaskOperator.sanitize_logs("alloc_id", "task_name", log_content)  # type: ignore [reportAttributeAccessIssue]
        assert file_path.is_file()

        more_log_content = "more\nlog\ncontent"
        logs += more_log_content
        assert more_log_content == NomadTaskOperator.sanitize_logs("alloc_id", "task_name", logs)  # type: ignore [reportAttributeAccessIssue]

        even_more_log_content = "even\nmuch\nmore\nlog\ncontent"
        logs += even_more_log_content
        assert even_more_log_content == NomadTaskOperator.sanitize_logs(
            "alloc_id", "task_name", logs
        )  # type: ignore [reportAttributeAccessIssue]
    finally:
        file_path.unlink()


def test_params():
    op = NomadTaskOperator(task_id="task_id")

    context = Context(
        {
            "params": {"image": "alpine:3.21", "entrypoint": ["/bin/sh", "-c"], "args": ["date"]},
            "ti": MagicMock(
                task_id="task_op_test",
                dag_id="task_op_dag",
                run_id="run_id",
                try_number=1,
                map_index=-1,
            ),
        }
    )
    template = op.prepare_job_template(context)
    assert template
    assert template.Job.TaskGroups[0].Tasks[0].Config.entrypoint == ["/bin/sh", "-c"]
    assert template.Job.TaskGroups[0].Tasks[0].Config.image == "alpine:3.21"
    assert template.Job.TaskGroups[0].Tasks[0].Config.args == ["date"]
    assert len(template.Job.TaskGroups[0].Tasks[0].Config.model_dump(exclude_unset=True)) == 3


def test_params_defaults():
    op = NomadTaskOperator(task_id="task_id")

    context = Context(
        {
            "params": {"args": ["date"]},
            "ti": MagicMock(
                task_id="task_op_test",
                dag_id="task_op_dag",
                run_id="run_id",
                try_number=1,
                map_index=-1,
            ),
        }
    )
    template = op.prepare_job_template(context)
    assert template
    assert template.Job.TaskGroups[0].Tasks[0].Config.image == default_image
    assert template.Job.TaskGroups[0].Tasks[0].Config.args == ["date"]
    assert len(template.Job.TaskGroups[0].Tasks[0].Config.model_dump(exclude_unset=True)) == 2


def test_params_invalid():
    op = NomadTaskOperator(task_id="task_id")

    context = Context(
        {
            "params": {"entrypoint": ["/bin/sh", "-c"], "command": ["uptime"]},
            "ti": MagicMock(
                task_id="task_op_test",
                dag_id="task_op_dag",
                run_id="run_id",
                try_number=1,
                map_index=-1,
            ),
        }
    )

    with pytest.raises(NomadOperatorError) as err:
        op.prepare_job_template(context)

    assert "Both 'entrypoint' and 'command' specified" in str(err.value)
