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

import logging
import pytest
from unittest.mock import MagicMock

from airflow.sdk import Context

from tests_common.test_utils.config import conf_vars
from airflow.providers.nomad.exceptions import NomadOperatorError
from airflow.providers.nomad.operators.nomad_task import NomadTaskOperator


def test_nomad_task_operator_execute(test_datadir):
    file_path = test_datadir / "simple_batch.hcl"
    content = open(file_path).read()

    op = NomadTaskOperator(task_id="task_op_test")
    runtime_ti = MagicMock(
        task_id="task_op_test",
        dag_id="task_op_dag",
        run_id="run_id",
        try_number=1,
        map_index=-1,
    )
    context = Context(
        {
            "params": {"template_content": content, "image": "alpine:3.21", "args": ["date"]},
            "ti": runtime_ti,
        }
    )

    op.execute(context)

    job_id = op.template.Job.ID  # type: ignore[optionalMemberAccess, union-attr]
    job_summary = op.nomad_mgr.nomad.job.get_summary(job_id)  # type: ignore[optionalMemberAccess, union-attr]
    job_info = op.nomad_mgr.nomad.job.get_job(job_id)  # type: ignore[optionalMemberAccess, union-attr]

    summary = job_summary["Summary"]["example"]
    assert summary["Complete"] > 0
    assert summary["Failed"] == 0
    assert summary["Lost"] == 0
    assert summary["Queued"] == 0
    assert summary["Running"] == 0
    assert summary["Starting"] == 0
    assert summary["Unknown"] == 0

    config = job_info["TaskGroups"][0]["Tasks"][0]["Config"]
    assert config["image"] == "alpine:3.21"
    assert config["args"] == ["date"]


@conf_vars({("nomad_provider", "alloc_pending_timeout"): "0"})
def test_nomad_task_operator_execute_fails(caplog):
    op = NomadTaskOperator(task_id="task_op_test_wrong_image")

    runtime_ti = MagicMock(
        task_id="task_op_test_wrong_image",
        dag_id="task_op_dag",
        run_id="run_id",
        try_number=1,
        map_index=-1,
    )
    context = Context({"params": {"image": "non-existent-image"}, "ti": runtime_ti})

    with caplog.at_level(logging.INFO):
        with pytest.raises(NomadOperatorError) as err:
            op.execute(context)

    job_id = op.template.Job.ID  # type: ignore [reportOptionalMemberAccess]

    error = f"Task {job_id} seems dead, stopping it"
    assert error in caplog.text
    assert (
        "Failed to pull `non-existent-image`: Error response from daemon: "
        "pull access denied for non-existent-image, repository does not exist"
    ) in str(err.value)

    job_summary = op.nomad_mgr.nomad.job.get_summary(job_id)  # type: ignore[optionalMemberAccess, union-attr]
    summary = job_summary["Summary"]["airflow-execution-taskgroup"]
    assert summary["Complete"] == 0
    assert summary["Failed"] > 1
    assert summary["Lost"] == 0
    assert summary["Queued"] == 0
    assert summary["Running"] == 0
    assert summary["Starting"] == 0
    assert summary["Unknown"] == 0
