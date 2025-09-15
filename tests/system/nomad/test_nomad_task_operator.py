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

from unittest.mock import MagicMock

from airflow.sdk import Context

from airflow.providers.nomad.operators.nomad_task import NomadTaskOperator


def test_nomad_task_operator_execute(test_datadir):
    file_path = test_datadir / "simple_batch.hcl"
    content = open(file_path).read()

    op = NomadTaskOperator(task_id="task_op_test")

    context = Context(
        {
            "params": {"template_content": content, "image": "alpine:3.21", "args": ["date"]},
            "ti": MagicMock(
                task_id="task_op_test",
                dag_id="task_op_dag",
                run_id="run_id",
                try_number=1,
                map_index=-1,
            ),
        }
    )

    op.execute(context)

    job_id = "nomad-task-task_op_dag-task_op_test-run_id-1--1"
    job_summary = op.nomad_mgr.nomad.job.get_summary(job_id)  # type: ignore[optionalMemberAccess, union-attr]
    job_info = op.nomad_mgr.nomad.job.get_job(job_id)  # type: ignore[optionalMemberAccess, union-attr]

    assert job_summary["Summary"]["example"]["Complete"] > 0
    assert job_summary["Summary"]["example"]["Failed"] == 0
    assert job_summary["Summary"]["example"]["Lost"] == 0
    assert job_summary["Summary"]["example"]["Queued"] == 0
    assert job_summary["Summary"]["example"]["Running"] == 0
    assert job_summary["Summary"]["example"]["Starting"] == 0
    assert job_summary["Summary"]["example"]["Unknown"] == 0

    config = job_info["TaskGroups"][0]["Tasks"][0]["Config"]
    assert config["image"] == "alpine:3.21"
    assert config["args"] == ["date"]
