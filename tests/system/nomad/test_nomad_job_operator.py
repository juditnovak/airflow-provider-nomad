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

from time import time
import logging
import pytest
from airflow.sdk import Context

from airflow.providers.nomad.operators.nomad_job import NomadJobOperator
from airflow.providers.nomad.exceptions import NomadOperatorError
from tests_common.test_utils.config import conf_vars


def test_nomad_job_operator_execute():
    job_id = f"nomad-job-operator-test-hcl-{time()}"
    hcl = (
        """
job "%s" {
  type = "batch"

  group "example" {
    count = 2
    task "uptime" {
      driver = "docker"
      config {
        image = "alpine:latest"
        args = ["uptime"]
      }
    }
  }
}
""".strip()
        % job_id
    )

    op = NomadJobOperator(task_id="task_id")
    context = Context({"params": {"template_content": hcl}})

    op.execute(context)
    job_summary = op.nomad_mgr.nomad.job.get_summary(job_id)  # type: ignore[optionalMemberAccess, union-attr]
    summary = job_summary["Summary"]["example"]
    assert summary["Complete"] > 0
    assert summary["Failed"] == 0
    assert summary["Lost"] == 0
    assert summary["Queued"] == 0
    assert summary["Running"] == 0
    assert summary["Starting"] == 0
    assert summary["Unknown"] == 0


@conf_vars({("nomad_provider", "alloc_pending_timeout"): "0"})
def test_nomad_job_operator_execute_fails(caplog):
    job_id = f"nomad-job-operator-fails-{time()}"
    hcl = (
        """
job "%s" {
  type = "batch"

  group "example" {
    count = 5
    task "uptime" {
      driver = "docker"
      config {
        image = "non-existent-image"
        args = ["uptime"]
      }
    }
    restart {
      attempts = 0
      mode = "fail"
    }
  }
}""".strip()
        % job_id
    )

    op = NomadJobOperator(task_id="job_op_test_wrong_image")
    context = Context({"params": {"template_content": hcl}})

    with caplog.at_level(logging.INFO):
        with pytest.raises(NomadOperatorError) as err:
            op.execute(context)

    error = f"Task {job_id} seems dead, stopping it"
    assert error in caplog.text
    assert (
        "Failed to pull `non-existent-image`: Error response from daemon: "
        "pull access denied for non-existent-image, repository does not exist"
    ) in str(err.value)

    job_summary = op.nomad_mgr.nomad.job.get_summary(job_id)  # type: ignore[optionalMemberAccess, union-attr]
    summary = job_summary["Summary"]["example"]
    assert summary["Complete"] == 0
    assert summary["Failed"] > 1
    assert summary["Lost"] == 0
    assert summary["Queued"] == 0
    assert summary["Running"] == 0
    assert summary["Starting"] == 0
    assert summary["Unknown"] == 0
