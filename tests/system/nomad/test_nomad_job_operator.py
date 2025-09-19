###############################################################################
# These tests assume that a Nomad service is available as appears
# in the test configuration
###############################################################################

from time import time

import pytest
from airflow.sdk import Context
from tests_common.test_utils.config import conf_vars

from airflow.providers.nomad.exceptions import NomadOperatorError
from airflow.providers.nomad.operators.job import NomadJobOperator


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
def test_nomad_job_operator_execute_fails():
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

    with pytest.raises(NomadOperatorError) as err:
        op.execute(context)

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
