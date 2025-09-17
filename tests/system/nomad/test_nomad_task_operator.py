###############################################################################
# These tests assume that a Nomad service is available as appears
# in the test configuration
###############################################################################


from unittest.mock import MagicMock

import pytest
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
def test_nomad_task_operator_execute_fails():
    op = NomadTaskOperator(task_id="task_op_test_wrong_image")

    runtime_ti = MagicMock(
        task_id="task_op_test_wrong_image",
        dag_id="task_op_dag",
        run_id="run_id",
        try_number=1,
        map_index=-1,
    )
    context = Context({"params": {"image": "non-existent-image"}, "ti": runtime_ti})

    with pytest.raises(NomadOperatorError) as err:
        op.execute(context)

    job_id = op.template.Job.ID  # type: ignore [reportOptionalMemberAccess]

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
