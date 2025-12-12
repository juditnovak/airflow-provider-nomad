import datetime
import os
from time import time

import attrs
from airflow.sdk import DAG
from airflow.sdk.definitions.param import ParamsDict

from airflow.providers.nomad.operators.job import NomadJobOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "test-nomad-job-operator"
JOB_NAME = "test-nomad-job-operator"
JOB_NAMESPACE = "default"


##############################################################################
# DAG with relative location
#
# Airflow SDK DAGs are constucted with their absolute path ('fileloc' attr)
# instead of the relative location within the 'dags-folder'.
# In order to allow for remote job submission, this requires a patch
##############################################################################


def _default_fileloc() -> str:
    return os.path.basename(__file__)


def _all_after_dag_id_to_kw_only(cls, fields: list[attrs.Attribute]):
    i = iter(fields)
    f = next(i)
    if f.name != "dag_id":
        raise RuntimeError("dag_id was not the first field")
    yield f

    for f in i:
        yield f.evolve(kw_only=True)


@attrs.define(repr=False, field_transformer=_all_after_dag_id_to_kw_only, slots=False)  # pyright: ignore[reportArgumentType]
class myDAG(DAG):
    fileloc: str = attrs.field(init=False, factory=_default_fileloc)

    def __hash__(self):
        return super().__hash__()


##############################################################################


content = (
    """
job "nomad-test-hcl-%s" {
  type = "batch"

  constraint {
    attribute = "${attr.kernel.name}"
    value     = "linux"
  }

  group "example" {
    count = 1
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
    % time()
)


with myDAG(
    dag_id=DAG_ID,
    dagrun_timeout=datetime.timedelta(minutes=10),
    disable_bundle_versioning=True,
    catchup=False,
    tags=["nomad", "nomadjoboperator", "nomadexecutor", "nomad-provider-test"],
    params=ParamsDict({"template_content": content}),
) as dag:
    run_this_first = NomadJobOperator(task_id="nomad_job_from_content", template_content=content)

    run_this_last = NomadJobOperator(
        task_id="nomad_job_from_path", template_path="templates/simple_batch.json"
    )

    run_this_first >> run_this_last  # type: ignore [reportUnusedExpression]

# # Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
try:
    from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

    test_run = get_test_run(dag)

except ImportError:
    pass
