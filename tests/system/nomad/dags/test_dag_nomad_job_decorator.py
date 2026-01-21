import datetime
import pendulum
import os
import time

import attrs
from airflow.sdk import DAG, task


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "test-nomad-job-decorator"
JOB_NAME = "test-nomad-job-decorator"
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

with myDAG(
    dag_id=DAG_ID,
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    dagrun_timeout=datetime.timedelta(minutes=10),
    disable_bundle_versioning=True,
    catchup=False,
    tags=["nomad", "nomadjobdecorator", "nomadexecutor", "nomad-provider-test"],
) as dag:
    # def test_nomad_task_decorator_dag():

    @task.nomad_job()
    def nomad_command_date():
        now = time.time()
        content = """
        job "nomad-test-hcl-%s" {
          type = "batch"

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

        return content % now

    nomad_command_date()


# # Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
try:
    from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

    test_run = get_test_run(dag)

except ImportError:
    pass
