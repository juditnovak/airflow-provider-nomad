import datetime
import pendulum
import os

import attrs
from airflow.sdk import DAG, task


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "test-nomad-python-task-decorator-localexecutor"
JOB_NAME = "test-nomad-python-task-decorator-localexecutor"
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


content = """
job "nomad-test-hcl" {
  type = "batch"

  group "example" {
    count = 1
    task "python" {
      driver = "docker"
      config {
        image = "python:3.12-alpine"
        entrypoint = ["python", "-c"]
      }
    }
  }
}
""".strip()


with myDAG(
    dag_id=DAG_ID,
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    dagrun_timeout=datetime.timedelta(minutes=10),
    disable_bundle_versioning=True,
    catchup=False,
    tags=["nomad", "nomadtaskdecorator", "nomad-provider-test-localexecutor"],
    default_args={"executor": "LocalExecutor"},
) as dag:

    @task.nomad(template_content=content, image="python:3.13-alpine", do_xcom_push=True)
    def nomad_hello():
        import sys

        print("Hello stderr", file=sys.stderr)
        print(42)

    @task.nomad()
    def nomad_pipe():
        print(
            "Result from the previous process is",
            "{{ task_instance.xcom_pull(task_ids='nomad_hello') }}",
        )

    nomad_hello() >> nomad_pipe()  # type: ignore [reportUnusedExpression]


# # Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
try:
    from airflow.configuration import conf
    from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

    from ..constants import TEST_DAGS_LOCALEXECUTOR_PATH

    os.environ["TEST_DAGS_PATH"] = str(TEST_DAGS_LOCALEXECUTOR_PATH)

    # Sadly none of the DAG executor settings are considered in the test environment
    # Running it only in a pre-configured environment
    if conf.get("core", "executor") == "LocalExecutor":
        test_run = get_test_run(dag)
except ImportError:
    pass
