import datetime
import os
from time import time

import attrs
from airflow.sdk import DAG

from airflow.providers.nomad.operators.job import NomadJobOperator
from airflow.providers.nomad.operators.task import NomadTaskOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "test-nomad-task-operator-combined-localexecutor"
JOB_NAME = "test-nomad-task-operator-combined-localexecutor"
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

  group "example" {
    count = 1
    task "first" {
      driver = "docker"
      config {
        image = "alpine:latest"
        entrypoint = ["/bin/sh", "-c"]
        args = ["echo -n $STARTVAR"]
      }
      env {
        STARTVAR = "Message from 1st task"
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
    tags=["nomad", "nomadjoboperator", "nomad-provider-test-localexecutor"],
    default_args={"executor": "LocalExecutor"},
) as dag:
    run_this_first = NomadJobOperator(
        task_id="nomad_job1", template_content=content, do_xcom_push=True
    )

    run_this_middle1 = NomadJobOperator(
        task_id="nomad_job2",
        template_content="""
job "nomad-test-hcl-%s" {
  type = "batch"

  group "example" {
    task "second" {
      driver = "docker"
      config {
        image = "alpine:latest"
        entrypoint = ["/bin/sh", "-c"]
        args = ["echo -n $SECONDVAR"]
      }
      env {
        SECONDVAR = "The 1st task told me: {{ task_instance.xcom_pull(task_ids='nomad_job1') }}"
      }
    }
  }
}
""".strip()
        % time(),
        do_xcom_push=True,
    )

    run_this_middle2 = NomadTaskOperator(
        task_id="nomad_job3",
        params={
            "image": "alpine:3.21",
            "entrypoint": ["/bin/sh", "-c"],
            "args": ["echo -n 'I heard from Job2 that: '$THIRDVAR"],
        },
        env={"THIRDVAR": "{{ task_instance.xcom_pull(task_ids='nomad_job2') }}"},
        do_xcom_push=True,
    )

    run_this_last = NomadTaskOperator(
        task_id="nomad_job4",
        params={
            "image": "alpine:3.21",
            "entrypoint": ["/bin/sh", "-c"],
            "args": ["echo -n 'Finally Job3 said so: '$FOURTHVAR"],
        },
        env={"FOURTHVAR": "{{ task_instance.xcom_pull(task_ids='nomad_job3') }}"},
        do_xcom_push=True,
    )

    run_this_first >> run_this_middle1 >> run_this_middle2 >> run_this_last  # type: ignore [reportUnusedExpression]


# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
try:
    from airflow.configuration import conf
    from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

    from ..constants import TEST_DAGS_LOCALEXECUTOR_PATH

    if conf.get("core", "executor") == "LocalExecutor":
        os.environ["TEST_DAGS_PATH"] = str(TEST_DAGS_LOCALEXECUTOR_PATH)
        test_run = get_test_run(dag)

except ImportError:
    pass
