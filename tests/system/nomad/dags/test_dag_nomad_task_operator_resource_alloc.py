import datetime
import pendulum
import os

import attrs
from airflow.sdk import DAG

from airflow.providers.nomad.operators import NomadTaskOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "test-nomad-task-operator-resource-alloc"
JOB_NAME = "test-nomad-task-operator-resource-alloc"
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
job "nomad-test-hcl-op-param-volumes-%s" {
  type = "batch"

  group "example" {
    count = 1
    task "uptime" {
      driver = "docker"
      config {
        image = "alpine:latest"
        args = ["date"]
      }
    }
  }
}
""".strip()

resources = {"MemoryMB": 500, "DiskMB": 100, "Cores": 2}
ephemeral_disk = {"Size": 200, "Sticky": True}

vol_data = {
    "test_dags_folder": {
        "AccessMode": "",
        "AttachmentMode": "",
        "MountOptions": None,
        "Name": "dags",
        "PerAlloc": False,
        "ReadOnly": True,
        "Source": "dags",
        "Sticky": False,
        "Type": "host",
    }
}

vol_mounts_data = [
    {
        "Destination": "/opt/airflow/dags",
        "PropagationMode": "private",
        "ReadOnly": True,
        "SELinuxLabel": "",
        "Volume": "test_dags_folder",
    },
]

airflow_path = "/opt/airflow/dags"

with myDAG(
    dag_id=DAG_ID,
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    dagrun_timeout=datetime.timedelta(minutes=10),
    disable_bundle_versioning=True,
    catchup=False,
    tags=["nomad", "nomadtaskoperator", "nomadexecutor", "nomad-provider-test"],
) as dag:
    run_this_first = NomadTaskOperator(
        task_id="volume_mounts",
        template_content=content % ("{{ ti.id }}"),
        volumes=vol_data,
        volume_mounts=vol_mounts_data,
        args=["cat", f"{airflow_path}/templates/simple_batch.hcl"],
    )

    run_this_middle1 = NomadTaskOperator(
        task_id="resources", template_content=content % ("{{ ti.id }}"), task_resources=resources
    )

    run_this_middle2 = NomadTaskOperator(
        task_id="ephemeral",
        template_content=content % ("{{ ti.id }}"),
        ephemeral_disk=ephemeral_disk,
    )

    run_this_last = NomadTaskOperator(
        task_id="all_together",
        template_content=content % ("{{ ti.id }}"),
        task_resources=resources,
        ephemeral_disk=ephemeral_disk,
        volumes=vol_data,
        volume_mounts=vol_mounts_data,
        args=["cat", "/opt/airflow/dags/templates/simple_batch.hcl"],
    )

    run_this_first >> run_this_middle1 >> run_this_middle2 >> run_this_last  # type: ignore [reportUnusedExpression]

# # Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
try:
    from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

    test_run = get_test_run(dag)

except ImportError:
    pass
