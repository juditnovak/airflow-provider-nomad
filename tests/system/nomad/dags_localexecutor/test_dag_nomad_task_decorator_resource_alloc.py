import datetime
import os

import attrs
import pendulum
from airflow.sdk import DAG

from airflow.providers.nomad.decorators.task import nomad_task

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "test-nomad-task-decorator-param-volumes"
JOB_NAME = "task-test-task-decorator-param-volumes"
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
        args = ["uptime"]
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

with myDAG(
    dag_id=DAG_ID,
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["nomad", "nomadjoboperator", "nomadexecutor"],
) as dag:

    @nomad_task(
        template_content=content % ("{{ ti.id }}"),
        image="alpine:latest",
        volumes=vol_data,
        volume_mounts=vol_mounts_data,
    )
    def nomad_volumes_only():
        return ["cat", "/opt/airflow/dags/templates/simple_batch.hcl"]

    @nomad_task(
        template_content=content % ("{{ ti.id }}"), image="alpine:latest", task_resources=resources
    )
    def nomad_resources():
        return ["cat", "/proc/meminfo"]

    @nomad_task(
        template_content=content % ("{{ ti.id }}"),
        image="alpine:latest",
        ephemeral_disk=ephemeral_disk,
    )
    def nomad_ephemeral():
        return ["ls", "-la", "alloc/data"]

    @nomad_task(
        template_content=content % ("{{ ti.id }}"),
        image="alpine:latest",
        task_resources=resources,
        ephemeral_disk=ephemeral_disk,
        volumes=vol_data,
        volume_mounts=vol_mounts_data,
    )
    def nomad_all_in_one():
        return ["cat", "/opt/airflow/dags/templates/simple_batch.hcl"]

    nomad_volumes_only() >> nomad_resources() >> nomad_ephemeral() >> nomad_all_in_one()  # type: ignore [reportUnusedExpression]


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
