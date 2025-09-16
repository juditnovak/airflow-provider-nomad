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

import datetime
import os
from time import time

import attrs
import pendulum
from airflow.sdk import DAG

from airflow.providers.nomad.operators.nomad_job import NomadJobOperator
from airflow.providers.nomad.operators.nomad_task import NomadTaskOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "test-nomad-task-operator-combined"
JOB_NAME = "task-test-config-default-job-template-hcl"
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
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["nomad", "nomadjoboperator", "nomadexecutor"],
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
        args = ["echo -n The 1st task told me: {{ task_instance.xcom_pull(task_ids='nomad_job1') }}"]
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
        image="alpine:3.21",
        entrypoint=["/bin/sh", "-c"],
        args=["echo -n 'I heard from Job 2. that: '$THIRDVAR"],
        env={"THIRDVAR": "{{ task_instance.xcom_pull(task_ids='nomad_job2') }}"},
        do_xcom_push=True,
    )

    run_this_last = NomadTaskOperator(
        task_id="nomad_job4",
        image="alpine:3.21",
        entrypoint=["/bin/sh", "-c"],
        args=[
            "echo -n Finally Job 3. said so: {{ task_instance.xcom_pull(task_ids='nomad_job3') }}"
        ],
        do_xcom_push=True,
    )

    run_this_first >> run_this_middle1 >> run_this_middle2 >> run_this_last


# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
try:
    from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

    test_run = get_test_run(dag)

except ImportError:
    pass


if __name__ == "__main__":
    dag.test()
