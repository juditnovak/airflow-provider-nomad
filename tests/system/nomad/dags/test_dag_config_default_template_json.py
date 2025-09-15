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

import os
from datetime import timedelta

import attrs
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "test-config-default-job-template-json"
JOB_NAME = "task-test-config-default-job-template-json"
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
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["test", "config", "json_template"],
) as dag:
    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )

    run_this = BashOperator(
        task_id="placeholder",
        bash_command="echo 'Testing default_job_template' parameter with JSON",
    )

    run_this >> run_this_last


# # Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
try:
    from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

    from airflow.providers.nomad.executors.nomad_executor import conf
    from airflow.providers.nomad.constants import CONFIG_SECTION

    from ..constants import TEST_DATA_PATH

    conf.set(
        CONFIG_SECTION,
        "default_job_template",
        str(TEST_DATA_PATH / "nomad_provider_job_template.json"),
    )
    test_run = get_test_run(dag)
except ImportError:
    pass
