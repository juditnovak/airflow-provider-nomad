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

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.sdk.definitions.param import ParamsDict

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

JOB_NAME = "test-pi"
JOB_NAMESPACE = "default"

with DAG(
    dag_id="example_bash_operator_judit",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["example", "example2"],
    params=ParamsDict({"example_key": "example_value"}),
) as dag:
    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )

    # [START howto_operator_bash]
    run_this = BashOperator(
        task_id="run_after_loop",
        bash_command="echo https://airflow.apache.org/",
    )
    # [END howto_operator_bash]

    run_this >> run_this_last

    for i in range(3):
        task = BashOperator(
            task_id=f"runme_{i}",
            bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
        )
        task >> run_this

    # [START howto_operator_bash_template]
    also_run_this = BashOperator(
        task_id="also_run_this",
        bash_command='echo "ti_key={{ task_instance_key_str }}"',
    )
    # [END howto_operator_bash_template]
    also_run_this >> run_this_last

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


# ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
# DAG_ID = "spark_pi"
#
# with DAG(
#     DAG_ID,
#     default_args={"max_active_runs": 1},
#     description="submit spark-pi as sparkApplication on kubernetes",
#     schedule=timedelta(days=1),
#     start_date=datetime(2021, 1, 1),
#     catchup=False,
# ) as dag:
#     # [START SparkKubernetesOperator_DAG]
#     pi_example_path = pathlib.Path(__file__).parent.resolve()
#     t1 = SparkKubernetesOperator(
#         task_id="spark_pi_submit",
#         namespace="default",
#         application_file=join(
#             pi_example_path, "example_spark_kubernetes_spark_pi.yaml"
#         ),
#         do_xcom_push=True,
#         dag=dag,
#     )
#
#     t2 = SparkKubernetesSensor(
#         task_id="spark_pi_monitor",
#         namespace="default",
#         application_name="{{ task_instance.xcom_pull(task_ids='spark_pi_submit')['metadata']['name'] }}",
#         dag=dag,
#     )
#     t1 >> t2
#
#     # [END SparkKubernetesOperator_DAG]
#     from tests_common.test_utils.watcher import watcher
#
#     # This test needs watcher in order to properly mark success/failure
#     # when "tearDown" task with trigger rule is part of the DAG
#     list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
