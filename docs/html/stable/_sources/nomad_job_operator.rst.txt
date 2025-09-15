.. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


Nomad Job Operator
======================

.. note:: If a Nomad Operator is used with ``NomadExecutor``, it introduces the overhead of two task executions each time (as ``NomadExecutor`` is running every task as a Nomad job). In case this is undesirable, Airflow can be configured with multiple executors, and Nomad Operators can be used with ``LocalExecutor``

The ``NomadJobOperator`` is spawning a new Nomad job to run the wrapped task.
Practically the operator is useful when complete job templates are to be run as separate Nomad executions.

There is no restriction on the template (number of ``Tasks``, or ``TaskGroups``, etc.)


Parameters
############

``template_content (str)``: A nomad job in the shape of either JSON, HCL or a Python dictionary

``observe``: Supervise the lifetime of the spawned Nomad execution, or abandon it as a "daemon" task. NOTE: In this case the job is loosing track from an Airflow point of view



Examples
##############


.. code-block:: Python

    content = """
    job "nomad-test-hcl" {
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


    with DAG(
        dag_id="nomad-job-example"
        schedule="0 0 * * *",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=60),
        tags=["nomad-example"],
        params=ParamsDict({"template_content": content}),
    ) as dag:
        run_this_first = NomadJobOperator(task_id="nomad_task", do_xcom_push=True)

        run_this_last = BashOperator(
            task_id="bash_task",
            bash_command="echo 'Uptime was: {{ task_instance.xcom_pull(task_ids='nomad_task') }}'",
        )

        run_this_first >> run_this_last
