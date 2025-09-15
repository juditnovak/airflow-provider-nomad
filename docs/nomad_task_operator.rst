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


Nomad Task Operator
======================

.. note:: If a Nomad Operator is used with ``NomadExecutor``, it introduces the overhead of two task executions each time (as ``NomadExecutor`` is running every task as a Nomad job). In case this is undesirable, Airflow can be configured with multiple executors, and Nomad Operators can be used with ``LocalExecutor``

The ``NomadTaskOperator`` is spawning a new Nomad job to run the wrapped task.
The operator is handy to run Nomad jobs with lightweight, minimal configuration.

Optionally a template can be specified as well, as JSON, HCL or Python dictionary.
However the template must not have more than a single ``TaskGroup`` an a single ``Task`` within.


Parameters
############

``template_content (str)``: A nomad job in the shape of either JSON, HCL or a Python dictionary. If not specified, then the default Nomad Executor template is used

``image (str)``: The docker image to be run

``entrypoint (list[str])``: Entrypoint to the Docker image (incompatible with ``command``)

``args (dict[str])``: Arguments to be added to the Docker image entrypoint

``command (dict[str])``: Command to be run by the Docker image (incompatible with ``entrypoint``)


Examples
##############

.. code-block:: Python

    with DAG(
        dag_id="nomad-task-example"
        schedule="0 0 * * *",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=60),
        tags=["nomad-example"],
        params=ParamsDict({"image": "alpine:latest", "args": ["date"]}),
    ) as dag:
        run_this_first = NomadTaskOperator(task_id="nomad_task", do_xcom_push=True)

        run_this_last = BashOperator(
            task_id="bash_task",
            bash_command="echo 'Nomad executoin time was: {{ task_instance.xcom_pull(task_ids='nomad_task') }}'",
        )

        run_this_first >> run_this_last

