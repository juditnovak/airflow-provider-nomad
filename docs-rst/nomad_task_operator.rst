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

All parameters are supporting Airflow's Jinja-based templating mechanism.


Parameters
############

``image (str)``: The docker image to be run

``entrypoint (list[str])``: Entrypoint to the Docker image (incompatible with ``command``)

``args (dict[str])``: Arguments to be added to the Docker image entrypoint

``command (dict[str])``: Command to be run by the Docker image (incompatible with ``entrypoint``)

``env (dict[str, str])``: Environment variables specified as a Python dictionary

``template_path (str)``: Path to a Nomad job JSON or HCL file. Otherwise the default nomad executor template is used.

``template_content (str)``: A JSON or HCL string, or a Python dictionary. Otherwise, the default Nomad Executor template is used.

    In case a template was specified, it must have a single ``TaskGroup`` with a single ``Task`` within,
    and can only have a single execution (``Count`` is ``1``).

The same parameters are also recognized if submitted as DAG/task ``params``. However in this case templating may not apply. 


Configuration
###############

``operator_poll_delay (int)``: Wait time between repeating checks on submitted child job


Job submission
################

Job submission and execution are following the the principles of `Nomad Executor Job submission <nomad_provider.html#job-execution>`_.

The job is submitted by the Operator using a unique Job ID (overriding the corresponding field of the template).

The operator is refreshing information about the spawned task state every ``nomad_provider/operator_poll_delay`` intervals.


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
    ) as dag:
        run_this_first = NomadTaskOperator(
            task_id="nomad_task", 
            image="alpine:latest",
            args=["date"],
            do_xcom_push=True
        )

        run_this_last = BashOperator(
            task_id="bash_task",
            bash_command="echo 'Nomad executoin time was: {{ task_instance.xcom_pull(task_ids='nomad_task') }}'",
        )

        run_this_first >> run_this_last


.. code-block:: Python

    with DAG(
        dag_id="nomad-task-af-template",
        schedule="0 0 * * *",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=60),
        tags=["nomad", "nomadtask"],
    ) as dag:

        run_this_first = NomadTaskOperator(
            task_id="nomad_task1",
            image="alpine:3.21",
            entrypoint=["/bin/sh", "-c"],
            args=["echo -n $MYVAR"],
            env={"MYVAR": "{{ task_instance.xcom_pull(task_ids='nomad_job2') }}"},
            do_xcom_push=True,
        )

        run_this_last = NomadTaskOperator(
            task_id="nomad_task2",
            image="alpine:3.21",
            entrypoint=["/bin/sh", "-c"],
            args=["echo -n Job 1. said so: {{ task_instance.xcom_pull(task_ids='nomad_task2') }}" ],
        )

        run_this_first >> run_this_last
