 .. This file is part of apache-airflow-providers-nomad which is
    released under Apache License 2.0. See file LICENSE or go to

       http://www.apache.org/licenses/LICENSE-2.0

 .. for full license details.

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

.. include:: shared/task_intro_warning.rst 

Parameters
############

All parameters are supporting Airflow's Jinja-based templating mechanism.


.. include:: shared/parameters_task_template.rst

.. include:: shared/parameters_task_exec.rst

``args (dict[str])``: Arguments to be added to the Docker image entrypoint/command

.. include:: shared/parameters_task_resources.rst


The same parameters are also recognized if submitted as DAG/task ``params``. However in this case templating may not apply. 


Configuration
###############

``operator_poll_delay (int)``: Wait time between repeating checks on submitted child job

``runner_log_dir (str)``: Location where the runner may be writing logs (within the container). Default: ``/tmp``.


Job submission
################

Job submission and execution are following the the principles of `Nomad Executor Job submission <nomad_provider.html#job-execution>`_.

The job is submitted by the Operator using a unique Job ID (overriding the corresponding field of the template).

The operator is refreshing information about the spawned task state every ``nomad_provider/operator_poll_delay`` intervals.


 .. _examples-label:

Examples
##############


An additional list of usage examples is available at `Nomad Task Decorator Examples <nomad_task_decorator.html#examples>`_

A number of example DAGs are included in the project's `System Tests <system_tests.html#system-test-dags>`_ . See `DAG sources <_modules/index.html>`_ for further examples and basic use cases.

Execution: using ``args`` (`Execution`_)

.. code-block:: Python

   from airflow.sdk import DAG
   from airflow.providers.nomad.operators import NomadTaskOperator 

    with DAG(dag_id="nomad-task-example") as dag:
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

Using ``entrypoint`` (`Execution`_) and templating

.. code-block:: Python

   from airflow.sdk import DAG
   from airflow.providers.nomad.operators import NomadTaskOperator 

    with DAG(dag_id="nomad-task-af-template") as dag:

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


Using ``volumes`` and ``volume_mounts`` (`Resources`_)

.. code-block:: Python

   from airflow.sdk import DAG
   from airflow.providers.nomad.operators import NomadTaskOperator 

    content = """
    job "nomad-test-hcl-op-param-volumes-%s" {
      type = "batch"

      group "example" {
        count = 1
        task "uptime" {
          driver = "docker"
          config {
            image = "alpine:latest"
          }
        }
      }
    }
    """.strip()

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

    with DAG(dag_id="nomad-task-volume-mount") as dag:
        run_this_first = NomadTaskOperator(
            task_id="volume_mounts",
            template_content=content % ("{{ ti.id }}"),
            volumes=vol_data,
            volume_mounts=vol_mounts_data,
            args=["cat", f"{airflow_path}/templates/simple_batch.hcl"],
        )

Executing Python code on a Nomad runner:

.. code-block:: Python

    from airflow.sdk import DAG
    from airflow.providers.nomad.operators import NomadTaskOperator 

    with DAG(dag_id="nomad-python-task") as dag:
        nomad_op = NomadTaskOperator(    
              task_id="nomad_op",     
              image="python:3.12-alpine",
              entrypoint=["python", "-c"],    
              args=["""    
      import sys    
          
      print(f"Informative message", file=sys.stderr)    
      print(42)
          """    
              ],    
          )    
          
          chain(nomad_op)   



