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



Nomad Job Operator
======================

.. note:: If a Nomad Operator is used with ``NomadExecutor``, it introduces the overhead of two task executions each time (as ``NomadExecutor`` is running every task as a Nomad job). In case this is undesirable, Airflow can be configured with multiple executors, and Nomad Operators can be used with ``LocalExecutor``

The ``NomadJobOperator`` is spawning a new Nomad job to run the wrapped task.
Practically the operator is useful when complete job templates are to be run as separate Nomad executions.

There is no restriction on the template (number of ``Tasks``, or ``TaskGroups``, ``Count``, etc.)

The Nomad job template is executed with no modification.

 .. note:: Only job submission with unique Job IDs is supported




Parameters
############


``observe (bool)``: Supervise the lifetime of the spawned Nomad execution, or abandon it as a "daemon" task. NOTE: In the latter case the Airflow is loosing track of the job

``template_path (str)``: Path to a Nomad job JSON or HCL file. if not specified, then the default nomad executor template is used

``template_content (str)``: A JSON or HCL string, or a Python dictionary.


Configuration
###############

``operator_poll_delay``: Wait time between repeating checks on submitted child job that is observed

``runner_log_dir (str)``: Location where the runner may be writing logs (within the container). Default: ``/tmp``.

Examples
##############


.. code-block:: Python

    content = """
    job "nomad-test-hcl" {
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


    with DAG(dag_id="nomad-job-example") as dag:
        run_this_first = NomadJobOperator(
            task_id="nomad_job",
            template_content=content,
            do_xcom_push=True
        )

        run_this_middle= NomadJobOperator(
            task_id="nomad_job_from_path",
            template_path="templates/simple_batch.json"
        )

        run_this_last = BashOperator(
            task_id="bash_task",
            bash_command="echo 'Uptime was: {{ task_instance.xcom_pull(task_ids='nomad_job') }}'",
        )

        run_this_first >> run_this_middle >> run_this_last


.. code-block:: Python

    content = """
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
    """.strip() % time()

    with DAG(dag_id=DAG_ID) as dag:
        run_this_first = NomadJobOperator(
            task_id="nomad_job1", template_content=content, do_xcom_push=True
        )

        run_this_last = NomadJobOperator(
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
    """.strip() % time(),
            do_xcom_push=True,
        )

    run_this_first >> run_this_last
