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


Nomad Python Task Operator
============================

.. note:: If a Nomad Python Operator is used with ``NomadExecutor``, it introduces the overhead of two task executions each time (as ``NomadExecutor`` is running every task as a Nomad job). In case this is undesirable, Airflow can be configured with multiple executors, and Nomad Operators can be used with ``LocalExecutor``

The ``NomadPythonTaskOperator`` is spawning a new Nomad job to run the wrapped task.
The operator is handy to run Nomad jobs with lightweight, minimal configuration.

Submission scheme with ``LocalExecutor`` (recommended):

.. |local_exec_nomad_py_task_op| image:: images/nomad-airflow-LocalExecutor_NomadOperator.drawio.svg

.. raw:: html

    <div style="margin-left;auto;margin-right:auto;text-align:center:max-width:200px">
        <object data="_images/nomad-airflow-LocalExecutor_NomadOperator.drawio.svg" type="image/svg+xml"> </object>
    </div>

Submission scheme with ``NomadExecutor``:

.. |nomad_exec_nomad_py_task_op| image:: images/nomad-airflow-NomadExecutor_with_nomad_ops.drawio.svg

.. raw:: html

    <div style="margin-left;auto;margin-right:auto;text-align:center:max-width:200px">
        <object data="_images/nomad-airflow-NomadExecutor_with_nomad_ops.drawio.svg" type="image/svg+xml"> </object>
    </div>


Airflow Jinja templating is supported also within the python code submitted.

.. caution:: Keep in mind that the Python code will be run a remote execution environment on a Nomad host. The Python code has to be self-contained, and the remote image has to include all libraries that may be needed (See Examples also for `Nomad Python Task Decorator <nomad_python_task_decorator.html#examples>`_)

Known issues
*********************

- No parameters can be supplied for the decorated function. (Note: This issue can be worked around --at least on a very basic level-- using templating and Xcom-s.)
- Only the return value Xcoms are possible to set


Parameters
############

All parameters are supporting Airflow's Jinja-based templating mechanism.

``python_command``: The executable code to be run on the Nomad cluster node

.. include:: shared/parameters_task_template.rst

.. include:: shared/parameters_task_exec.rst

``args (dict[str])``: Arguments to be added to the Docker image entrypoint/command

.. warning:: Since the same argument is used to pass the Python code body of the decorated function to the Nomad Docker container, careful attention is required to make sure that a healthy Docker command will be constructed

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

Additional examples are available at `Nomad Python Task Decorator Examples <nomad_python_task_decorator.html#examples>`_

A number of example DAGs are included in the project's `System Tests <system_tests.html#system-test-dags>`_ . See `DAG sources <_modules/index.html>`_ for further examples and basic use cases.

.. code-block:: Python

    from airflow.sdk import DAG, task

    content = """
    job "nomad-test-hcl" {
      type = "batch"

      group "example" {
        count = 1
        task "uptime" {
          driver = "docker"
          config {
            image = "python:3.12-alpine"
            entrypoint = ["python", "-c"]
          }
        }
      }
    }
    """.strip()


    with DAG(dag_id="nomad_python_test") as dag:
        first = NomadPythonTaskOperator(
            task_id="nomad-hello",
            template_content=content,
            image="python:3.13-alpine",
            do_xcom_push=True,
            python_command="print (42)\n",
        )

        last = NomadPythonTaskOperator(
            task_id="nomad-pipe",
            python_command='print("Result from the previous process is ", '
            + "{{ task_instance.xcom_pull(task_ids='nomad-hello') }}"
            + ")",
        )

        first >> last
