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



Nomad Python Task Decorator
============================

.. note:: If a Nomad Decorator is used with ``NomadExecutor``, it introduces the overhead of two task executions each time (as ``NomadExecutor`` is running every task as a Nomad job). In case this is undesirable, Airflow can be configured with multiple executors, and Nomad Operators can be used with ``LocalExecutor``

The ``@task.nomad`` decorator is a wrapper around the ``NomadPythonTaskOperator``. For a detailed description please refer to the `Nomad Python Task Operator <nomad_python_task_operator.html>`_ documentation. 

Submission scheme with ``LocalExecutor`` (recommended):

.. |local_exec_nomad_py_task_dec| image:: images/nomad-airflow-LocalExecutor_NomadOperator.drawio.svg

.. raw:: html

    <div style="margin-left;auto;margin-right:auto;text-align:center:max-width:200px">
        <object data="_images/nomad-airflow-LocalExecutor_NomadOperator.drawio.svg" type="image/svg+xml"> </object>
    </div>

Submission scheme with ``NomadExecutor``:

.. |nomad_exec_nomad_py_task_dec| image:: images/nomad-airflow-NomadExecutor_with_nomad_ops.drawio.svg

.. raw:: html

    <div style="margin-left;auto;margin-right:auto;text-align:center:max-width:200px">
        <object data="_images/nomad-airflow-NomadExecutor_with_nomad_ops.drawio.svg" type="image/svg+xml"> </object>
    </div>

Airflow Jinja templating is supported also within the decorated function body.

.. caution:: Keep in mind that the Python code will be run a remote execution environment on a Nomad host. The Python code has to be self-contained, and the remote image has to include all libraries that may be needed (See `Examples <#examples>`_ below.)

Known issues
*********************

- No parameters can be supplied for the decorated function. (Note: This issue can be worked around --at least on a very basic level-- using templating and Xcom-s.)
- Only the return value Xcoms are possible to set


Parameters
############

The ``@task.nomad`` decorator takes practically the same parameters as the ``NomadPythonTaskOperator``.

Instead, the content of the template could be added as the Python function body decorated by ``nomad_task``.

.. include:: shared/parameters_task_template.rst

.. include:: shared/parameters_task_exec.rst

``args (dict[str])``: Arguments to be added to the Docker image entrypoint/command.

.. warning:: Since the same argument is used to pass the Python code body of the decorated function to the Nomad Docker container, careful attention is required to make sure that a healthy Docker command will be constructed

.. include:: shared/parameters_task_resources.rst



Examples
##############


Additional examples are available at `Nomad Python Task Operator Examples <nomad_python_task_operator.html#examples>`_

Also, number of example DAGs are included in the project's `System Tests <system_tests.html#system-test-dags>`_ . See `DAG sources <_modules/index.html>`_ for further examples and basic use cases.


.. code-block:: Python

    from airflow.sdk import DAG, task

    content = """
    job "nomad-test-hcl" {
      type = "batch"

      group "example" {
        count = 1
        task "python" {
          driver = "docker"
          config {
            image = "python:3.12-alpine"
            entrypoint = ["python", "-c"]
          }
        }
      }
    }
    """.strip()


    with myDAG(
        dag_id=DAG_ID,
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        dagrun_timeout=timedelta(minutes=10),
        disable_bundle_versioning=True,
        catchup=False,
        tags=["nomad", "nomadtaskdecorator", "nomadexecutor", "nomad-provider-test"],
    ) as dag:

        @task.nomad(template_content=content, image="python:3.13-alpine", do_xcom_push=True)
        def nomad_hello():
            import sys

            print("Hello stderr", file=sys.stderr)
            print(42)

        @task.nomad(entrypoint=["python", "-c"])
        def nomad_pipe():
            print(
                "Result from the previous process is",
                "{{ task_instance.xcom_pull(task_ids='nomad_hello') }}",
            )

        nomad_hello() >> nomad_pipe()  # type: ignore [reportUnusedExpression]
