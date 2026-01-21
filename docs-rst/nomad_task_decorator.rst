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



Nomad Task Decorator
======================

.. note:: If a Nomad Decorator is used with ``NomadExecutor``, it introduces the overhead of two task executions each time (as ``NomadExecutor`` is running every task as a Nomad job). In case this is undesirable, Airflow can be configured with multiple executors, and Nomad Operators can be used with ``LocalExecutor``

The ``@task.nomad_task`` decorator is a wrapper around the ``NomadTaskOperator``. For a detailed description please refer to the `Nomad Task Operator <nomad_task_operator.html>`_ documentation. 

.. include:: shared/task_intro_warning.rst 


Parameters
############

The ``@task.nomad_task`` decorator takes the same parameters as the ``NomadTaskOperator``.

Instead, the content of the template could be added as the Python function body decorated by ``nomad_task``.

.. include:: shared/parameters_task_template.rst

.. include:: shared/parameters_task_exec.rst

``args (dict[str])``: Arguments to be added to the Docker image entrypoint/command.

.. warning:: Since the same argument is used to pass the output of the Operator Function to the Nomad Docker container, careful attention is required to make sure that a healthy Docker command will be constructed

.. include:: shared/parameters_task_resources.rst


Examples
##############


An additional list of usage examples is available at `Nomad Task Operator Examples <nomad_task_operator.html#examples>`_


.. code-block:: Python

    from airflow.sdk import dag, task

    @dag()
    def test_nomad_task_decorator_dag():

        @task.nomad_task(template_content=content, image="alpine:latest", do_xcom_push=True)
        def nomad_command_nproc():
            return ["nproc", "--all"]

        @task.nomad_task()
        def nomad_command_response():
            return [
                "echo",
                "Runner node has ",
                "{{ task_instance.xcom_pull(task_ids='nomad_command_nproc') }}",
                " CPUs",
            ]

        nomad_command_nproc() >> nomad_command_response()

.. code-block:: Python

    from airflow.sdk import dag, task

    @dag()
    def test_nomad_task_decorator_dag():

        @task.nomad_task(template_content=content, image="alpine:latest", args=["python", "-c"])
        def nomad_command():
            return ["print('This should work!')"]


        nomad_command()


