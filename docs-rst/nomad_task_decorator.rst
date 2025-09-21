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



Nomad Job Decorator
======================

.. note:: If a Nomad Decorator is used with ``NomadExecutor``, it introduces the overhead of two task executions each time (as ``NomadExecutor`` is running every task as a Nomad job). In case this is undesirable, Airflow can be configured with multiple executors, and Nomad Operators can be used with ``LocalExecutor``

The ``nomad_task`` decorator is a wrapper around the ``NomadTaskOperator``. For a detailed description please refer to the `Nomad Job Operator <nomad_job_operator.html>`_ documentation. 


Parameters
############

The ``nomad_task`` decorator takes the same parameters as the ``NomadTaskOperator``, except the ``args`` parameter.

Instead, the content of the template could be added as the Python function body decorated by ``nomad_task``.


Examples
##############


.. code-block:: Python

    @dag()
    def test_nomad_task_decorator_dag():

        @nomad_task(template_content=content, image="alpine:latest", do_xcom_push=True)
        def nomad_command_nproc():
            return ["nproc", "--all"]

        @nomad_task()
        def nomad_command_response():
            return [
                "echo",
                "Runner node has ",
                "{{ task_instance.xcom_pull(task_ids='nomad_command_nproc') }}",
                " CPUs",
            ]

        nomad_command_nproc() >> nomad_command_response()
