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

The ``@task.nomad_job`` decorator is a wrapper around the ``NomadJobOperator``. For a detailed description please refer to the `Nomad Job Operator <nomad_job_operator.html>`_ documentation. 


Parameters
############

The ``@task.nomad_job`` decorator takes the same parameters as the ``NomadJobOperator``, except the ``template_content`` parameter.

Instead, the content of the template could be added as the Python function body decorated by ``@task.nomad_job``.


Examples
##############


.. code-block:: Python


    from airflow.sdk import DAG, task

    @dag()
    def test_nomad_task_decorator_dag():

        @task.nomad_job()
        def nomad_command_date():
            now = time.time()
            content = """
            job "nomad-test-hcl-%s" {
              type = "batch"

              group "example" {
                count = 1
                task "uptime" {
                  driver = "docker"
                  config {
                    image = "alpine:latest"
                    args = ["date"]
                  }
                }
              }
            }
            """.strip()

            return content % now

        nomad_command_date()
