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


``apache-airflow-providers-nomad``
============================================


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Basics

    Home <self>
.. Security <security>

.. toctree::
    :hidden:
    :maxdepth: 2
    :caption: Executors

    NomadExecutor <nomad_executor>

.. toctree::
    :hidden:
    :maxdepth: 2
    :caption: Operators

    NomadTaskOperator <nomad_task_operator>
    NomadJobOperator <nomad_job_operator>

.. toctree::
    :hidden:
    :maxdepth: 2
    :caption: Decorators

    NomadTaskDecorator <nomad_task_decorator>
    NomadJobDecorator <nomad_job_decorator>

.. toctree::
    :hidden:
    :maxdepth: 2
    :caption: Loggers

    NomadLoghandler <nomad_logger>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

..  Configuration <configurations-ref>
    Python API <_api/airflow/providers/nomad/index>
..  Nomad Job template <_api/airflow/providers/nomad/templates/nomad_job_template/index>

.. toctree::
    :hidden:
    :maxdepth: 2
    :caption: System tests

    System Tests <system_tests>
.. System Tests <_api/tests/system/nomad/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Development

    Changelog <changelog>


.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. .. toctree::
..     :hidden:
..     :maxdepth: 1
..     :caption: Commits
..
..     Detailed list of commits <commits>


Release: 0.0.2

.. Release Date: ``|PypiReleaseDate|``

Airflow provider for the `Hasicorp Nomad <https://developer.hashicorp.com/nomad/>`__ workload orchestrator. 


Installation
------------

You can install this package on top of an existing Airflow 3 installation via
``pip install apache-airflow-providers-nomad``.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``3.0.4``.

============================  ====================
PIP package                   Version required
============================  ====================
``apache-airflow-core``       ``>=3.0.4``
``apache-airflow-task-sdk``   (matching Airflow)
``python-nomad``              ``>=2.1.0``
============================  ====================
