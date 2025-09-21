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


System Tests
=============

.. note::

   AIRFLOW_HOME should be carefully pointed to a test area. Recommended setting: ``<systest_root>/server/airflow_home``


Nomad Provider System Tests involve a base level of complexity.

 * A Nomad server has to be present
 * And Airflow API has to be ready to process task updates
 * The local ``pytest`` environment has to load the same dags, as what will be made available for the runners


Structure
#############

In addition, keep in mind that ``airflow.cfg`` contains absolute path references. These all have to be adjusted
to wherever the test environment is executed. Furthermore, the Nomad client configuration also has to point
to the absolute paths of the necessary mount points (as described in `Usage <usage>`_)

To overcome the above issue, the Nomad Provider System Test environment is using templates, that
which are used for configuration generation on-the-fly.

The test environment is split to the following core functionalities:

 * ``server``: Airflow services (such as Airflow API, scheduler, etc.) space
 * ``runner``: Nomad runner's setup and configuration area

On an orthogonal angle, two more folders to be mentioned:

``dags``: This folder contains the majority of the tests -- all the DAGs that are triggered. They must be available both to the local ``pytest`` system, as 'test code', and for the Nomad runners as 'Airflow DAGs' (``dags-folder``, mounted by the Nomad client).

There are a few tests outside of the ``dags`` folder, performing actions live actions (that require a Nomad server), but don't use Airflow's DAG-based system test runs (i.e. ``system`` pytest marker).
These are the tests at the root of the System Tests.

``config``: Following Airflow standards, this folder holds the main reference template for local Airflow services. The rendered version of this file will be both loaded to the ``pytest`` environment, and used as ``airflow.cfg`` for local services (by creating a symlink in ``AIRFLOW_HOME`` to the rendered template).


Execution prerequisites
################################


Airflow Test Environment

    The Nomad Provider Test environment is using Airflow's development environment. (See dependency in ``pyptoject.toml`` on ``apache-airflow-devel-common``.)

    In addition, this requires to have the Apache source tree available. Having checked it out, the following must be set:

    Furthermore the ``_AIRFLOW__SYSTEM_TEST_USE_EXECUTOR`` variable also has to be set to enable executor usage (the test environment would bypass the scheduler otherwise).

.. code-block::

    export AIRFLOW_SOURCES=<airflow_codebase>
    export _AIRFLOW__SYSTEM_TEST_USE_EXECUTOR=1



Local Test Environment

    Before the first test run (or any changes on config templates), the ``scripts/init_airflow_cfg.sh`` script must be run.
    This will overwrite ``AIRFLOW_HOME/airflow.cfg`` using the test configuration templates.

.. warning:: This step will modify AIRFLOW_HOME/airflow.cfg

.. note:: The recommended ``AIRFLOW_HOME`` setting is the designated area within the ``server`` space: ``<nomad_systest_root>server/airflow_home``


Services
#############


Airflow web API

    Any Airflow services that are outside of the scope of testing must be executed outside of the ``pytest`` environment.

    In our case this is the web API server. Make sure to set the same ``AIRFLOW_HOME`` for the API server, as you are using for the tests.

.. code-block::

   uv run airflow run api-server


Nomad Agent

    Regarding the Nomad server and client, the test environment would start it up automatically at ``http://localhost:4646``, pre-configured
    with the test ``dags`` and ``runner/config`` folders mounted (with rendered config). This agent will be shut down at the end of the test run.

    if another Nomad service is preferred to be used during the tests, you should add the ``--no-nomad-agent`` argument to the test invocation.



Running the tests
#####################

And finally, after all the above was prepared and set, we can run the tests

.. code-block:: sh

    uv run tox run -e system_test


In case there may be complaints regarding the test DB, you can always run 

.. code-block:: sh

   uv airflow db reset

or add 

.. code-block:: sh

    uv run tox run -e system_test -- --with-db-init



System Test DAGs
=======================

The full list of `System Test Dags <_api/tests/system/nomad/dags/index.html>`_ 

NOTE: These are not the only System Tests executed against a live environment. See `System Tests <_api/tests/system/nomad/index.html>`_ for the full list.
