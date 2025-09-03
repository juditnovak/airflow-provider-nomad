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


.. _NomadExecutor:

Nomad Executor
===================

.. note::

    The executor requires Airflow version 3.0.0 or higher.

The Nomad executor runs each task instance in its own container on a Nomad cluster.

NomadExecutor runs as a process in the Airflow Scheduler. The scheduler itself does
not at all need to be running on Nomad, but does need API access to a Nomad cluster.

When a DAG submits a task, the NomadExecutor requests a job execution via the Nomad API.


Usage
#################

Pre-requisites
**********************

Assuming that a Nomad cluster is already available, there must be a few configuration details
to be adjusted on the cluster's side.

Airflow executions will require two shared folders on each runner:

.. code-block::
    :caption: Nomad client HCL configuration template

    client {
      host_volume "config" {
        path      = "<server_path>/<runner_space>/config"
        read_only = true
      }

      host_volume "dags" {
        path      = "<server_path>/dags/"
        read_only = true
      }


(Currently only host volumes are supported. Extending soon.)

 * ``dags``: Airflow runners need access to the dags that are to be executed
 * ``config``: Airflow runners need access to a local ``airflow.cfg``.
   * NOTE: This configuration is different from the Airflow server's configuration in terms of the following:

   
.. code-block::
    :caption: Nomad runner Airflow configuration

    [core]
    dags_folder = /opt/airflow/dags
    base_url = <airflow_api_server_url>

    [logging]
    base_log_folder = /opt/airflow/logs




Enabling the `NomadExecutor`
**********************************

To enable ``NomadExecutor`` to perform action, the Airflow Scheduler service
configuration has to contain the following information:

.. code-block::

   [core]
   executor = airflow.providers.nomad.executors.nomad_executor.NomadExecutor

   [nomad_executor]
   server_ip = <server_ip>


After these changes, the scheduler has to be restarted with 

.. code-block::

    airflow scheduler


From this point on Airflow pipeline tasks will be submitted to the target Nomad cluster.

Each task is executed as a separate Nomad task, in an individual taskgroup. This maps
each of them to different docker containers.


Logging
#############

``NomadExecutor`` supports the default Airflow logger (``FileTaskHandler`` or ``task``). This is the preferred
method to be used for remote logging (or in case Nomad logs may be locally mounted).

In case none of the above, ``NomadLoghandler`` may be enabled (see `NomadLoghandler <nomad_logger.html>`_).




