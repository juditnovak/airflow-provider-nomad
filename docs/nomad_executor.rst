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

.. code-block:: hcl
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
    }


(Currently only host volumes are supported. Extending soon.)

 * ``dags``: Airflow runners need access to the dags that are to be executed
 * ``config``: Airflow runners need access to a local ``airflow.cfg``.
   * NOTE: This configuration is different from the Airflow server's configuration in terms of the following:

   
.. code-block:: ini
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

.. code-block:: ini

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


Configuration
#################

Configuration options that are available for ``NomadExecutor`` are the following:

``parallelism``: Airflow executor setting: the maximum number of tasks to be run by this executor in parallel.

``server_ip``: The IP address of the Nomad server. (Default: ``127.0.0.1``)

``default_job_template``: If a custom HCL or JSON template is to be used for job submission instead of the in-built defaults. 

    Any image that may be appointed by this template is assumed to have Airflow task submission pre-requisites (``apache-airflow-core``, ``apache-airflow-task-sdk``) installed.


``secure``: Whether TLS is enabled. 

``verify``: This configuration may either hold a boolean value (``true``/``false``) or the absolute path of the CA certificate.

``cert_path``: The absolute path of the client certificate.

``key_path``: The absolute path of the client key.


For TLS-related configuration in detail, see the `Security`_ section.



Security
###############

Secure connection to a Nomad cluster is supported. 

In terms of Nomad configuration, corresponding Nomad certificates must be available
(see `Enable TLS encryption <https://developer.hashicorp.com/nomad/docs/secure/traffic/tls>`_).

We assume that the Nomad client is running a similar configuration to:

.. code-block:: hcl


    client {
      host_volume "config" {
        path      = "<server_path>/<runner_space>/config"
        read_only = true
      }

      host_volume "dags" {
        path      = "<server_path>/dags/"
        read_only = true
      }
    }

    # Require TLS

    tls {
      http = true
      rpc  = true

      ca_file   = "certs/nomad-agent-ca.pem"
      cert_file = "certs/global-client-nomad.pem"
      key_file  = "certs/global-client-nomad-key.pem"

      verify_server_hostname = false
      verify_https_client    = false
    }

The Airflow configuration of the Airflow scheduler (running ``NomadExecutor``) has to be changed such as

.. code-block:: ini

    [nomad]
    server_ip = <nomad_server_ip>
    cert_path = /home/devel/share/workspace_airflow/nomad_provider/certs/global-cli-nomad.pem
    key_path = /home/devel/share/workspace_airflow/nomad_provider/certs/global-cli-nomad-key.pem
    verify = /home/devel/share/workspace_airflow/nomad_provider/certs/nomad-agent-ca.pem
    secure = true


Having restarted the scheduler, job submission to the Nomad cluster is enabled.

.. note::

   In case of self-signed certificates, make sure that ``keyUsage`` extension is enabled and required (see `helpful guidelines <https://www.herongyang.com/PKI-Certificate/OpenSSL-Add-keyUsage-into-Root-CA.html>`_)


Logging
#############

``NomadExecutor`` supports the default Airflow logger (``FileTaskHandler`` or ``task``). This is the preferred
method to be used for remote logging (or in case Nomad logs may be locally mounted).

In case none of the above, ``NomadLoghandler`` may be enabled (see `NomadLoghandler <nomad_logger.html>`_).




