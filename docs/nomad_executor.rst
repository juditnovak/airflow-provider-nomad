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
