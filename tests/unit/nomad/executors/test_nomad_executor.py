# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pytest
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.utils.state import State

from airflow.providers.nomad.executors.nomad_executor import NomadExecutor


def test_base():
    """Base functionality including processing config"""

    nomad_executor = NomadExecutor()

    assert nomad_executor
    assert nomad_executor.parallelism == 128


def test_connect():
    """Connection to the Nomad cluster"""

    nomad_executor = NomadExecutor()
    nomad_executor.start()

    assert nomad_executor.nomad
    assert nomad_executor.nomad.agent.get_members()


@pytest.mark.skipif(
    NomadExecutor is None, reason="nomad_provider python package is not installed"
)
def test_run_next(mock_nomad_client):
    """ """

    nomad_executor = NomadExecutor()
    nomad_executor.start()

    try:
        try_number = 1
        task_instance_key = TaskInstanceKey("dag", "task", "run_id", try_number)
        nomad_executor.execute_async(
            key=task_instance_key,
            queue=None,
            command=["airflow", "tasks", "run", "true", "some_parameter"],
        )
        nomad_executor.sync()

        # A job was registered
        assert mock_nomad_client.job.job_register.call_count == 0

        assert nomad_executor.task_queue.empty()
        assert nomad_executor.event_buffer[task_instance_key][0] == State.QUEUED
    finally:
        nomad_executor.end()
        pass
