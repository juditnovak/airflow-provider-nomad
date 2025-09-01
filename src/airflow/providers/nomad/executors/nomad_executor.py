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


"""
Nomad Executor.
"""

from __future__ import annotations

import argparse
import copy
import logging
from typing import Any

import nomad  # type: ignore[import-untyped]
from airflow.cli.cli_config import GroupCommand
from airflow.configuration import conf
from airflow.models.taskinstancekey import TaskInstanceKey

from airflow.providers.nomad.generic_interfaces.executor_interface import ExecutorInterface
from airflow.providers.nomad.templates.nomad_job_template import default_task_template

NOMAD_COMMANDS = ()

# For Nomad: jobID,  taskID (within that job submission),  allocation
NomadJob = tuple[str, str, str]

PROVIDER_NAME = "nomad"

logger = logging.getLogger(__name__)


class NomadExecutor(ExecutorInterface):
    """Executor for Nomad."""

    RUNNING_POD_LOG_LINES = 100
    supports_ad_hoc_ti_run: bool = True
    EXECUTOR_NAME = "nomad_executor"

    def __init__(self):
        self.parallelism = conf.getint(PROVIDER_NAME, "parallelism", fallback=1)
        self.nomad_server_ip = conf.get(PROVIDER_NAME, "server_ip", fallback="0.0.0.0")
        self.secure = conf.getboolean(PROVIDER_NAME, "secure", fallback=False)
        self.cert_path = conf.get(PROVIDER_NAME, "cert_path", fallback="")
        self.key_path = conf.get(PROVIDER_NAME, "key_path", fallback="")
        self.verify = conf.getboolean(PROVIDER_NAME, "verify", fallback=False)
        self.namespace = conf.get(PROVIDER_NAME, "namespace", fallback="")
        self.token = conf.get(PROVIDER_NAME, "token", fallback="")

        self.nomad: nomad.Nomad | None = None
        self.nomad_jobs: dict[TaskInstanceKey, NomadJob] = {}

        super().__init__(parallelism=self.parallelism)

    def start(self) -> None:
        """Start the executor."""
        self.log.info("Start Nomad executor")

        self.nomad = nomad.Nomad(
            host=self.nomad_server_ip,
            secure=self.secure,
            cert=(self.cert_path, self.key_path),
            verify=self.verify,
            namespace=self.namespace,
            token=self.token,
        )
        assert self.nomad, "Nomad client unavailable"
        self.log.info("Nomad client initiated")

    @classmethod
    def job_id_from_taskinstance_key(cls, key: TaskInstanceKey) -> str:
        dag_id, task_id, run_id, try_number, map_index = key
        return f"{dag_id}-{task_id}-{run_id}-{try_number}-{map_index}"

    @classmethod
    def job_task_id_from_taskinstance_key(cls, key: TaskInstanceKey) -> str:
        dag_id, task_id, _, _, _ = key
        return f"{dag_id}-{task_id}"

    def apply_command_to_job_template(
        self, key: TaskInstanceKey, command: list[str]
    ) -> dict[str, Any]:
        """Apply command to the Nomad job template"""
        job_id = self.job_id_from_taskinstance_key(key)
        job_task_id = self.job_task_id_from_taskinstance_key(key)
        job_template = copy.deepcopy(default_task_template)

        job_template["Job"]["TaskGroups"][0]["Tasks"][0]["Config"]["args"] = command
        job_template["Job"]["TaskGroups"][0]["Tasks"][0]["Name"] = job_task_id
        job_template["Job"]["Name"] = f"airflow-run-{job_task_id}-{key[3]}"
        job_template["Job"]["ID"] = job_id

        self.log.debug(
            f"Command running: python -m airflow.sdk.execution_time.execute_workload --json-string '{command[0]}'\n"
        )
        return job_template

    def run_job(self, key: TaskInstanceKey, job_template: dict[str, Any]) -> None:
        """Execute the job described by job_template in Nomad."""
        assert self.nomad, "Nomad client unavailable"

        job_id = job_template["Job"]["ID"]
        job_task_id = job_template["Job"]["TaskGroups"][0]["Tasks"][0]["Name"]
        self.nomad.job.register_job(job_id, job_template)
        self.nomad_jobs[key] = (job_id, job_task_id, "")

        self.log.debug(f"Runing task ({job_id}) with template {job_template})")

    def retrieve_logs(self, key: TaskInstanceKey) -> tuple[list[str], list[str]]:
        # Note: this method is executed by the FileTaskHandler and not the Scheduler
        # We have no access to the running NomadExecutor's state
        if not self.nomad:
            self.start()

        assert self.nomad, "Couldn't initialize Nomad client"

        messages = []
        job_id = self.job_id_from_taskinstance_key(key)
        job_task_id = self.job_task_id_from_taskinstance_key(key)
        allocations = self.nomad.job.get_allocations(job_id)
        if len(allocations) == 0:
            messages.append(f"No allocations found for {job_id}/{job_task_id}")
        elif len(allocations) > 1:
            messages.append(
                f"Multiple allocations found found for {job_id}/{job_task_id}: {allocations}"
            )

        allocation_id = allocations[0].get("ID")
        if not allocation_id:
            messages.append(f"Allocation for {job_id}/{job_task_id} not found")
            return ([], messages)

        logs = self.nomad.client.cat.read_file(
            allocation_id, path=f"alloc/logs/{job_task_id}.stdout.0"
        )
        return messages, logs.splitlines()

    @staticmethod
    def get_cli_commands() -> list[GroupCommand]:
        return [
            GroupCommand(
                name=PROVIDER_NAME,
                help=f"Tools to help run the {PROVIDER_NAME} executor",
                subcommands=NOMAD_COMMANDS,
            )
        ]


def _get_parser() -> argparse.ArgumentParser:
    """
    Generate documentation; used by Sphinx.

    :meta private:
    """
    return NomadExecutor._get_parser()
