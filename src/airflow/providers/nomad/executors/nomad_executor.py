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
from functools import cached_property
from pathlib import Path
from typing import Any

import nomad  # type: ignore[import-untyped]
from airflow.cli.cli_config import GroupCommand
from airflow.configuration import conf
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import remove_escape_codes
from nomad.api.exceptions import BaseNomadException  # type: ignore[import-untyped]

from airflow.providers.nomad.exceptions import NomadValidationError
from airflow.providers.nomad.generic_interfaces.executor_interface import ExecutorInterface
from airflow.providers.nomad.models import NomadJobModel
from airflow.providers.nomad.nomad_log import NomadLogHandler
from airflow.providers.nomad.templates.nomad_job_template import default_task_template
from airflow.providers.nomad.utils import parse_hcl_job_template, parse_json_job_template

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
        self.parallelism: int = conf.getint(PROVIDER_NAME, "parallelism", fallback=1)
        self.nomad_server_ip: str = conf.get(PROVIDER_NAME, "server_ip", fallback="0.0.0.0")
        self.nomad_server_port: int = conf.getint(PROVIDER_NAME, "server_port", fallback=4646)
        self.secure: bool = conf.getboolean(PROVIDER_NAME, "secure", fallback=False)
        self.cert_path: str = conf.get(PROVIDER_NAME, "cert_path", fallback="")
        self.key_path: str = conf.get(PROVIDER_NAME, "key_path", fallback="")
        self.namespace: str = conf.get(PROVIDER_NAME, "namespace", fallback="")
        self.token: str = conf.get(PROVIDER_NAME, "token", fallback="")

        self.verify: bool | str
        verify = conf.get(PROVIDER_NAME, "verify", fallback="")
        if verify == "true":
            self.verify = True
        elif verify == "false":
            self.verify = False
        else:
            self.verify = verify

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
            verify=self.verify,  # type: ignore[reportArtumentType]
            namespace=self.namespace,
            token=self.token,
        )
        assert self.nomad, "Couldn't connect to Nomad"
        self.log.info("Nomad client initiated")

    @cached_property
    def nomad_url(self):
        protocol = "http" if not self.secure else "https"
        return f"{protocol}://{self.nomad_server_ip}:{self.nomad_server_port}"

    @classmethod
    def job_id_from_taskinstance_key(cls, key: TaskInstanceKey) -> str:
        dag_id, task_id, run_id, try_number, map_index = key
        return f"{dag_id}-{task_id}-{run_id}-{try_number}-{map_index}"

    @classmethod
    def job_task_id_from_taskinstance_key(cls, key: TaskInstanceKey) -> str:
        dag_id, task_id, _, _, _ = key
        return f"{dag_id}-{task_id}"

    def _get_job_template(self) -> NomadJobModel | None:
        if not (job_tpl_loc := conf.get("nomad", "default_job_template", fallback="")):
            return None

        job_tpl_path = Path(job_tpl_loc)
        if not job_tpl_path.is_file():
            self.log.error("Configured template %s is not a file", job_tpl_path)
            return None

        job_template = None
        try:
            if job_tpl_path.suffix == ".json":
                job_template = parse_json_job_template(job_tpl_path)
            elif job_tpl_path.suffix == ".hcl":
                job_template = parse_hcl_job_template(self.nomad_url, job_tpl_path)
        except (NomadValidationError, IOError) as err:
            self.log.error("Couldn't parse job template %s (%s)", job_tpl_path, err)

        if job_template:
            return job_template
        return None

    def apply_command_to_job_template(
        self, key: TaskInstanceKey, command: list[str]
    ) -> dict[str, Any]:
        """Apply command to the Nomad job template"""
        job_id = self.job_id_from_taskinstance_key(key)
        job_task_id = self.job_task_id_from_taskinstance_key(key)

        job_template = None
        if job_model := self._get_job_template():
            job_template = job_model.model_dump(exclude_unset=True)
            job_template["Job"]["TaskGroups"][0]["Tasks"][0]["Config"]["args"] = [
                "python",
                "-m",
                "airflow.sdk.execution_time.execute_workload",
                "--json-string",
            ] + command

        if not job_template:
            job_template = copy.deepcopy(default_task_template)
            job_template = copy.deepcopy(default_task_template)
            job_template["Job"]["TaskGroups"][0]["Tasks"][0]["Config"]["args"] = command
            job_template["Job"]["Name"] = f"airflow-run-{job_task_id}-{key[3]}"

        job_template["Job"]["TaskGroups"][0]["Tasks"][0]["Name"] = job_task_id
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
        try:
            self.nomad.job.register_job(job_id, job_template)
            self.nomad_jobs[key] = (job_id, job_task_id, "")
            self.log.debug(f"Runing task ({job_id}) with template {job_template})")
        except BaseNomadException as err:
            self.log.error("Couldn't run task %s (%s)", key, err)

    def _get_task_log(
        self, ti: TaskInstance, try_number: int, stderr=False
    ) -> tuple[list[str], list[str]]:
        messages = []
        log = []
        logtype = "error" if stderr else "standard"
        try:
            messages.append(
                f"Attempting to fetch {logtype} logs for task {ti.key} through Nomad API (attempts: {try_number})"
            )
            messages_received, logs_received = self.retrieve_logs(ti.key, stderr=stderr)
            messages += messages_received

            for line in logs_received:
                log.append(remove_escape_codes(line))
            if log:
                messages.append(f"Found {logtype} logs for running job via Nomad API")
        except Exception as e:
            messages.append(f"Reading {logtype} logs failed: {e}")
        return messages, log

    def get_task_log(self, ti: TaskInstance, try_number: int) -> tuple[list[str], list[str]]:
        messages, logs = self._get_task_log(ti, try_number)
        if (
            conf.get("logging", "task_log_reader", fallback=FileTaskHandler.name)
            != NomadLogHandler.name
        ):
            messages_err, logs_err = self._get_task_log(ti, try_number, stderr=True)
            if logs_err:
                logs = logs + logs_err
                messages = messages + messages_err
        return messages, logs

    def get_task_stderr(self, ti: TaskInstance, try_number: int) -> tuple[list[str], list[str]]:
        return self._get_task_log(ti, try_number, stderr=True)

    def retrieve_logs(self, key: TaskInstanceKey, stderr=False) -> tuple[list[str], list[str]]:
        # Note: this method is executed by the FileTaskHandler and not the Scheduler
        # We have no access to the running NomadExecutor's state
        if not self.nomad:
            self.start()

        assert self.nomad, "Couldn't initialize Nomad client"

        messages = []
        logs = ""
        ""
        job_id = self.job_id_from_taskinstance_key(key)
        job_task_id = self.job_task_id_from_taskinstance_key(key)
        allocations = self.nomad.job.get_allocations(job_id)
        if not isinstance(allocations, list):
            messages.append("Unexpected result from Nomad API allocations query")
        elif len(allocations) == 0:
            messages.append(f"No allocations found for {job_id}/{job_task_id}")
        elif len(allocations) > 1:
            messages.append(
                f"Multiple allocations found found for {job_id}/{job_task_id}: {allocations}"
            )
        else:
            allocation_id = allocations[0].get("ID")
            if not allocation_id:
                messages.append(f"Allocation for {job_id}/{job_task_id} not found")
                return (messages, [])

            if stderr:
                logs = self.nomad.client.cat.read_file(
                    allocation_id, path=f"alloc/logs/{job_task_id}.stderr.0"
                )
            else:
                logs = self.nomad.client.cat.read_file(
                    allocation_id, path=f"alloc/logs/{job_task_id}.stdout.0"
                )
        return messages, logs.splitlines()  # type: ignore[reportReturnType]

    @staticmethod
    def get_cli_commands() -> list[GroupCommand]:
        return [
            GroupCommand(
                name=PROVIDER_NAME,
                help=f"Tools to help run the {PROVIDER_NAME} executor",
                subcommands=NOMAD_COMMANDS,
            )
        ]


def _get_parser() -> argparse.ArgumentParser:  # pragma: no-cover
    """
    Generate documentation; used by Sphinx.

    :meta private:
    """
    return NomadExecutor._get_parser()
