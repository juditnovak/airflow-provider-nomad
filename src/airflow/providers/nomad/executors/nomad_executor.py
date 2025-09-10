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
import json
import logging
from datetime import datetime
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
from airflow.utils.state import TaskInstanceState
from nomad.api.exceptions import BaseNomadException  # type: ignore[import-untyped]
from nomad.api.exceptions import URLNotFoundNomadException  # type: ignore[import-untyped]
from pydantic import ValidationError

from airflow.providers.nomad.exceptions import NomadProviderException, NomadValidationError
from airflow.providers.nomad.generic_interfaces.executor_interface import ExecutorInterface
from airflow.providers.nomad.models import (
    JobEvalStatus,
    JobInfoStatus,
    NomadEvalList,
    NomadEvaluation,
    NomadJobAllocations,
    NomadJobAllocList,
    NomadJobModel,
    NomadJobSubmission,
    NomadJobSummary,
)
from airflow.providers.nomad.nomad_log import NomadLogHandler
from airflow.providers.nomad.templates.nomad_job_template import default_task_template
from airflow.providers.nomad.utils import (
    dict_to_lines,
    parse_hcl_job_template,
    parse_json_job_template,
)

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
        super().__init__()
        self.parallelism: int = conf.getint(self.EXECUTOR_NAME, "parallelism", fallback=1)
        self.nomad_server_ip: str = conf.get(self.EXECUTOR_NAME, "server_ip", fallback="0.0.0.0")
        self.nomad_server_port: int = conf.getint(self.EXECUTOR_NAME, "server_port", fallback=4646)
        self.secure: bool = conf.getboolean(self.EXECUTOR_NAME, "secure", fallback=False)
        self.cert_path: str = conf.get(self.EXECUTOR_NAME, "cert_path", fallback="")
        self.key_path: str = conf.get(self.EXECUTOR_NAME, "key_path", fallback="")
        self.namespace: str = conf.get(self.EXECUTOR_NAME, "namespace", fallback="")
        self.token: str = conf.get(self.EXECUTOR_NAME, "token", fallback="")

        self.alloc_pending_timeout: int = conf.getint(
            self.EXECUTOR_NAME, "alloc_pending_timeout", fallback=600
        )
        self.pending_jobs: dict[TaskInstanceKey, int] = {}

        self.verify: bool | str
        verify = conf.get(self.EXECUTOR_NAME, "verify", fallback="")
        if verify == "true":
            self.verify = True
        elif verify == "false":
            self.verify = False
        else:
            self.verify = verify

        self.nomad: nomad.Nomad | None = None

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
        if self.nomad:
            self.log.info("Nomad client initiated")
        else:
            self.log.error("Can't initialize nomad client")

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
        if not (job_tpl_loc := conf.get(self.EXECUTOR_NAME, "default_job_template", fallback="")):
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
                job_template = parse_hcl_job_template(
                    self.nomad_url,
                    job_tpl_path,
                    verify=self.verify,
                    cert=(self.cert_path, self.key_path),
                )
        except (NomadValidationError, IOError) as err:
            self.log.error("Couldn't parse job template %s (%s)", job_tpl_path, err)

        if job_template:
            return job_template
        return None

    def prepare_job_template(self, key: TaskInstanceKey, command: list[str]) -> dict[str, Any]:
        """Adjutst template to suit upcoming job execution

        :param key: reference to the task instance in question
        :return: job template as as dictionary
        """
        job_id = self.job_id_from_taskinstance_key(key)
        job_task_id = self.job_task_id_from_taskinstance_key(key)

        job_template = None
        if job_model := self._get_job_template():
            # job_template = job_model.model_dump(exclude_unset=True)
            job_model.Job.TaskGroups[0].Tasks[0].Config.args = [
                "python",
                "-m",
                "airflow.sdk.execution_time.execute_workload",
                "--json-string",
            ] + command

        if not job_template:
            job_template = copy.deepcopy(default_task_template)
            job_model = NomadJobModel.model_validate(job_template)
            job_model.Job.TaskGroups[0].Tasks[0].Config.args = command
            job_model.Job.Name = f"airflow-run-{job_task_id}-{key[3]}"

        if not job_model:
            raise NomadProviderException("Couldn't retrieve job template")

        job_model.Job.TaskGroups[0].Tasks[0].Name = job_task_id
        job_model.Job.ID = job_id

        self.log.debug(
            f"Command running: python -m airflow.sdk.execution_time.execute_workload --json-string '{command[0]}'\n"
        )
        return job_model.model_dump()

    def run_job(self, job_template: dict[str, Any] | None) -> str | None:
        """Execute the job defined by a potential job template

        :param: Job template corresponding to the job
        :return: No news is good news, or the error that occured on execution attempt
        """
        if not job_template:
            raise NomadProviderException("Job template missing")

        try:
            job_model = NomadJobModel.model_validate(job_template)
        except ValidationError:
            raise NomadProviderException("Job template doesn't comply to expected format ({err})")

        try:
            self.nomad.job.register_job(job_model.Job.ID, job_template)  # type: ignore[reportOptionalMemberAccess, union-attr]

            self.log.debug(
                "Runing task (%s) with template %s)", job_model.Job.ID, str(job_template)
            )
        except BaseNomadException as err:
            self.log.error("Couldn't run task %s (%s)", job_model.Job.ID, err)
            try:
                self.nomad.job.deregister_job(job_model.Job.ID)  # type: ignore[reportOptionalMemberAccess, union-attr]
            except BaseNomadException:
                pass
            return str(err)
        return None

    def get_nomad_evaluations_info(self, job_id: str) -> NomadEvalList | None:  # type: ignore[return]
        job_eval = self.nomad.job.get_evaluations(job_id)  # type: ignore[reportOptionalMemberAccess, union-attr]
        try:
            return NomadEvaluation.validate_python(job_eval)
        except ValidationError as err:
            self.log.error("Couldn't parse Nomad job validation output: %s %s", err, err.errors())

    def get_nomad_job_submission_info(self, job_id: str) -> NomadJobSubmission | None:  # type: ignore[return]
        job_status = self.nomad.job.get_job(job_id)  # type: ignore[reportOptionalMemberAccess, union-attr]
        try:
            return NomadJobSubmission.model_validate(job_status)
        except ValidationError as err:
            self.log.error("Couldn't parse Nomad job submission info: %s %s", err, err.errors())

    def get_nomad_job_allocation_info(self, job_id: str) -> NomadJobAllocList | None:  # type: ignore[return]
        job_allocations = self.nomad.job.get_allocations(job_id)  # type: ignore[reportOptionalMemberAccess, union-attr]
        try:
            return NomadJobAllocations.validate_python(job_allocations)
        except ValidationError as err:
            self.log.error("Couldn't parse Nomad job allocations info: %s %s", err, err.errors())

    def get_nomad_job_summary(self, job_id: str) -> NomadJobSummary | None:  # type: ignore[return]
        job_summary = self.nomad.job.get_summary(job_id)  # type: ignore[reportOptionalMemberAccess, union-attr]
        try:
            return NomadJobSummary.model_validate(job_summary)
        except ValidationError as err:
            self.log.error("Couldn't parse Nomad job summary: %s %s", err, err.errors())

    def timeout_expired(self, key: TaskInstanceKey) -> bool:
        now = int(datetime.now().timestamp())
        if not self.pending_jobs.get(key):
            self.pending_jobs[key] = now
            return False
        else:
            return now - self.pending_jobs[key] > self.alloc_pending_timeout

    def is_job_dead(
        self, key: TaskInstanceKey
    ) -> tuple[bool, TaskInstanceState | None, str] | None:
        """Whether the job failed on Nomad side

        Typically on allocaton errors there is not feedback to Airflow, as the
        job remains in 'pending' state on Nomad side. Such issues have to be detected
        and the job execution is to be reported as failed.

        NOTE: The executor failing a job run is considered as an ERROR by Airflow.
        Despite the log message, this is the efficient way for this case. Potential Airflow-level
        task re-tries are applied corectly.

        :param key: reference to the task instance in question
        :return: either a tuple of: True/False, potential task status to set (typically: FAILED), additional info
                 or None if no data could be retrieved for the job
        """
        job_id = self.job_id_from_taskinstance_key(key)
        try:
            job_status = self.get_nomad_job_submission_info(job_id)
        except URLNotFoundNomadException:
            self.log.error("Summary retrieval error: job %s not found", job_id)
            return None
        except BaseNomadException as err:
            self.log.error("Couldn't get job information %s", str(err))
            return None

        if not job_status:
            return None

        # Allocation failures: job is stuck in a 'pending' state
        if job_status.Status == JobEvalStatus.pending:
            if not self.timeout_expired(key):
                return False, None, ""

            if not (job_eval := self.get_nomad_evaluations_info(job_id)):
                return None

            item = None
            for item in job_eval:
                if (
                    item.Status == JobEvalStatus.complete
                    and item.TriggeredBy == "job-register"
                    and item.FailedTGAllocs
                ):
                    break

            taskgroup_name = job_status.TaskGroups[0].Name
            if item and (failed_alloc := item.FailedTGAllocs.get(taskgroup_name)):  # type: ignore[reportOperationalMemberAccess]
                self.log.info("Task %s was pending beyond timeout, stopping it.", key)
                self.nomad.job.deregister_job(job_id)  # type: ignore[reportOptionalMemberAccess, union-attr]
                error = failed_alloc.errors()
                return True, TaskInstanceState.FAILED, str(error)
        else:
            self.pending_jobs.pop(key, None)

        # Failures during job setup: job is stuck in a 'dead' state
        if job_status.Status == JobInfoStatus.dead:
            job_alloc_info = self.get_nomad_job_allocation_info(job_id)
            job_summary = self.get_nomad_job_summary(job_id)
            if job_summary and job_summary.all_failed():
                self.log.info("Task %s seems dead, stopping it.", key)
                self.nomad.job.deregister_job(job_id)  # type: ignore[reportOptionalMemberAccess, union-attr]
                if job_alloc_info:
                    errors = [alloc.errors() for alloc in job_alloc_info]
                    return True, TaskInstanceState.FAILED, str(errors)
                return True, TaskInstanceState.FAILED, ""

        return False, None, ""

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

        # In case the task didn't even make it to be submitted, we may be able to get info about reasons from Nomad
        if not log and not stderr:
            job_id = self.job_id_from_taskinstance_key(ti.key)
            job_eval = self.get_nomad_evaluations_info(job_id)
            job_alloc_info = self.get_nomad_job_allocation_info(job_id)
            job_summary = self.get_nomad_job_summary(job_id)
            if job_eval or job_alloc_info or job_summary:
                messages.append("Nomad job evaluations retrieved")
                log.append(
                    "No task logs found, but the following information was retrieved from Nomad:"
                )
            if job_eval:
                log.append("Job evaluations:")
                log += dict_to_lines(json.loads(NomadEvaluation.dump_json(job_eval)))
            if job_alloc_info:
                log.append("Job allocations info:")
                log += dict_to_lines(json.loads(NomadJobAllocations.dump_json(job_alloc_info)))
            if job_summary:
                log.append("Job summary:")
                log += dict_to_lines(json.loads(NomadJobSummary.model_dump_json(job_summary)))
        return messages, log

    def get_task_log(self, ti: TaskInstance, try_number: int) -> tuple[list[str], list[str]]:
        """Universal way to retreive logs

        Retrieving logs so that it's compatible both with FileTaskHandler, and
        leaves space for NomadLogHandler grouping feature.

        :params ti: TaskInstance in question
        :return: a list of operational messages together with the logs (as a list of entries)
        """
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

        messages = []
        logs = ""
        job_id = self.job_id_from_taskinstance_key(key)
        job_task_id = self.job_task_id_from_taskinstance_key(key)
        allocations = self.nomad.job.get_allocations(job_id)  # type: ignore[reportOptionalMemberAccess, union-attr]

        if not isinstance(allocations, list):
            messages.append("Unexpected result from Nomad API allocations query")
        elif len(allocations) == 0:
            messages.append(f"No allocations found for {job_id}/{job_task_id}")
        else:
            for allocation in allocations:
                allocation_id = allocation.get("ID")
                if not allocation_id:
                    messages.append(f"Allocation for {job_id}/{job_task_id} not found")
                    return (messages, [])
                elif len(allocations) > 1:
                    logs += f"\nAllocation ID {allocation_id}:\n"

                if stderr:
                    logs += self.nomad.client.cat.read_file(  # type: ignore[reportOptionalMemberAccess, union-attr]
                        allocation_id, path=f"alloc/logs/{job_task_id}.stderr.0"
                    )
                else:
                    logs += self.nomad.client.cat.read_file(  # type: ignore[reportOptionalMemberAccess, union-attr]
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
