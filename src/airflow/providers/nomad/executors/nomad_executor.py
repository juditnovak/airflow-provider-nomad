# This file is part of apache-airflow-providers-nomad which is
# released under Apache License 2.0. See file LICENSE or go to
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# for full license details.
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
from pathlib import Path
from typing import Any

from airflow.configuration import conf
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import remove_escape_codes
from airflow.utils.state import TaskInstanceState
from pydantic import ValidationError

from airflow.providers.nomad.constants import CONFIG_SECTION
from airflow.providers.nomad.exceptions import NomadProviderException, NomadValidationError
from airflow.providers.nomad.generic_interfaces.executor_interface import ExecutorInterface
from airflow.providers.nomad.log import NomadLogHandler
from airflow.providers.nomad.manager import NomadManager
from airflow.providers.nomad.models import NomadJobModel
from airflow.providers.nomad.templates.job_template import default_task_template
from airflow.providers.nomad.utils import (
    job_id_from_taskinstance_key,
    job_task_id_from_taskinstance_key,
)

logger = logging.getLogger(__name__)


class NomadExecutor(ExecutorInterface):
    """Executor for Nomad."""

    RUNNING_POD_LOG_LINES = 100
    supports_ad_hoc_ti_run: bool = True

    def __init__(self):
        self.parallelism: int = conf.getint(CONFIG_SECTION, "parallelism", fallback=128)
        self.nomad_mgr = NomadManager()
        super().__init__(parallelism=self.parallelism)

    def start(self) -> None:
        """Start the executor."""
        self.log.info("Starting Nomad executor")
        self.nomad_mgr.initialize()

    def _get_job_template(self) -> NomadJobModel | None:
        if not (job_tpl_loc := conf.get(CONFIG_SECTION, "default_job_template", fallback="")):
            return None

        job_tpl_path = Path(job_tpl_loc)
        if not job_tpl_path.is_file():
            self.log.error("Configured template %s is not a file", job_tpl_path)
            return None

        job_template = None

        try:
            with open(job_tpl_path) as file:
                content = file.read()
        except (IOError, OSError) as err:
            self.log.error("Couldn't open job template file %s (%s)", job_tpl_path, err)
            return  # type: ignore [return-value]

        try:
            if job_tpl_path.suffix == ".json":
                job_template = self.nomad_mgr.parse_template_json(content)
            elif job_tpl_path.suffix == ".hcl":
                job_template = self.nomad_mgr.parse_template_hcl(content)
        except NomadValidationError as err:
            self.log.error("Couldn't parse job template %s (%s)", job_tpl_path, err)

        return job_template

    def prepare_job_template(self, key: TaskInstanceKey, command: list[str]) -> dict[str, Any]:
        """Adjutst template to suit upcoming job execution

        :param key: reference to the task instance in question
        :return: job template as as dictionary
        """
        job_id = job_id_from_taskinstance_key(key)
        job_task_id = job_task_id_from_taskinstance_key(key)

        if job_model := self._get_job_template():
            job_model.Job.TaskGroups[0].Tasks[0].Config.args = [
                "python",
                "-m",
                "airflow.sdk.execution_time.execute_workload",
                "--json-string",
            ] + command

        if not job_model:
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
        return job_model.model_dump(exclude_unset=True)

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

        return self.nomad_mgr.register_job(job_model)

    def remove_job_if_hanging(self, key: TaskInstanceKey) -> tuple[TaskInstanceState, str] | None:
        """Whether the job failed outside of the Airflow context

        :param key: reference to the task instance in question
        :return: either a tuple of: (task status to set (typically: FAILED), additional info)
                 or None if no data could be retrieved for the job
        """
        job_id = job_id_from_taskinstance_key(key)
        if not (outcome := self.nomad_mgr.remove_job_if_hanging(job_id)):
            return None

        killed, error_msg = outcome
        if killed:
            return TaskInstanceState.FAILED, error_msg
        return None

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
            job_id = job_id_from_taskinstance_key(ti.key)
            if job_info := self.nomad_mgr.job_all_info_str(job_id):
                messages.append("Nomad job evaluations retrieved")
                log.append(
                    "No task logs found, but the following information was retrieved from Nomad:"
                )
                log += job_info

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
        if not self.nomad_mgr.nomad:
            self.start()

        messages = []
        logs = ""
        job_id = job_id_from_taskinstance_key(key)
        allocations = self.nomad_mgr.get_nomad_job_allocation(job_id)

        if not isinstance(allocations, list):
            messages.append("Unexpected result from Nomad API allocations query")
        elif len(allocations) == 0:
            messages.append(f"No allocations found for {job_id}")
        else:
            for allocation in allocations:
                if len(allocations) > 1:
                    logs += f"\nAllocation ID {allocation.ID}:\n"

                # Normally this is single element, as the executor insists on a single task
                for task_name in allocation.TaskStates:
                    if len(allocation.TaskStates) > 1:
                        logs += f"\nTask Name {task_name}:\n"

                    if stderr:
                        logs += self.nomad_mgr.get_job_stderr(allocation.ID, task_name)
                    else:
                        logs += self.nomad_mgr.get_job_stdout(allocation.ID, task_name)

        return messages, logs.splitlines()  # type: ignore[reportReturnType]


def _get_parser() -> argparse.ArgumentParser:  # pragma: no-cover
    """
    Generate documentation; used by Sphinx.

    :meta private:
    """
    return NomadExecutor._get_parser()
