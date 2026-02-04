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
Executor Interface.
"""

from __future__ import annotations

import argparse
import contextlib
import multiprocessing
from collections.abc import Sequence
from datetime import datetime, timedelta
from queue import Empty, Queue
from typing import Any

import pytz  # type: ignore[import-untyped]
from airflow.configuration import conf
from airflow.executors import workloads
from airflow.executors.base_executor import BaseExecutor
from airflow.executors.workloads import All, ExecuteTask
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.standard.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils.state import TaskInstanceState, TerminalTIState
from sqlalchemy.orm import Session  # type: ignore[import-untyped]

# TaskInstanceKey, Workload in a list, ExecutorConfig
Job = tuple[TaskInstanceKey, list[Any], Any]

RESPONSE_DELAY = 60


class ExecutorInterface(BaseExecutor):
    """Executor with run-queues."""

    supports_ad_hoc_ti_run: bool = True
    serve_logs = True

    def __init__(self, parallelism: int = 1):
        if not AIRFLOW_V_3_0_PLUS:
            raise RuntimeError("Nomad executor only available for Airflow > 3.0.0")

        self._manager = multiprocessing.Manager()
        self.task_queue: Queue[Job] = self._manager.Queue()
        self.parallelism = parallelism
        self.task_hb_timeout = conf.getint(
            "scheduler", "task_instance_heartbeat_timeout", fallback=60
        )
        super().__init__(parallelism=self.parallelism)

    def is_exec_task(self, cmd: Any) -> bool:
        """Safety method in case running in a legacy environment"""
        if isinstance(cmd, ExecuteTask):
            return True
        self.log.error("Workload of unsupported type: '%s'", cmd)
        return False

    def queue_workload(self, workload: All, session: Session) -> None:
        self.log.info("Queueing task: '%s'", workload)
        if not self.is_exec_task(workload):
            return
        ti = workload.ti
        self.queued_tasks[ti.key] = workload  # type: ignore
        self.log.info("Queued task: '%s'", workload)

    def _process_workloads(self, workloads: Sequence[workloads.All]) -> None:
        for w in workloads:
            self.log.info("Processing workload task: '%s'", w)
            if not self.is_exec_task(w) or not w.ti:
                return

            # TODO: AIP-72 handle populating tokens once https://github.com/apache/airflow/issues/45107 is handled.
            command = [w]
            key = w.ti.key
            queue = w.ti.queue
            executor_config = w.ti.executor_config or {}

            del self.queued_tasks[key]
            self.execute_async(
                key=key, command=command, queue=queue, executor_config=executor_config
            )
            self.running.add(key)

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: Any,
        queue: str | None = None,
        executor_config: Any | None = None,
    ) -> None:
        """Execute task asynchronously."""
        self.log.info("Adding task %s with command %s to run queue", key, command)
        self.event_buffer[key] = (TaskInstanceState.QUEUED, None)
        self.task_queue.put((key, command, executor_config))

    def set_state(self, key: TaskInstanceKey, state: TaskInstanceState, info: str = "") -> None:
        self.log.info("Setting state for key: %s state: %s info: %s", key, state, info)

        if state not in TaskInstanceState:
            self.log.error("Unknown task state %s, setting it as failed", state)
            self.fail(key)
            return

        if state == TaskInstanceState.QUEUED:
            self.queued(key)
        elif state == TaskInstanceState.SUCCESS:
            self.success(key, info=info)
        elif state == TaskInstanceState.FAILED:
            self.fail(key, info=info)
        elif state not in [
            TaskInstanceState.RUNNING,
            TaskInstanceState.RESTARTING,
        ]:
            self.change_state(key, state, remove_running=True, info=info)  # type: ignore[reportArgumentType]

    def sync(self) -> None:
        """Synchronize task state.

        IMPORTANT: We must avoid at all cost that an exception may be raised, as
        it results in the complete termination of the Executor
        """
        self.log.info("self.running: %s", self.running)
        self.log.info("self.queued: %s", list(self.queued_tasks))

        for ti_key in list(self.running | set(self.queued_tasks)):
            # Remove finished tasks from the run queue
            # NOTE: Since tasks are changing state based on reporting results to the API,
            # the executor is not aware
            dag_id, task_id, run_id, _, map_index = ti_key

            if ti := TaskInstance.get_task_instance(dag_id, run_id, task_id, map_index):
                if ti.state in TerminalTIState:
                    try:
                        self.running.remove(ti_key)
                    except KeyError:
                        pass
                    self.queued_tasks.pop(ti_key, None)
            else:
                self.log.error("Task instance %s can't be found", ti_key)

            # Security measure: If the job is stale, we check if it may be finished/hanging
            # Options:
            # 1) The job may have finished, but couldn't report to the API for some reason
            # 2) The job got hanging
            now = datetime.now(pytz.timezone("UTC"))
            if not ti or ti_key in self.running:
                try:
                    # Skip jobs that may still report a valid status with delay
                    if (
                        ti
                        and ti.last_heartbeat_at
                        and ti.last_heartbeat_at
                        > now - timedelta(seconds=(3 * self.task_hb_timeout))
                    ):
                        continue
                except Exception:
                    pass

                if (status := self.is_job_done(ti_key)) or (
                    status := self.remove_job_if_hanging(ti_key)
                ):
                    state, info = status
                    self.set_state(ti_key, state=state, info=info)

        # Run jobs from the task_queue
        with contextlib.suppress(Empty):
            while self.slots_available > 0 and (task := self.task_queue.get_nowait()):
                key = None
                try:
                    key, _, _ = task
                    self.run_task(task)
                except Exception as err:
                    self.log.exception("Failed to run task %s (%s)", task, str(err))
                    if key:
                        self.set_state(
                            key, state=TaskInstanceState.FAILED, info="Couldn't run task"
                        )
                finally:
                    self.task_queue.task_done()

    def workload_to_command_args(self, workload: ExecuteTask) -> list[str]:
        """Convert a workload object to Task SDK command arguments."""
        return [workload.model_dump_json()]

    def _not_exec_task(self, cmd: Any) -> str | None:
        """Safety method in case running in a legacy environment"""
        if (isinstance(cmd, list) and len(cmd) > 0) and isinstance(cmd[0], ExecuteTask):
            return None
        msg = f"Workload of unsupported type: {cmd}"
        self.log.error(msg)
        return msg

    def run_task(self, task: Job) -> None:
        """Run the next task in the queue."""
        key, cmd, exec_cfg = task
        dag_id, task_id, run_id, try_number, _ = key
        if not (isinstance(cmd, list) and len(cmd) > 0) or not self.is_exec_task(cmd[0]):
            self.fail(key, info=f"Unsupported workload '{cmd}'")
            return

        command = self.workload_to_command_args(cmd[0])

        self.log.info(
            "Runing task (%s) from dag (%s) with run ID (%s) (retries %s)",
            task_id,
            dag_id,
            run_id,
            try_number,
        )

        job_template = self.prepare_job_template(key, command, exec_cfg)
        try:
            failed_info = self.run_job(job_template)
        except Exception as err:
            failed_info = f"Couldn't run job {key}: {err}"

        if failed_info:
            self.log.error(failed_info)
            self.fail(key, info=failed_info)

    def prepare_job_template(
        self, key: TaskInstanceKey, command: list[str], executor_config: dict
    ) -> dict[str, Any]:
        """Adjust template to suit upcoming job execution

        :param key: reference to the task instance in question
        :return: job template as as dictionary
        """
        self.log.debug(
            f"Executing key {key} with command {command} and dynamic config {executor_config}"
        )
        raise NotImplementedError

    def run_job(self, job_template: dict[str, Any] | None) -> str | None:
        """Execute the job defined by a potential job template

        :param: Job template corresponding to the job
        :return: No news is good news, or the error that occurred on execution attempt
        """
        self.log.debug(f"Executing template {job_template}")
        raise NotImplementedError

    def is_job_done(self, key: TaskInstanceKey) -> tuple[TaskInstanceState, str] | None:
        """Return job status if the jobs is not running anymore

        :param key: reference to the task instance in question
        :return: either a tuple of: (task status to set (typically: FAILED), additional info)
                 or None if no data could be retrieved for the job
        """
        self.log.debug(f"Checking if task is done {key}")
        raise NotImplementedError

    def remove_job_if_hanging(self, key: TaskInstanceKey) -> tuple[TaskInstanceState, str] | None:
        """Whether the job failed (typically outside of Airflow execution)

        :param key: reference to the task instance in question
        :return: either a tuple of: True/False, potential task status to set (typically: FAILED), additional info
                 or None if no data could be retrieved for the job
        """
        self.log.debug(f"Evaluating status of key {key}")
        raise NotImplementedError

    def get_task_log(self, ti: TaskInstance, try_number: int) -> tuple[list[str], list[str]]:
        self.log.debug(f"Retrieving logs for {ti.key} with {try_number}")
        raise NotImplementedError

    def end(self) -> None:
        """Shut down the executor."""
        self.log.info("Shutting down Nomad executor")
        try:
            # self.log.debug("Flushing task_queue...")
            # self._flush_task_queue()
            # Both queues should be empty...
            self.task_queue.join()
        except ConnectionResetError:
            self.log.exception("Connection Reset error while flushing task_queue.")
        except Exception:
            self.log.exception("Unknown error while flushing task queue and result queue.")
        self._manager.shutdown()


def _get_parser() -> argparse.ArgumentParser:
    """
    Generate documentation; used by Sphinx.

    :meta private:
    """
    return ExecutorInterface._get_parser()
