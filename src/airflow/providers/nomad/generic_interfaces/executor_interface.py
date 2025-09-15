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
# under the License
"""
Executor Interface.
"""

from __future__ import annotations

import argparse
import contextlib
import multiprocessing
from collections.abc import Sequence
from queue import Empty, Queue
from typing import Any

from airflow.executors import workloads
from airflow.executors.base_executor import BaseExecutor
from airflow.executors.workloads import All, ExecuteTask
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.standard.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils.state import TaskInstanceState
from sqlalchemy.orm import Session  # type: ignore[import-untyped]

Job = tuple[TaskInstanceKey, Any, Any]

Results = tuple[TaskInstanceKey, TaskInstanceState | str | None]


class ExecutorInterface(BaseExecutor):
    """Executor with run-queues."""

    RUNNING_POD_LOG_LINES = 100
    supports_ad_hoc_ti_run: bool = True

    serve_logs = True

    def __init__(self, parallelism: int = 1):
        if not AIRFLOW_V_3_0_PLUS:
            raise RuntimeError("Nomad executor only available for Airflow > 3.0.0")

        self._manager = multiprocessing.Manager()
        self.task_queue: Queue[Job] = self._manager.Queue()
        self.parallelism = parallelism
        super().__init__(parallelism=self.parallelism)

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
        """Synchronize task state."""
        if self.running:
            self.log.debug("self.running: %s", self.running)
        if self.queued_tasks:
            self.log.debug("self.queued: %s", self.queued_tasks)

        with contextlib.suppress(Empty):
            for ti_key in list(self.running | set(self.queued_tasks)):
                if status := self.remove_job_if_hanging(ti_key):
                    state, info = status
                    self.set_state(ti_key, state=state, info=info)

        with contextlib.suppress(Empty):
            task = self.task_queue.get_nowait()

            try:
                key, command, _ = task
            except Exception as err:
                self.log.exception(
                    "Failed to run task (neither task nor command could be retrieved (%s))",
                    str(err),
                )
                raise

            try:
                self.run_task(task)
            except Exception as err:
                self.log.exception(
                    "Failed to run task %s with command %s (%s)", key, command, str(err)
                )
            finally:
                # TODO
                self.task_queue.task_done()

    def run_task(self, task: Job) -> None:
        """Run the next task in the queue."""
        key, instruction, _ = task
        dag_id, task_id, run_id, try_number, _ = key
        if not isinstance(instruction[0], ExecuteTask):
            raise ValueError(f"Workload of unsupported type: {type(instruction[0])}")

        command = self.workload_to_command_args(instruction[0])

        self.log.info(
            f"Runing task ({task_id}) from dag ({dag_id}) with run ID ({run_id}) (retries {try_number})"
        )
        job_template = self.prepare_job_template(key, command)
        if failed_info := self.run_job(job_template):
            self.log.error("Received info %s, failing job", failed_info)
            self.fail(key, info=failed_info)

    def workload_to_command_args(self, workload: ExecuteTask) -> list[str]:
        """Convert a workload object to Task SDK command arguments."""
        return [workload.model_dump_json()]

    def prepare_job_template(self, key: TaskInstanceKey, command: list[str]) -> dict[str, Any]:
        """Adjutst template to suit upcoming job execution

        :param key: reference to the task instance in question
        :return: job template as as dictionary
        """
        self.log.debug(f"Executing key {key} with command {command}")
        raise NotImplementedError

    def run_job(self, job_template: dict[str, Any] | None) -> str | None:
        """Execute the job defined by a potential job template

        :param: Job template corresponding to the job
        :return: No news is good news, or the error that occured on execution attempt
        """
        self.log.debug(f"Executing template {job_template}")
        raise NotImplementedError

    def remove_job_if_hanging(self, key: TaskInstanceKey) -> tuple[TaskInstanceState, str] | None:
        """Whether the job failed (typically outside of Airflow execution)

        :param key: reference to the task instance in question
        :return: either a tuple of: True/False, potential task status to set (typically: FAILED), additional info
                 or None if no data could be retrieved for the job
        """
        self.log.debug(f"Evaluating status of key {key}")
        raise NotImplementedError

    def queue_workload(self, workload: All, session: Session) -> None:
        if not isinstance(workload, ExecuteTask):
            raise ValueError(
                f"Un-handled workload kind {type(workload).__name__!r} in {type(self).__name__}"
            )
        ti = workload.ti
        self.queued_tasks[ti.key] = workload  # type: ignore

    def _process_workloads(self, workloads: Sequence[workloads.All]) -> None:
        from airflow.executors.workloads import ExecuteTask

        for w in workloads:
            if not isinstance(w, ExecuteTask):
                raise RuntimeError(f"{type(self)} cannot handle workloads of type {type(w)}")

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
