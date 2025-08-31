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
import logging
import multiprocessing
import time
from collections import Counter
from collections.abc import Sequence
from queue import Empty, Queue
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.executors import workloads
from airflow.executors.base_executor import BaseExecutor
from airflow.executors.workloads import All, ExecuteTask
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.standard.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils.log.logging_mixin import remove_escape_codes
from airflow.utils.state import TaskInstanceState
from sqlalchemy.orm import Session  # type: ignore[import-untyped]

Job = tuple[TaskInstanceKey, Any, Any]

Results = tuple[TaskInstanceKey, TaskInstanceState | str | None, str, str, str]


class ExecutorInterface(BaseExecutor):
    """Executor with run-queues."""

    RUNNING_POD_LOG_LINES = 100
    supports_ad_hoc_ti_run: bool = True
    EXECUTOR_NAME = "generic_executor"

    serve_logs = True

    def __init__(self, parallelism: int = 1):
        if not AIRFLOW_V_3_0_PLUS:
            raise RuntimeError("Nomad executor only available for Airflow > 3.0.0")

        self._manager = multiprocessing.Manager()
        self.task_queue: Queue[Job] = self._manager.Queue()
        self.result_queue: Queue[Results] = self._manager.Queue()
        self.queued_tasks_exec: dict[TaskInstanceKey, ExecuteTask] = {}

        self.last_handled: dict[TaskInstanceKey, float] = {}
        self.task_publish_retries: Counter[TaskInstanceKey] = Counter()
        self.task_publish_max_retries = conf.getint(
            self.EXECUTOR_NAME, "task_publish_max_retries", fallback=0
        )
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
        assert self.task_queue

        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug(
                "Add task %s with command %s, executor_config %s",
                key,
                command,
                executor_config,
            )
        else:
            self.log.info("Add task %s with command %s", key, command)

        self.event_buffer[key] = (TaskInstanceState.QUEUED, None)
        self.task_queue.put((key, command, executor_config))
        # We keep a temporary local record that we've handled this so we don't
        # try and remove it from the QUEUED state while we process it
        self.last_handled[key] = time.time()

    def sync(self) -> None:
        """Synchronize task state."""
        assert self.result_queue
        assert self.task_queue

        if self.running:
            self.log.debug("self.running: %s", self.running)
        if self.queued_tasks:
            self.log.debug("self.queued: %s", self.queued_tasks)

        with contextlib.suppress(Empty):
            task = self.task_queue.get_nowait()

            try:
                key, command, _ = task
            except:
                self.log.exception(
                    "Failed to run task (neither task nor command could be retrieved)"
                )
                raise

            try:
                self.run_task(task)
                self.task_publish_retries.pop(key, None)
            except:
                self.log.exception(
                    "Failed to run task %s with command %s", key, command
                )
            finally:
                # TODO
                self.task_queue.task_done()

    def run_task(self, task: Job) -> None:
        """Run the next task in the queue."""
        key, command, _ = task
        dag_id, task_id, run_id, try_number, map_index = key
        if len(command) == 1:
            if isinstance(command[0], ExecuteTask):
                workload = command[0]
                command = self.workload_to_command_args(workload)
            else:
                raise ValueError(f"Workload of unsupported type: {type(command[0])}")
        elif command[0:3] != ["airflow", "tasks", "run"]:
            raise ValueError('The command must start with ["airflow", "tasks", "run"].')

        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug(
                f"Runing task ({task_id}) from dag ({dag_id}) with run ID ({run_id}) (retries {try_number}, map index ({map_index}))"
            )
        else:
            self.log.info(
                f"Runing task ({task_id}) from dag ({dag_id}) with run ID ({run_id}) (retries {try_number})"
            )
        job_template = self.apply_command_to_job_template(key, command)
        self.run_job(key, job_template)

    def workload_to_command_args(self, workload: ExecuteTask) -> list[str]:
        """Convert a workload object to Task SDK command arguments."""
        return [workload.model_dump_json()]

    def apply_command_to_job_template(
        self, key: TaskInstanceKey, command: list[str]
    ) -> dict[str, Any]:
        self.log.debug(f"Executing key {key} with command {command}")
        raise NotImplementedError

    def run_job(self, key: TaskInstanceKey, job_template: dict[str, Any]) -> None:
        self.log.debug(f"Executing key {key} template {job_template}")
        raise NotImplementedError

    def queue_workload(self, workload: All, session: Session) -> None:
        if not isinstance(workload, ExecuteTask):
            raise ValueError(
                f"Un-handled workload kind {type(workload).__name__!r} in {type(self).__name__}"
            )
        ti = workload.ti
        self.queued_tasks_exec[ti.key] = workload
        self.queued_tasks[ti.key] = workload  # type: ignore

    def _process_workloads(self, workloads: Sequence[workloads.All]) -> None:
        from airflow.executors.workloads import ExecuteTask

        for w in workloads:
            if not isinstance(w, ExecuteTask):
                raise RuntimeError(
                    f"{type(self)} cannot handle workloads of type {type(w)}"
                )

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

    def get_task_log(
        self, ti: TaskInstance, try_number: int
    ) -> tuple[list[str], list[str]]:
        messages = []
        log = []
        try:
            messages.append(
                f"Attempting to fetch logs from task {ti.key} through Nomad API (attempts: {try_number})"
            )
            messages_received, logs_received = self.retrieve_logs(ti.key)
            messages += messages_received

            for line in logs_received:
                log.append(remove_escape_codes(line))
            if log:
                messages.append("Found logs for running job via Nomad API")
        except Exception as e:
            messages.append(f"Reading logs failed: {e}")
        return messages, log

    def retrieve_logs(self, key: TaskInstanceKey) -> tuple[list[str], list[str]]:
        self.log.debug(f"Retrieving logs for {key}")
        raise NotImplementedError

    def end(self) -> None:
        """Shut down the executor."""
        if TYPE_CHECKING:
            assert self.task_queue
            assert self.result_queue

        self.log.info("Shutting down Nomad executor")
        try:
            # self.log.debug("Flushing task_queue...")
            # self._flush_task_queue()
            # self.log.debug("Flushing result_queue...")
            # self._flush_result_queue()
            # Both queues should be empty...
            self.task_queue.join()
            self.result_queue.join()
        except ConnectionResetError:
            self.log.exception(
                "Connection Reset error while flushing task_queue and result_queue."
            )
        except Exception:
            self.log.exception(
                "Unknown error while flushing task queue and result queue."
            )
        self._manager.shutdown()


def _get_parser() -> argparse.ArgumentParser:
    """
    Generate documentation; used by Sphinx.

    :meta private:
    """
    return ExecutorInterface._get_parser()
