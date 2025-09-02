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
"""Logging module to fetch logs via the Nomad API"""

import logging
from collections.abc import Callable
from itertools import chain

from airflow.executors.executor_loader import ExecutorLoader
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.utils.log.file_task_handler import (
    FileTaskHandler,
    LegacyProvidersLogType,
    LogHandlerOutputStream,
    LogMetadata,
    LogSourceInfo,
    StructuredLogMessage,
    StructuredLogStream,
)
from airflow.utils.state import TaskInstanceState

logger = logging.getLogger(__name__)


class ExecutorLogLinesHandler(FileTaskHandler):
    """Extended handler to retrieve logs directly from Nomad"""

    name = "executor_log_lines_handler"

    def __init__(self, base_log_folder, *args, **kwargs):
        super().__init__(base_log_folder, *args, **kwargs)

    def _get_executor_get_task_stderr(
        self, ti: TaskInstance
    ) -> Callable[[TaskInstance, int], tuple[list[str], list[str]]] | None:
        executor_name = ti.executor or self.DEFAULT_EXECUTOR_KEY
        executor = self.executor_instances.get(executor_name)
        if executor is not None and hasattr(executor, "get_task_stderr"):
            return getattr(executor, "get_task_stderr", None)

        if executor_name == self.DEFAULT_EXECUTOR_KEY:
            self.executor_instances[executor_name] = ExecutorLoader.get_default_executor()
        else:
            self.executor_instances[executor_name] = ExecutorLoader.load_executor(executor_name)
        return getattr(self.executor_instances[executor_name], "get_task_stderr", None)

    def _read(
        self,
        ti: TaskInstance | TaskInstanceHistory,
        try_number: int,
        metadata: LogMetadata | None = None,
    ) -> tuple[LogHandlerOutputStream | LegacyProvidersLogType, LogMetadata]:
        """
        Re-write of the FileTaskHandler read method to add simple log retrieval
        from a line-by-line streamable source, if not file
        """
        logger.info("Collecting Nomad logs")
        sources: LogSourceInfo = []
        err_sources: LogSourceInfo = []
        source_list: list[str] = []

        logs: list[str] = []
        stderr: list[str] = []
        # Stdout
        executor_get_task_log = self._get_executor_get_task_log(ti)
        response = executor_get_task_log(ti, try_number)
        if response:
            sources, logs = response
        if sources:
            source_list.extend(sources)

        # Stderr
        if executor_get_task_stderr := self._get_executor_get_task_stderr(ti):
            response = executor_get_task_stderr(ti, try_number)
            if response:
                err_sources, stderr = response
            if err_sources:
                source_list.extend(err_sources)

        # out_stream: LogHandlerOutputStream = _interleave_logs((y for y in logs))

        def geneate_log_stream(logs: list[str]) -> StructuredLogStream:
            for line in logs:
                if not line:
                    continue
                try:
                    yield StructuredLogMessage.model_validate_json(line)
                except:  # noqa
                    yield StructuredLogMessage(event=str(line))

        out_stream: LogHandlerOutputStream = geneate_log_stream(logs)
        err_stream: LogHandlerOutputStream = geneate_log_stream(stderr)

        # Same as for FileTaskHandler, add source details as a collapsible group
        footer = StructuredLogMessage(event="::endgroup::")
        header = [
            StructuredLogMessage(
                event="::group::Log message source details",
                sources=source_list,  # type: ignore[call-arg]
            ),
            footer,
        ]
        end_of_log = ti.try_number != try_number or ti.state not in (
            TaskInstanceState.RUNNING,
            TaskInstanceState.DEFERRED,
        )
        if stderr:
            header_log = [StructuredLogMessage(event="::group::Task logs")]
            header_err = [StructuredLogMessage(event="::group::Errors outside of task execution")]
            out_stream = chain(
                header, header_log, out_stream, [footer], header_err, err_stream, [footer]
            )
        else:
            out_stream = chain(header, out_stream)
        log_pos = len(logs)
        if metadata and "log_pos" in metadata:
            log_pos = metadata["log_pos"] + log_pos

        return out_stream, {
            "end_of_log": end_of_log,
            "log_pos": log_pos,
        }
