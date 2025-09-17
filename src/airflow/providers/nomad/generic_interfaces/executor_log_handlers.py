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

"""Logging module to fetch logs via the Nomad API"""

import logging
from collections.abc import Callable
from itertools import chain
from types import GeneratorType
from typing import Generator, cast

from airflow.executors.base_executor import BaseExecutor
from airflow.executors.executor_loader import ExecutorLoader
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.utils.log.file_task_handler import (
    LegacyProvidersLogType,
    LogHandlerOutputStream,
    LogMetadata,
    LogSourceInfo,
    StructuredLogMessage,
    StructuredLogStream,
)
from airflow.utils.state import TaskInstanceState

logger = logging.getLogger(__name__)


class ExecutorLogLinesHandler(logging.Handler):
    """Log handler retrieve logs directly from Nomad

    Log format: Airflow's StructuredLogMessage stream
    """

    name = "executor_log_lines_handler"
    executor_instances: dict[str, BaseExecutor] = {}
    DEFAULT_EXECUTOR_KEY = "_default_executor"

    def __init__(self):
        super().__init__()
        self.handler: logging.Handler | None = None

    def emit(self, record):
        if self.handler:
            self.handler.emit(record)

    def flush(self):
        if self.handler:
            self.handler.flush()

    def close(self):
        if self.handler:
            self.handler.close()

    def _get_executor_function(
        self, ti: TaskInstance, function_name: str
    ) -> Callable[[TaskInstance, int], tuple[list[str], list[str]]] | None:
        executor_name = ti.executor or self.DEFAULT_EXECUTOR_KEY
        executor = self.executor_instances.get(executor_name)
        if executor is not None and hasattr(executor, function_name):
            function = getattr(executor, function_name, None)
        else:
            if executor_name == self.DEFAULT_EXECUTOR_KEY:
                self.executor_instances[executor_name] = ExecutorLoader.get_default_executor()
            else:
                self.executor_instances[executor_name] = ExecutorLoader.load_executor(executor_name)
            function = getattr(self.executor_instances[executor_name], function_name, None)

        if callable(function):
            return function  # type: ignore[reportReturnType]
        return None

    def _read(
        self,
        ti: TaskInstance | TaskInstanceHistory,
        try_number: int,
        metadata: LogMetadata | None = None,
    ) -> tuple[LogHandlerOutputStream | LegacyProvidersLogType, LogMetadata]:
        """Retieving logs from executor, parsing it to a StructuredLogMessage stream."""
        logger.info("Collecting Nomad logs")
        sources: LogSourceInfo = []
        err_sources: LogSourceInfo = []
        source_list: list[str] = []

        logs: list[str] = []
        stderr: list[str] = []

        # Stdout
        executor_get_task_log = self._get_executor_function(ti, "get_task_log")
        if not executor_get_task_log:
            logger.error("Executor doesn't support 'get_task_log()' call")
            return (iter([StructuredLogMessage(event="")]), {"end_of_log": True})

        response = executor_get_task_log(ti, try_number)
        if response:
            sources, logs = response
        if sources:
            source_list.extend(sources)

        # Stderr
        if executor_get_task_stderr := self._get_executor_function(ti, "get_task_stderr"):
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

    def read(
        self,
        task_instance: TaskInstance | TaskInstanceHistory,
        try_number: int | None = None,
        metadata: LogMetadata | None = None,
    ) -> tuple[LogHandlerOutputStream, LogMetadata]:
        """
        Read logs of given task instance from local machine.

        :param task_instance: task instance object
        :param try_number: task instance try_number to read logs from. If None
                            it returns the log of task_instance.try_number
        :param metadata: log metadata, can be used for steaming log reading and auto-tailing.
        :return: a list of listed tuples which order log string by host
        """
        if try_number is None:
            try_number = task_instance.try_number

        if try_number == 0 and task_instance.state == TaskInstanceState.SKIPPED:
            logs = [
                StructuredLogMessage(  # type: ignore[call-arg]
                    event="Task was skipped, no logs available."
                )
            ]
            return chain(logs), {"end_of_log": True}

        if try_number is None or try_number < 1:
            logs = [
                StructuredLogMessage(  # type: ignore[call-arg]
                    level="error",  # type: ignore[reportCallIssue]
                    event=f"Error fetching the logs. Try number {try_number} is invalid.",
                )
            ]
            return chain(logs), {"end_of_log": True}

        # compatibility for es_task_handler and os_task_handler
        read_result = self._read(task_instance, try_number, metadata)
        out_stream, metadata = read_result
        # If the out_stream is None or empty, return the read result
        if not out_stream:
            out_stream = cast("Generator[StructuredLogMessage, None, None]", out_stream)
            return out_stream, metadata
        if isinstance(out_stream, (chain, GeneratorType)):
            out_stream = cast("Generator[StructuredLogMessage, None, None]", out_stream)
            return out_stream, metadata
        if isinstance(out_stream, list) and isinstance(out_stream[0], StructuredLogMessage):
            out_stream = cast("list[StructuredLogMessage]", out_stream)
            return (log for log in out_stream), metadata
        else:
            raise TypeError(
                "Invalid log stream type. Expected a generator of StructuredLogMessage, list of StructuredLogMessage, list of str or str."
                f" Got {type(out_stream).__name__} instead."
            )
