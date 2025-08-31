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
from itertools import chain

import nomad  # type: ignore[import-untyped]
from airflow.configuration import conf
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.models.taskinstancekey import TaskInstanceKey
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

from airflow.providers.nomad.executors.nomad_executor import NomadExecutor

logger = logging.getLogger(__name__)

PROVIDER_NAME = "nomad"


class ExecutorLogLinesHandler(FileTaskHandler):
    """Extended handler to retrieve logs directly from Nomad"""

    name = "executor_log_lines_handler"

    def __init__(self, base_log_folder, *args, **kwargs):
        super().__init__(base_log_folder, *args, **kwargs)

        self.nomad_server_ip = conf.get(PROVIDER_NAME, "server_ip", fallback="0.0.0.0")
        self.secure = conf.getboolean(PROVIDER_NAME, "secure", fallback=False)
        self.cert_path = conf.get(PROVIDER_NAME, "cert_path", fallback="")
        self.key_path = conf.get(PROVIDER_NAME, "key_path", fallback="")
        self.verify = conf.getboolean(PROVIDER_NAME, "verify", fallback=False)
        self.namespace = conf.get(PROVIDER_NAME, "namespace", fallback="")
        self.token = conf.get(PROVIDER_NAME, "token", fallback="")

    def retrieve_logs(self, key: TaskInstanceKey) -> tuple[list[str], list[str]]:
        self.nomad = nomad.Nomad(
            host=self.nomad_server_ip,
            secure=self.secure,
            cert=(self.cert_path, self.key_path),
            verify=self.verify,
            namespace=self.namespace,
            token=self.token,
        )

        messages = []
        job_id = NomadExecutor.job_id_from_taskinstance_key(key)
        job_task_id = NomadExecutor.job_task_id_from_taskinstance_key(key)
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
            return ([], [])

        logs = self.nomad.client.cat.read_file(
            allocation_id, path=f"alloc/logs/{job_task_id}.stdout.0"
        )
        return messages, logs.splitlines()

    def _read(
        self,
        ti: TaskInstance | TaskInstanceHistory,
        try_number: int,
        metadata: LogMetadata | None = None,
    ) -> tuple[LogHandlerOutputStream | LegacyProvidersLogType, LogMetadata]:
        """
        Re-write of the FileTaskHandler read method to add log retrieval from Nomad
        """
        logger.info("Collecting Nomad logs")
        sources: LogSourceInfo = []
        source_list: list[str] = []

        logs: list[str] = []
        executor_get_task_log = self._get_executor_get_task_log(ti)
        response = executor_get_task_log(ti, try_number)
        if response:
            sources, logs = response
        if sources:
            source_list.extend(sources)

        # out_stream: LogHandlerOutputStream = _interleave_logs((y for y in logs))

        def geneate_log_stream(logs: list[str]) -> StructuredLogStream:
            for line in logs:
                if not line:
                    continue
                try:
                    yield StructuredLogMessage.model_validate_json(line)
                except:
                    yield StructuredLogMessage(event=str(line))

        out_stream: LogHandlerOutputStream = geneate_log_stream(logs)

        # Same as for FileTaskHandler, add source details as a collapsible group
        header = [
            StructuredLogMessage(
                event="::group::Log message source details",
                sources=source_list,  # type: ignore[call-arg]
            ),
            StructuredLogMessage(event="::endgroup::"),
        ]
        end_of_log = ti.try_number != try_number or ti.state not in (
            TaskInstanceState.RUNNING,
            TaskInstanceState.DEFERRED,
        )

        out_stream = chain(header, out_stream)
        log_pos = len(logs)
        if metadata and "log_pos" in metadata:
            log_pos = metadata["log_pos"] + log_pos

        return out_stream, {
            "end_of_log": end_of_log,
            "log_pos": log_pos,
        }
