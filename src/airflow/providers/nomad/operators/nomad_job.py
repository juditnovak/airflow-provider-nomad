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

import time
from pathlib import Path

from airflow.configuration import conf
from airflow.sdk import Context
from airflow.sdk.bases.operator import BaseOperator
from nomad.api.exceptions import BaseNomadException  # type: ignore[import-untyped]

from airflow.providers.nomad.exceptions import NomadJobOperatorError
from airflow.providers.nomad.models import (
    JobEvalStatus,
    JobInfoStatus,
    NomadJobAllocList,
    NomadJobModel,
)
from airflow.providers.nomad.nomad_manager import NomadManager
from airflow.providers.nomad.templates.nomad_job_template import default_task_template

EXECUTOR_NAME = "nomad_executor"


class NomadJobOperator(BaseOperator):
    def __init__(self, observe: bool = True, job_log_file: str | None = None, **kwargs):
        self.nomad_mgr = NomadManager()
        self.nomad_mgr.initialize()
        self.observe = observe
        self.job_log_file = job_log_file
        self.operator_poll_delay: int = conf.getint(
            EXECUTOR_NAME, "operator_poll_delay", fallback=10
        )
        super().__init__(**kwargs)

    @staticmethod
    def sanitize_logs(alloc_id: str, task_name: str, logs: str) -> str:
        sanitized_logs = logs

        fileloc = Path(f"{alloc_id}-{task_name}.log")
        if fileloc.is_file():
            with fileloc.open("r") as file:
                prefix = file.read()
                if logs.startswith(prefix):
                    sanitized_logs = logs[len(prefix) :]

        with fileloc.open("w") as file:
            file.write(logs)

        return sanitized_logs

    def get_children_output(
        self, job_id, alloc_info: NomadJobAllocList | None = None
    ) -> tuple[str, str]:
        if not alloc_info and not (alloc_info := self.nomad_mgr.get_nomad_job_allocation(job_id)):
            return "", ""

        all_logs = ""
        all_output = []
        for allocation in alloc_info:
            for task_name in allocation.TaskStates:
                logs = ""
                if self.job_log_file:
                    logs += self.nomad_mgr.get_job_file(allocation.ID, self.job_log_file)

                if output := self.nomad_mgr.get_job_stdout(allocation.ID, task_name):
                    all_output.append(output)
                    logs += output

                logs += self.nomad_mgr.get_job_stderr(allocation.ID, task_name)

                all_logs += self.sanitize_logs(allocation.ID, task_name, logs)

                for line in logs.splitlines():
                    self.log.info("[job: %s][alloc: %s] %s", job_id, allocation.ID, line)

        return ",".join(all_output), all_logs

    def execute(self, context: Context):
        # self.log.info(f"Context is {context}")

        if content := context.get("params", {}).get("template_content", ""):
            template = self.nomad_mgr.parse_template_content(content)
        else:
            template = NomadJobModel.model_validate(default_task_template)

        if not template:
            raise NomadJobOperatorError("Nothing to execute")

        job_id = template.Job.ID
        try:
            response = self.nomad_mgr.nomad.job.register_job(job_id, template.model_dump())  # type: ignore[optionalMemberAccess, union-attr]
        except BaseNomadException as err:
            raise NomadJobOperatorError(
                f"Job submission failed ({err}), job template: {template.model_dump_json()}"
            )

        if not response:
            self.log.warning("No response on job submission attempt, may suggest an error")

        if not self.observe:
            return

        status: JobInfoStatus | None = JobInfoStatus.pending
        job_info: list[str] = []
        logs = ""
        job_status, job_alloc, job_eval, job_summary = None, None, None, None
        job_snapshot = {}
        output = ""
        all_done = False
        while status != JobInfoStatus.dead and not all_done:
            job_info = []

            # Snapshotting state
            job_status = self.nomad_mgr.get_nomad_job_submission(job_id)
            job_alloc = self.nomad_mgr.get_nomad_job_allocation(job_id)
            job_eval = self.nomad_mgr.get_nomad_job_evaluations(job_id)
            job_summary = self.nomad_mgr.get_nomad_job_summary(job_id)
            job_snapshot = {
                "job_summary": job_summary,
                "job_alloc": job_alloc,
                "job_eval": job_eval,
            }

            output, logs = self.get_children_output(job_id, alloc_info=job_alloc)

            if not job_status or job_status.Status != JobInfoStatus.running:
                if (
                    result := self.nomad_mgr.remove_job_if_hanging(
                        job_id, job_status=job_status, **job_snapshot
                    )
                ) and result[0]:
                    _, error = result
                    if job_info := self.nomad_mgr.job_all_info_str(job_id, **job_snapshot):
                        NomadJobOperatorError(
                            "Job %s got killed due to error: %s\nAdditional info:\n",
                            job_id,
                            error,
                            "\n".join(job_info),
                        )
                    break

            time.sleep(self.operator_poll_delay)
            status = job_status.Status if job_status else None
            all_done = job_summary.all_done() if job_summary else False

        if job_eval and any(evalu.Status == JobEvalStatus.complete for evalu in job_eval):
            if output:
                return output
            if not job_info:
                job_info = self.nomad_mgr.job_all_info_str(job_id, **job_snapshot)
                return (
                    f"No output from job. Logs/stderr: {logs.splitlines()}\nJob info: \n"
                    + "\n".join(job_info)
                )

        job_alloc = self.nomad_mgr.get_nomad_job_allocation(job_id)
        job_eval = self.nomad_mgr.get_nomad_job_evaluations(job_id)
        job_summary = self.nomad_mgr.get_nomad_job_summary(job_id)
        job_info = self.nomad_mgr.job_all_info_str(
            job_id, job_alloc=job_alloc, job_eval=job_eval, job_summary=job_summary
        )
        job_info_str = "\n".join(job_info)
        raise NomadJobOperatorError(f"Job submission failed {job_info_str}")
