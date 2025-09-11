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

"""Nomad Job Manager

The purpose of this module is to de-couple Nomad job management functionalities,
that are used both on executor and operator side.
"""

from datetime import datetime
from functools import cached_property

import nomad  # type: ignore[import-untyped]
from airflow.configuration import conf
from airflow.utils.log.logging_mixin import LoggingMixin
from nomad.api.exceptions import BaseNomadException  # type: ignore[import-untyped]
from nomad.api.exceptions import URLNotFoundNomadException  # type: ignore[import-untyped]
from pydantic import ValidationError

from airflow.providers.nomad.exceptions import NomadProviderException
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

EXECUTOR_NAME = "nomad_executor"


class NomadJobManager(LoggingMixin):
    def __init__(self):
        self.nomad_server_ip: str = conf.get(EXECUTOR_NAME, "server_ip", fallback="0.0.0.0")
        self.nomad_server_port: int = conf.getint(EXECUTOR_NAME, "server_port", fallback=4646)
        self.secure: bool = conf.getboolean(EXECUTOR_NAME, "secure", fallback=False)
        self.cert_path: str = conf.get(EXECUTOR_NAME, "cert_path", fallback="")
        self.key_path: str = conf.get(EXECUTOR_NAME, "key_path", fallback="")
        self.namespace: str = conf.get(EXECUTOR_NAME, "namespace", fallback="")
        self.token: str = conf.get(EXECUTOR_NAME, "token", fallback="")

        self.alloc_pending_timeout: int = conf.getint(
            EXECUTOR_NAME, "alloc_pending_timeout", fallback=600
        )

        self.verify: bool | str
        verify = conf.get(EXECUTOR_NAME, "verify", fallback="")
        if verify == "true":
            self.verify = True
        elif verify == "false":
            self.verify = False
        else:
            self.verify = verify

        self.nomad: nomad.Nomad | None = None
        self.pending_jobs: dict[str, int] = {}

    def initialize(self):
        self.nomad = nomad.Nomad(
            host=self.nomad_server_ip,
            secure=self.secure,
            cert=(self.cert_path, self.key_path),
            verify=self.verify,  # type: ignore[reportArtumentType]
            namespace=self.namespace,
            token=self.token,
        )
        if not self.nomad:
            raise NomadProviderException("Can't initialize nomad client")

    @cached_property
    def nomad_url(self):
        protocol = "http" if not self.secure else "https"
        return f"{protocol}://{self.nomad_server_ip}:{self.nomad_server_port}"

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

    def get_job_stdout(self, allocation_id: str, job_task_id: str) -> str:
        return self.nomad.client.cat.read_file(  # type: ignore[reportOptionalMemberAccess, union-attr]
            allocation_id, path=f"alloc/logs/{job_task_id}.stdout.0"
        )

    def get_job_stderr(self, allocation_id: str, job_task_id: str) -> str:
        return self.nomad.client.cat.read_file(  # type: ignore[reportOptionalMemberAccess, union-attr]
            allocation_id, path=f"alloc/logs/{job_task_id}.stderr.0"
        )

    def register_job(self, job_model: NomadJobModel) -> str | None:
        template = job_model.model_dump()
        try:
            self.nomad.job.register_job(job_model.Job.ID, template)  # type: ignore[reportOptionalMemberAccess, union-attr]

            self.log.debug("Runing task (%s) with template %s)", job_model.Job.ID, str(template))
        except BaseNomadException as err:
            self.log.error("Couldn't run task %s (%s)", job_model.Job.ID, err)
            try:
                self.nomad.job.deregister_job(job_model.Job.ID)  # type: ignore[reportOptionalMemberAccess, union-attr]
            except BaseNomadException:
                pass
            return str(err)
        return None

    def timeout_expired(self, job_id: str) -> bool:
        now = int(datetime.now().timestamp())
        if not self.pending_jobs.get(job_id):
            self.pending_jobs[job_id] = now
            return False
        else:
            return now - self.pending_jobs[job_id] > self.alloc_pending_timeout

    def remove_job_if_hanging(self, job_id: str) -> tuple[bool, str] | None:
        """Whether the job failed on Nomad side

        Typically on allocaton errors there is not feedback to Airflow, as the
        job remains in 'pending' state on Nomad side. Such issues have to be detected
        and the job execution is to be reported as failed.

        NOTE: The executor failing a job run is considered as an ERROR by Airflow.
        Despite the log message, this is the efficient way for this case. Potential Airflow-level
        task re-tries are applied corectly.

        :param key: reference to the task instance in question
        :return: either a tuple of: True/False, additional info or None if no data could be
                retrieved for the job
        """
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
            if not self.timeout_expired(job_id):
                return False, ""

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
                self.log.info("Task %s was pending beyond timeout, stopping it.", job_id)
                self.nomad.job.deregister_job(job_id)  # type: ignore[reportOptionalMemberAccess, union-attr]
                error = failed_alloc.errors()
                return True, str(error)
        else:
            self.pending_jobs.pop(job_id, None)

        # Failures during job setup: job is stuck in a 'dead' state
        if job_status.Status == JobInfoStatus.dead:
            job_alloc_info = self.get_nomad_job_allocation_info(job_id)
            job_summary = self.get_nomad_job_summary(job_id)
            if job_summary and job_summary.all_failed():
                self.log.info("Task %s seems dead, stopping it.", job_id)
                self.nomad.job.deregister_job(job_id)  # type: ignore[reportOptionalMemberAccess, union-attr]
                if job_alloc_info:
                    errors = [alloc.errors() for alloc in job_alloc_info]
                    return True, str(errors)
                return True, ""

        return False, ""
