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
from typing import Any, Callable

import nomad  # type: ignore[import-untyped]
from airflow.configuration import conf
from airflow.utils.log.logging_mixin import LoggingMixin
from nomad.api.exceptions import BadRequestNomadException  # type: ignore[import-untyped]
from nomad.api.exceptions import BaseNomadException  # type: ignore[import-untyped]
from pydantic import ValidationError

from airflow.providers.nomad.exceptions import NomadProviderException, NomadValidationError
from airflow.providers.nomad.models import (
    JobEvalStatus,
    JobInfoStatus,
    NomadJobAllocations,
    NomadJobAllocList,
    NomadJobEvalList,
    NomadJobEvaluation,
    NomadJobModel,
    NomadJobSubmission,
    NomadJobSummary,
)
from airflow.providers.nomad.utils import dict_to_lines, validate_nomad_job, validate_nomad_job_json
from airflow.providers.nomad.constants import CONFIG_SECTION


class NomadManager(LoggingMixin):
    def __init__(self):
        self.nomad_server_ip: str = conf.get(CONFIG_SECTION, "agent_server_ip", fallback="0.0.0.0")
        self.nomad_server_port: int = conf.getint(
            CONFIG_SECTION, "agent_server_port", fallback=4646
        )
        self.secure: bool = conf.getboolean(CONFIG_SECTION, "agent_secure", fallback=False)
        self.cert_path: str = conf.get(CONFIG_SECTION, "agent_cert_path", fallback="")
        self.key_path: str = conf.get(CONFIG_SECTION, "agent_key_path", fallback="")
        self.namespace: str = conf.get(CONFIG_SECTION, "agent_namespace", fallback="")
        self.token: str = conf.get(CONFIG_SECTION, "agent_token", fallback="")

        self.alloc_pending_timeout: int = conf.getint(
            CONFIG_SECTION, "alloc_pending_timeout", fallback=600
        )

        self.verify: bool | str
        verify = conf.get(CONFIG_SECTION, "agent_verify", fallback="")
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

    def catch_nomad_exception(exc_retval: Any | None = None):  # type: ignore [reportGeneralTypeIssues]
        def decorator(f: Callable):
            def wrapped(self, *args, **kwargs):
                try:
                    return f(self, *args, **kwargs)
                except BaseNomadException as err:
                    self.log.info("Nomad error occurred: {%s}", err)
                return exc_retval

            return wrapped

        return decorator

    @catch_nomad_exception()
    def get_nomad_job_submission(self, job_id: str) -> NomadJobSubmission | None:  # type: ignore[return]
        if not (job_status := self.nomad.job.get_job(job_id)):  # type: ignore[reportOptionalMemberAccess, union-attr]
            return  # type: ignore [return-value]
        try:
            return NomadJobSubmission.model_validate(job_status)
        except ValidationError as err:
            self.log.debug("Couldn't parse Nomad job submission info: %s %s", err, err.errors())

    @catch_nomad_exception()
    def get_nomad_job_evaluations(self, job_id: str) -> NomadJobEvalList | None:  # type: ignore[return]
        if not (job_eval := self.nomad.job.get_evaluations(job_id)):  # type: ignore[reportOptionalMemberAccess, union-attr]
            return  # type: ignore [return-value]
        try:
            return NomadJobEvaluation.validate_python(job_eval)
        except ValidationError as err:
            self.log.debug("Couldn't parse Nomad job validation output: %s %s", err, err.errors())

    @catch_nomad_exception()
    def get_nomad_job_allocation(self, job_id: str) -> NomadJobAllocList | None:  # type: ignore[return]
        if not (job_allocations := self.nomad.job.get_allocations(job_id)):  # type: ignore[reportOptionalMemberAccess, union-attr]
            return  # type: ignore [return-value]
        try:
            return NomadJobAllocations.validate_python(job_allocations)
        except ValidationError as err:
            self.log.debug("Couldn't parse Nomad job allocations info: %s %s", err, err.errors())

    @catch_nomad_exception()
    def get_nomad_job_summary(self, job_id: str) -> NomadJobSummary | None:  # type: ignore[return]
        if not (job_summary := self.nomad.job.get_summary(job_id)):  # type: ignore[reportOptionalMemberAccess, union-attr]
            return  # type: ignore [return-value]
        try:
            return NomadJobSummary.model_validate(job_summary)
        except ValidationError as err:
            self.log.debug("Couldn't parse Nomad job summary: %s %s", err, err.errors())

    @catch_nomad_exception(exc_retval="")
    def get_job_stdout(self, allocation_id: str, job_task_id: str) -> str:
        return self.nomad.client.cat.read_file(  # type: ignore[reportOptionalMemberAccess, union-attr]
            allocation_id, path=f"alloc/logs/{job_task_id}.stdout.0"
        )

    @catch_nomad_exception(exc_retval="")
    def get_job_stderr(self, allocation_id: str, job_task_id: str) -> str:
        return self.nomad.client.cat.read_file(  # type: ignore[reportOptionalMemberAccess, union-attr]
            allocation_id, path=f"alloc/logs/{job_task_id}.stderr.0"
        )

    @catch_nomad_exception(exc_retval="")
    def get_job_file(self, allocation_id: str, file_path: str) -> str:
        return self.nomad.client.cat.read_file(  # type: ignore[reportOptionalMemberAccess, union-attr]
            allocation_id, path=f"alloc/{file_path}"
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

    def remove_job_if_hanging(
        self,
        job_id: str,
        job_status: NomadJobSubmission | None = None,
        job_alloc: NomadJobAllocList | None = None,
        job_eval: NomadJobEvalList | None = None,
        job_summary: NomadJobSummary | None = None,
    ) -> tuple[bool, str] | None:
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

        if not job_status and not (job_status := self.get_nomad_job_submission(job_id)):
            return None

        # Allocation failures: job is stuck in a 'pending' state
        if job_status.Status == JobEvalStatus.pending:
            if not self.timeout_expired(job_id):
                return False, ""

            if not job_eval and not (job_eval := self.get_nomad_job_evaluations(job_id)):
                return None

            failed_item = None
            for item in job_eval:
                if item.Status in JobEvalStatus.done_states() and item.FailedTGAllocs:
                    failed_item = item

            taskgroup_name = job_status.TaskGroups[0].Name
            if failed_item and (failed_alloc := failed_item.FailedTGAllocs.get(taskgroup_name)):  # type: ignore[reportOperationalMemberAccess, union-attr]
                self.log.info("Task %s was pending beyond timeout, stopping it.", job_id)
                self.nomad.job.deregister_job(job_id)  # type: ignore[reportOptionalMemberAccess, union-attr]
                error = failed_alloc.errors()
                return True, str(error)
        else:
            self.pending_jobs.pop(job_id, None)

        # Failures during job setup: job is stuck in a 'dead' state
        if job_status.Status == JobInfoStatus.dead:
            if not job_alloc:
                job_alloc = self.get_nomad_job_allocation(job_id)
            if not job_summary:
                job_summary = self.get_nomad_job_summary(job_id)

            if job_summary and job_summary.all_failed():
                self.log.info("Task %s seems dead, stopping it.", job_id)
                self.nomad.job.deregister_job(job_id)  # type: ignore[reportOptionalMemberAccess, union-attr]
                if job_alloc:
                    return True, str([alloc.errors() for alloc in job_alloc])
                return True, ""

        return False, ""

    def parse_template_json(self, template_content: str) -> NomadJobModel | None:  # type: ignore [return]
        try:
            return validate_nomad_job_json(template_content)
        except NomadValidationError as err:
            self.log.info("Couldn't parse template  as json, trying it as HCL (%s)", err)

    def parse_template_hcl(self, template_content: str) -> NomadJobModel | None:  # type: ignore [return]
        try:
            body = self.nomad.jobs.parse(template_content)  # type: ignore[optionalMemberAccess, union-attr]
            return validate_nomad_job({"Job": body})
        except BadRequestNomadException as err:
            self.log.info("Couldn't parse template as HCL (%s)", err)

    def parse_template_content(self, template_content: str) -> NomadJobModel | None:  # type: ignore [return]
        if not template_content:
            return None

        if isinstance(template_content, dict):
            return validate_nomad_job(template_content)

        template = self.parse_template_json(template_content)

        if not template:
            template = self.parse_template_hcl(template_content)

        return template

    def job_all_info_str(
        self,
        job_id,
        job_summary: NomadJobSummary | None = None,
        job_alloc: NomadJobAllocList | None = None,
        job_eval: NomadJobEvalList | None = None,
    ) -> list[str]:
        output = []
        if job_summary or (job_summary := self.get_nomad_job_summary(job_id)):
            output.append("Job summary:")
            output += dict_to_lines(NomadJobSummary.model_dump(job_summary))
        if job_alloc or (job_alloc := self.get_nomad_job_allocation(job_id)):
            output.append("Job allocations info:")
            output += dict_to_lines(NomadJobAllocations.dump_python(job_alloc))
        if job_eval or (job_eval := self.get_nomad_job_evaluations(job_id)):
            output.append("Job evaluations:")
            output += dict_to_lines(NomadJobEvaluation.dump_python(job_eval))
        return output
