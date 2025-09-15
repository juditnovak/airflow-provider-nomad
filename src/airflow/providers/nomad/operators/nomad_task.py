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

from random import randint

from airflow.sdk import Context
from airflow.sdk.types import RuntimeTaskInstanceProtocol
from pydantic import ValidationError  # type: ignore[import-untyped]

from airflow.providers.nomad.exceptions import NomadTaskOperatorError
from airflow.providers.nomad.generic_interfaces.nomad_operator_interface import NomadOperator
from airflow.providers.nomad.models import NomadJobModel, TaskConfig
from airflow.providers.nomad.templates.nomad_job_template import default_task_template
from airflow.providers.nomad.utils import job_id_from_taskinstance


class NomadTaskOperator(NomadOperator):
    def __init__(self, **kwargs):
        super().__init__(observe=True, **kwargs)

    def job_id(self, ti: RuntimeTaskInstanceProtocol):
        id_base = job_id_from_taskinstance(ti)
        rnd = randint(0, 10000)
        while self.nomad_mgr.get_nomad_job_submission(f"{id_base}-{rnd}"):
            rnd = randint(0, 10000)
        return f"{id_base}-{rnd}"

    def prepare_job_template(self, context: Context):
        try:
            if content := context.get("params", {}).get("template_content", ""):
                template = self.nomad_mgr.parse_template_content(content)
            else:
                template = NomadJobModel.model_validate(default_task_template)
                # Removing (Airflow SDK execution) entrypoint from default template
                config_dict = (
                    template.Job.TaskGroups[0].Tasks[0].Config.model_dump(exclude_unset=True)
                )
                config_dict.pop("entrypoint")
                template.Job.TaskGroups[0].Tasks[0].Config = TaskConfig.model_validate(config_dict)

            if not template:
                raise NomadTaskOperatorError("Nothing to execute")

            if len(template.Job.TaskGroups) > 1:
                raise NomadTaskOperatorError("NomadTaskOperator only allows for a single taskgroup")

            if len(template.Job.TaskGroups[0].Tasks) > 1:
                raise NomadTaskOperatorError("NomadTaskOperator only allows for a single task")

            if image := context.get("params", {}).get("image"):
                template.Job.TaskGroups[0].Tasks[0].Config.image = image

            if entrypoint := context.get("params", {}).get("entrypoint"):
                template.Job.TaskGroups[0].Tasks[0].Config.entrypoint = entrypoint

            if args := context.get("params", {}).get("args"):
                template.Job.TaskGroups[0].Tasks[0].Config.args = args

            if command := context.get("params", {}).get("command"):
                template.Job.TaskGroups[0].Tasks[0].Config.command = command

        except ValidationError as err:
            raise NomadTaskOperatorError(f"Template validation failed: {err.errors()}")

        if not (ti := context.get("ti")):
            raise NomadTaskOperatorError(f"No task instance found in context {context}")

        template.Job.ID = self.job_id(ti)
        self.template = template
