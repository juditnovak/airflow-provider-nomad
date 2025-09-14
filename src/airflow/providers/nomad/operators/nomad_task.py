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

from airflow.sdk import Context
from airflow.sdk.types import RuntimeTaskInstanceProtocol
from pydantic import ValidationError  # type: ignore[import-untyped]

from airflow.providers.nomad.exceptions import NomadOperatorError
from airflow.providers.nomad.generic_interfaces.nomad_operator_interface import NomadOperator
from airflow.providers.nomad.models import NomadJobModel, TaskConfig
from airflow.providers.nomad.templates.nomad_job_template import default_task_template

EXECUTOR_NAME = "nomad_executor"


class NomadTaskOperator(NomadOperator):
    def __init__(self, **kwargs):
        super().__init__(observe=True, **kwargs)

    def job_id(self, ti: RuntimeTaskInstanceProtocol):
        return f"nomad-task-{ti.dag_id}-{ti.task_id}-{ti.run_id}-{ti.try_number}-{ti.map_index}"

    def prepare_job_template(self, context: Context) -> NomadJobModel | None:
        default_template = False
        try:
            if content := context.get("params", {}).get("template_content", ""):
                template = self.nomad_mgr.parse_template_content(content)
            else:
                template = NomadJobModel.model_validate(default_task_template)
                default_template = True

            if not template:
                raise NomadOperatorError("Nothing to execute")

            if len(template.Job.TaskGroups) > 1:
                raise NomadOperatorError("NomadTaskOperator only allows for a single taskgroup")

            if len(template.Job.TaskGroups[0].Tasks) > 1:
                raise NomadOperatorError("NomadTaskOperator only allows for a single task")

            if image := context.get("params", {}).get("image"):
                template.Job.TaskGroups[0].Tasks[0].Config.image = image

            if entrypoint := context.get("params", {}).get("entrypoint"):
                template.Job.TaskGroups[0].Tasks[0].Config.entrypoint = entrypoint
            elif default_template:
                # Removing (Airflow SDK execution) entrypoint from default template
                config_dict = (
                    template.Job.TaskGroups[0].Tasks[0].Config.model_dump(exclude_unset=True)
                )
                config_dict.pop("entrypoint")
                template.Job.TaskGroups[0].Tasks[0].Config = TaskConfig.model_validate(config_dict)

            if args := context.get("params", {}).get("args"):
                template.Job.TaskGroups[0].Tasks[0].Config.args = args

            if command := context.get("params", {}).get("command"):
                template.Job.TaskGroups[0].Tasks[0].Config.command = command

        except ValidationError as err:
            raise NomadOperatorError(f"Template validation failed: {err.errors()}")

        if not (ti := context.get("ti")):
            raise NomadOperatorError(f"No task instance found in context {context}")

        template.Job.ID = self.job_id(ti)

        return template
