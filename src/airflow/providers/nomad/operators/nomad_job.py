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

from collections.abc import Collection

from airflow.sdk import Context
from pydantic import ValidationError

from airflow.providers.nomad.generic_interfaces.nomad_operator_interface import NomadOperator
from airflow.providers.nomad.models import NomadJobModel
from airflow.providers.nomad.templates.nomad_job_template import default_task_template


class NomadJobOperator(NomadOperator):
    template_fields: Collection[str] = ["template_content"]

    def __init__(
        self,
        template_path: str | None = None,
        template_content: str | None = None,
        observe: bool = True,
        job_log_file: str | None = None,
        **kwargs,
    ):
        if template_path and template_content:
            raise ValueError("Only one of 'template_content' and 'template_path' can be specified")
        self.template_content = template_content
        self.template_path = template_path
        super().__init__(observe=observe, job_log_file=job_log_file, **kwargs)

    def prepare_job_template(self, context: Context):
        if self.template_content:
            self.template = self.nomad_mgr.parse_template_content(self.template_content)
            return

        content = None
        if self.template_path:
            filepath = self.figure_path(self.template_path)
            try:
                with open(filepath) as f:
                    content = f.read()
            except (OSError, IOError) as err:
                self.log.error(f"Can't load job template ({err})")
                return

        if content or (content := context.get("params", {}).get("template_content", "")):
            self.template = self.nomad_mgr.parse_template_content(content)
            return

        try:
            self.template = NomadJobModel.model_validate(default_task_template)
        except ValidationError:
            self.log.error("Default template validation failed")
