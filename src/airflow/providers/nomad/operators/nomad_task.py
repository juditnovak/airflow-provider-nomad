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

from collections.abc import Collection
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
    """Nomad Operator allowing for lightweight job submission"""

    template_fields: Collection[str] = [
        "image",
        "entrypoint",
        "args",
        "command",
        "env",
        "template_content",
        "template_path",
    ]

    def __init__(
        self,
        template_path: str | None = None,
        template_content: str | None = None,
        env: dict[str, str] | None = None,
        image: str | None = None,
        entrypoint: list[str] | None = None,
        args: list[str] | None = None,
        command: str | None = None,
        **kwargs,
    ):
        if template_path and template_content:
            raise ValueError("Only one of 'template_content' and 'template_path' can be specified")
        self.template_content = template_content
        self.template_path = template_path
        self.image = image
        self.entrypoint = entrypoint
        self.args = args
        self.command = command
        self.env = env
        super().__init__(observe=True, **kwargs)

    def job_id(self, ti: RuntimeTaskInstanceProtocol):
        id_base = job_id_from_taskinstance(ti)
        rnd = randint(0, 10000)
        while self.nomad_mgr.get_nomad_job_submission(f"{id_base}-{rnd}"):
            rnd = randint(0, 10000)
        return f"{id_base}-{rnd}"

    def prepare_job_template(self, context: Context):
        content = None
        if self.template_path:
            filepath = self.figure_path(self.template_path)
            try:
                with open(filepath) as f:
                    content = f.read()
            except (OSError, IOError) as err:
                self.log.error(f"Can't load job template ({err})")
                return
        try:
            if (
                content
                or (content := self.template_content)
                or (content := context.get("params", {}).get("template_content", ""))
            ):
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

            if template.Job.TaskGroups[0].Count and template.Job.TaskGroups[0].Count > 1:
                raise NomadTaskOperatorError("Only a single execution is allowed (count=1)")

            if (image := self.image) or (image := context.get("params", {}).get("image")):
                template.Job.TaskGroups[0].Tasks[0].Config.image = image

            if (entrypoint := self.entrypoint) or (
                entrypoint := context.get("params", {}).get("entrypoint")
            ):
                template.Job.TaskGroups[0].Tasks[0].Config.entrypoint = entrypoint

            if (args := self.args) or (args := context.get("params", {}).get("args")):
                template.Job.TaskGroups[0].Tasks[0].Config.args = args

            if (command := self.command) or (command := context.get("params", {}).get("command")):
                template.Job.TaskGroups[0].Tasks[0].Config.command = command

            if (env := self.env) or (env := context.get("params", {}).get("env")):
                template.Job.TaskGroups[0].Tasks[0].Env = env

        except ValidationError as err:
            raise NomadTaskOperatorError(f"Template validation failed: {err.errors()}")

        if not (ti := context.get("ti")):
            raise NomadTaskOperatorError(f"No task instance found in context {context}")

        template.Job.ID = self.job_id(ti)
        self.template = template
