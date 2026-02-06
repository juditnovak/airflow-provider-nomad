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
from copy import deepcopy
from random import randint
from typing import Any

from airflow.sdk import Context
from airflow.sdk.types import RuntimeTaskInstanceProtocol

from airflow.providers.nomad.exceptions import NomadTaskOperatorError
from airflow.providers.nomad.generic_interfaces.nomad_operator_interface import NomadOperator
from airflow.providers.nomad.templates.job_template import DEFAULT_TASK_TEMPLATE_BASH
from airflow.providers.nomad.utils import job_id_from_taskinstance, job_short_id_from_taskinstance


class NomadTaskOperator(NomadOperator):
    """Nomad Operator allowing for lightweight job submission"""

    template_fields: Collection[str] = [
        "template_path",
        "template_content",
        "env",
        "image",
        "entrypoint",
        "args",
        "command",
        "task_resources",
        "ephemeral_disk",
        "volumes",
        "volume_mounts",
    ]
    default_template = DEFAULT_TASK_TEMPLATE_BASH

    def __init__(
        self,
        template_path: str | None = None,
        template_content: str | None = None,
        env: dict[str, str] | None = None,
        image: str | None = None,
        entrypoint: list[str] | None = None,
        args: list[str] | None = None,
        command: str | None = None,
        task_resources: dict[str, Any] | None = None,
        ephemeral_disk: dict[str, str] | None = None,
        volumes: dict[str, dict[str, int | bool | str]] | None = None,
        volume_mounts: list[dict[str, int | bool | str]] | None = None,
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
        self.task_resources = task_resources
        self.ephemeral_disk = ephemeral_disk
        self.volumes = volumes
        self.volume_mounts = volume_mounts
        super().__init__(observe=True, **kwargs)

    def generate_job_id(self, ti: RuntimeTaskInstanceProtocol):
        id_base = job_id_from_taskinstance(ti)
        rnd = randint(0, 10000)
        while self.nomad_mgr.get_nomad_job_submission(f"{id_base}-{rnd}"):
            rnd = randint(0, 10000)
        return f"{id_base}-{rnd}", rnd

    def param_defined(self, param: str, context: Context) -> Any | None:
        if value := getattr(self, param, None):
            return value
        return context.get("params", {}).get(param)

    @property
    def tpl_attrs_dict(self):
        attrs = [
            "template_path",
            "template_content",
            "image",
            "entrypoint",
            "args",
            "command",
            "env",
            "task_resources",
            "volumes",
            "volume_mounts",
            "ephemeral_disk",
        ]
        return {attr: getattr(self, attr) for attr in attrs}

    def prepare_job_template(self, context: Context):
        updated_context = deepcopy(context.get("params", {}))
        self_attrs = {
            attr: self.tpl_attrs_dict[attr]
            for attr in self.tpl_attrs_dict
            if self.tpl_attrs_dict[attr] is not None
        }
        updated_context.update(self_attrs)

        if not (
            template := self.nomad_mgr.prepare_job_template(updated_context, self.default_template)
        ):
            raise NomadTaskOperatorError(f"No template for task with context {context}")

        if not (ti := context.get("ti")):
            raise NomadTaskOperatorError(f"No task instance found in context {context}")

        job_id, rnd = self.generate_job_id(ti)
        template.Job.ID = job_id
        template.Job.Name += f"-{job_short_id_from_taskinstance(ti)}-{rnd}"
        self.template = template
