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

from airflow.sdk import Context

from airflow.providers.nomad.operators.task import NomadTaskOperator
from airflow.providers.nomad.templates.job_template import DEFAULT_TASK_TEMPLATE_PYTHON


class NomadPythonTaskOperator(NomadTaskOperator):
    """Nomad Operator allowing for lightweight job submission"""

    template_fields: Collection[str] = list(NomadTaskOperator.template_fields) + ["python_command"]
    default_template = DEFAULT_TASK_TEMPLATE_PYTHON

    def __init__(
        self,
        python_command: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.python_command = python_command

    def prepare_job_template(self, context: Context):
        super().prepare_job_template(context)
        if not self.template:
            return

        if self.template.Job.TaskGroups[0].Tasks[0].Config.args:
            self.template.Job.TaskGroups[0].Tasks[0].Config.args.append(self.python_command)
        else:
            self.template.Job.TaskGroups[0].Tasks[0].Config.args = [self.python_command]
