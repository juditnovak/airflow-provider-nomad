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

import json
import logging
from json.decoder import JSONDecodeError
from pathlib import Path
from typing import Any

from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.sdk.types import RuntimeTaskInstanceProtocol
from pydantic import ValidationError

from airflow.providers.nomad.exceptions import NomadValidationError
from airflow.providers.nomad.models import NomadJobModel

logger = logging.getLogger(__name__)


def job_id(dag_id: str, task_id: str, run_id: str, try_number: int, map_index: int):
    return f"{dag_id}-{task_id}-{run_id}-{try_number}-{map_index}"


def job_id_from_taskinstance_key(key: TaskInstanceKey) -> str:
    dag_id, task_id, run_id, try_number, map_index = key
    return job_id(dag_id, task_id, run_id, try_number, map_index)


def job_id_from_taskinstance(ti: TaskInstance | RuntimeTaskInstanceProtocol) -> str:
    index = ti.map_index if ti.map_index else -1
    run_id = ti.run_id if ti.run_id else ""
    return job_id(ti.dag_id, ti.task_id, run_id, ti.try_number, index)


def job_task_id_from_taskinstance_key(key: TaskInstanceKey) -> str:
    dag_id, task_id, _, _, _ = key
    return f"{dag_id}-{task_id}"


def validate_nomad_job(data: dict[str, Any]) -> NomadJobModel:
    try:
        return NomadJobModel.model_validate(data)
    except ValidationError as err:
        raise NomadValidationError(err)


def validate_nomad_job_json(json_str: str) -> NomadJobModel:
    try:
        return NomadJobModel.model_validate_json(json_str)
    except (ValidationError, JSONDecodeError) as err:
        raise NomadValidationError(err)


def parse_json_job_template(path: Path) -> NomadJobModel | None:
    """try to parse a json or hcl input as a nomad job template"""
    try:
        file = open(str(path))
    except (IOError, OSError) as err:
        logger.error(f"Can't open file: {path}' ({err})")
        return  # type: ignore [return-value]

    with file:
        try:
            data = json.load(file)
        except json.JSONDecodeError:
            return None
        return validate_nomad_job(data)


def dict_to_lines(d: dict[str, Any]) -> list[str]:
    return json.dumps(d, sort_keys=True, indent=4).splitlines()
