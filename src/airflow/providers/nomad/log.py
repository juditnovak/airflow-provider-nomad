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

"""Logging module to fetch logs via the Nomad API"""

import copy
import logging

import airflow.logging_config
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

from airflow.providers.nomad.generic_interfaces.executor_log_handlers import ExecutorLogLinesHandler

logger = logging.getLogger(__name__)


class NomadLogHandler(ExecutorLogLinesHandler):
    """Extended handler to retrieve logs directly from Nomad"""

    name = "nomad_log_handler"


NOMAD_HANDLER_NAME = NomadLogHandler.name

NOMAD_LOG_CONFIG = copy.deepcopy(DEFAULT_LOGGING_CONFIG)

NOMAD_LOG_CONFIG["handlers"][NOMAD_HANDLER_NAME] = {
    "class": "airflow.providers.nomad.log.NomadLogHandler",
    "formatter": "airflow",
    "filters": ["mask_secrets"],
}

NOMAD_LOG_CONFIG["loggers"]["airflow.task"]["handlers"].append(NOMAD_HANDLER_NAME)


# Due to bug on loading config for services such as dag-processor
# Reproduce: uncomment these lines and run `airflow dag-processor
airflow.logging_config.REMOTE_TASK_LOG = None
