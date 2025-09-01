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

"""Logging module to fetch logs via the Nomad API"""

import copy
import logging

from airflow.config_templates.airflow_local_settings import BASE_LOG_FOLDER, DEFAULT_LOGGING_CONFIG

from airflow.providers.nomad.generic_interfaces.executor_log_handlers import ExecutorLogLinesHandler

logger = logging.getLogger(__name__)


class NomadLogHandler(ExecutorLogLinesHandler):
    """Extended handler to retrieve logs directly from Nomad"""

    name = "nomad_log_handler"


NOMAD_HANDLER_NAME = NomadLogHandler.name

NOMAD_LOG_CONFIG = copy.deepcopy(DEFAULT_LOGGING_CONFIG)

NOMAD_LOG_CONFIG["handlers"][NOMAD_HANDLER_NAME] = {
    "class": "airflow.providers.nomad.executors.nomad_log.NomadLogHandler",
    "formatter": "airflow",
    "base_log_folder": BASE_LOG_FOLDER,
    "filters": ["mask_secrets"],
}

NOMAD_LOG_CONFIG["loggers"]["airflow.task"]["handlers"].append(NOMAD_HANDLER_NAME)


# Due to bug on loading config for services such as dag-processor
# Reproduce: uncomment these lines and run `airflow dag-processor
import airflow.logging_config

airflow.logging_config.REMOTE_TASK_LOG = None
