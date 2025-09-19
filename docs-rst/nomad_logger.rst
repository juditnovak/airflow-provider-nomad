 .. This file is part of apache-airflow-providers-nomad which is
    released under Apache License 2.0. See file LICENSE or go to

       http://www.apache.org/licenses/LICENSE-2.0

 .. for full license details.

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


NomadLoghandler
=====================

.. note:: ``NomadExecutor`` is fully compatible with Airflow default ``FileTaskHandler`` log handler. Environments with remote logging may rather stick to that.

This feature may rather be useful in small, local (development) environments.

``NomadLoghandler`` uses Nomad as the only source. Logs are directly downloaded from Nomad each time. 
Log format is using Airflow standards, thus the output complies to the regular Airflow logs view.

This is useful when no remote logging is enabled,

Rationale:

    Since Airflow's default ``FileTashHandler`` log handler operates on remote or local log files, it only refers to
    the executor's log retrieval method (`get_task_log()`) while the task is running. In case of short-lived tasks
    this timeframe is short. In case no local/remote logfile exists, information about the task is impossible to retrieve

``NomadLoghandler`` instead constantly refers to Nomad. As long as the runner is not cleaned up on Nomad
side, it remains available for the Airflow API. 



Configuration
********************

To enable `NomadLoghandler` the following configuration should be set:


 * ``logging_conf_class``: This setting defines the logger configuration to be loaded. The ``airflow.providers.nomad.nomad_log.NOMAD_LOG_CONFIG`` variable holds a full logger configuration, having added ``nomad_log_handler`` to the Airflow defaults.
 * ``task_log_reader``: This setting refers to the log handler used by the Airflow web API. Must be set to ``nomad_log_handler``

.. code-block:: ini

    [logging]
    logging_config_class = airflow.providers.nomad.nomad_log.NOMAD_LOG_CONFIG
    task_log_reader = nomad_log_handler


Examples:


.. figure:: images/logs_as_normal.png

    The above image is showing the output once the task had no errors


.. figure:: images/log_groups.png

    The above image is showing the output once the task execution also produced standard output


.. figure:: images/logs_nomad_job_info.png

    The above image is showing the Nomad context once task execution did not perform.

