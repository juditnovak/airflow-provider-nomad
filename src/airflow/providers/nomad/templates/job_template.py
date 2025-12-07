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

from typing import Any

from airflow.configuration import conf

from airflow.providers.nomad.constants import CONFIG_SECTION

default_image = conf.get(
    CONFIG_SECTION, "default_docker_image", fallback="novakjudit/airflow-nomad-runner:0.0.6.dev1"
)

default_task_template: dict[str, Any] = {
    "Job": {
        "AllAtOnce": None,
        "Constraints": None,
        "CreateIndex": None,
        "Datacenters": ["dc1"],
        "ID": "example",
        "JobModifyIndex": None,
        "Meta": None,
        "ModifyIndex": None,
        "Name": "ariflow_run",
        "Namespace": None,
        "ParameterizedJob": None,
        "ParentID": None,
        "Payload": None,
        "Periodic": None,
        "Priority": None,
        "Region": None,
        "Stable": None,
        "Status": None,
        "StatusDescription": None,
        "Stop": None,
        "SubmitTime": None,
        "TaskGroups": [
            {
                "Constraints": None,
                "Count": 1,
                "EphemeralDisk": {"Migrate": None, "SizeMB": 300, "Sticky": None},
                "Meta": None,
                "Name": "airflow-execution-taskgroup",
                "RestartPolicy": {
                    "Attempts": 0,
                    "Delay": 25000000000,
                    "Interval": 300000000000,
                    "Mode": "fail",
                },
                "Tasks": [
                    {
                        "Artifacts": None,
                        "Config": {
                            "image": default_image,
                            "entrypoint": [
                                "python",
                                "-m",
                                "airflow.sdk.execution_time.execute_workload",
                                "--json-string",
                            ],
                            "args": [],
                        },
                        "Constraints": None,
                        "DispatchPayload": None,
                        "Driver": "docker",
                        "Env": {
                            "AIRFLOW_CONFIG": "/opt/airflow/config/airflow.cfg",
                            "AIRFLOW_HOME": "/opt/airflow/",
                        },
                        "KillTimeout": None,
                        "Leader": False,
                        "LogConfig": None,
                        "Meta": None,
                        "Name": "airflow-task",
                        "Resources": {
                            "CPU": 500,
                            "DiskMB": None,
                            "MemoryMB": 256,
                        },
                        "ShutdownDelay": 1000000000000,
                        "Templates": None,
                        "User": "",
                        "Vault": None,
                        "VolumeMounts": [
                            {
                                "Destination": "/opt/airflow/config",
                                "PropagationMode": "private",
                                "ReadOnly": True,
                                "SELinuxLabel": "",
                                "Volume": "config",
                            },
                            {
                                "Destination": "/opt/airflow/dags",
                                "PropagationMode": "private",
                                "ReadOnly": True,
                                "SELinuxLabel": "",
                                "Volume": "dags",
                            },
                            {
                                "Destination": "/opt/airflow/logs",
                                "PropagationMode": "private",
                                "ReadOnly": False,
                                "SELinuxLabel": "",
                                "Volume": "logs",
                            },
                        ],
                    }
                ],
                "Update": None,
                "Volumes": {
                    "config": {
                        "AccessMode": "",
                        "AttachmentMode": "",
                        "MountOptions": None,
                        "Name": "config",
                        "PerAlloc": False,
                        "ReadOnly": True,
                        "Source": "config",
                        "Sticky": False,
                        "Type": "host",
                    },
                    "dags": {
                        "AccessMode": "",
                        "AttachmentMode": "",
                        "MountOptions": None,
                        "Name": "dags",
                        "PerAlloc": False,
                        "ReadOnly": True,
                        "Source": "dags",
                        "Sticky": False,
                        "Type": "host",
                    },
                    "logs": {
                        "AccessMode": "single-node-writer",
                        "AttachmentMode": "file-system",
                        "MountOptions": None,
                        "Name": "logs",
                        "PerAlloc": False,
                        "ReadOnly": False,
                        "Source": "airflow-logs",
                        "Sticky": False,
                        "Type": "host",
                    },
                },
            }
        ],
        "Type": "batch",
        "VaultToken": None,
        "Version": None,
    }
}
