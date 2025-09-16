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


def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-nomad",
        "name": "Nomad",
        "description": "`Nomad <https://developer.hashicorp.com/nomad/>`__\n",
        "integrations": [
            {
                "integration-name": "Nomad",
                "external-doc-url": "https://developer.hashicorp.com/nomad/",
                "how-to-guide": ["/docs/apache-airflow-providers-nomad/operators.rst"],
                "logo": "/docs/integration-logos/Nomad.png",
                "tags": ["software"],
            },
        ],
        "executors": ["airflow.providers.nomad.executors.nomad_executor.NomadExecutor"],
        "operators": [
            {
                "integration-name": "Nomad",
                "python-modules": [
                    "airflow.providers.nomad.operators.nomad_job",
                    "airflow.providers.nomad.operators.nomad_task",
                ],
            }
        ],
        # "sensors": [
        #     {
        #         "integration-name": "Nomad",
        #         "python-modules": ["airflow.providers.nomad.sensors.nomad"],
        #     }
        # ],
        # "hooks": [
        #     {
        #         "integration-name": "Nomad",
        #         "python-modules": ["airflow.providers.nomad.hooks.nomad"],
        #     }
        # ],
        # "triggers": [
        #     {
        #         "integration-name": "Nomad",
        #         "python-modules": [
        #             "airflow.providers.nomad.triggers.pod",
        #             "airflow.providers.nomad.triggers.job",
        #         ],
        #     }
        # ],
        # "task-decorators": [
        #     {
        #         "class-name": "airflow.providers.nomad.decorators.nomad.nomad_task",
        #         "name": "nomad",
        #     },
        # ],
        "config": {
            "nomad_provider": {
                "description": None,
                "options": {
                    "parallelism": {
                        "description": "Generic Airflow executor parallelism (should be higher than 0)",
                        "version_added": "0.0.1",
                        "type": "integer",
                        "example": "128",
                        "default": "128",
                    },
                    "agent_host": {
                        "description": "Nomad server (FQDN or IP)",
                        "version_added": "0.0.1",
                        "type": "string",
                        "example": "192.168.122.226",
                        "default": "0.0.0.0",
                    },
                    "agent_secure": {
                        "description": "Whether TLS certificates are to be considered",
                        "version_added": "0.0.1",
                        "type": "boolean",
                        "example": None,
                        "default": "False",
                    },
                    "agent_cert_path": {
                        "description": "Absolute path to client certificate",
                        "version_added": "0.0.1",
                        "type": "string",
                        "example": "/absolute/path/to/certs/global-cli-nomad.pem",
                        "default": "",
                    },
                    "agent_key_path": {
                        "description": "Absolute path to client key",
                        "version_added": "0.0.1",
                        "type": "string",
                        "example": "/absolute/path/to/certs/global-cli-nomad-key.pem",
                        "default": "",
                    },
                    "agent_verify": {
                        "description": "Absolute paht to CA certificate or true/false",
                        "version_added": "0.0.1",
                        "type": "string",
                        "example": "/absolute/path/to/certs/nomad-agent-ca.pem",
                        "default": "",
                    },
                    "default_job_template": {
                        "description": "Specific .hcl or .json template to use for job submission, instead of in-built defaults",
                        "version_added": "0.0.1",
                        "type": "string",
                        "example": "/absolute/path/to/job_template.{json,hcl}",
                        "default": "",
                    },
                    "default_docker_image": {
                        "description": "Default Docker image for the default job template",
                        "version_added": "0.0.2",
                        "type": "string",
                        "example": "python:latest",
                        "default": "novakjudit/airflow-nomad-runner:latest",
                    },
                    "alloc_pending_timeout": {
                        "description": "Timeout in seconds before failed allocations may be considered as failed jobs",
                        "version_added": "0.0.1",
                        "type": "integer",
                        "example": "600",
                        "default": "600",
                    },
                    "operator_poll_delay": {
                        "description": "Time delay for Nomad Opeators supervision cycle, to check on child Nomad job",
                        "version_added": "0.0.2",
                        "type": "integer",
                        "example": "5",
                        "default": "10",
                    },
                },
            },
        },
    }
