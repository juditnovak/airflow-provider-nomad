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

import json
from pathlib import Path
from typing import Any

import jq  # type: ignore
import requests  # type: ignore[import-untyped]
from pydantic import ValidationError

from airflow.providers.nomad.exceptions import NomadValidationError
from airflow.providers.nomad.models import NomadJobModel


def validate_nomad_job(data: dict[str, Any]) -> NomadJobModel:
    try:
        return NomadJobModel.model_validate(data)
    except ValidationError as err:
        raise NomadValidationError(err)


def parse_json_job_template(path: Path) -> NomadJobModel | None:
    """try to parse a json or hcl input as a nomad job template"""
    try:
        data = json.load(open(str(path)))
    except json.JSONDecodeError:
        return None
    return validate_nomad_job(data)


def parse_hcl_job_template(nomad_url: str, path: Path) -> NomadJobModel | None:
    content = open(path).read()
    payload = jq.compile("{ JobHCL: ., Canonicalize: true }").input_value(content).first()
    response = requests.post(nomad_url + "/v1/jobs/parse", data=json.dumps(payload))
    if response.status_code != 200:
        raise NomadValidationError(response.text)

    data = response.json()
    data = {"Job": data}

    return validate_nomad_job(data)
