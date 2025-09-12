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

from airflow.providers.nomad.models import NomadJobModel
from airflow.providers.nomad.nomad_manager import NomadManager


def test_nomad_job_operator_parse_template_hcl(test_datadir):
    mgr = NomadManager()
    mgr.initialize()

    file_path1 = test_datadir / "simple_batch.hcl"
    hcl_content = open(file_path1).read()

    dict_content = mgr.nomad.jobs.parse(hcl_content)  # type: ignore[optionalMemberAccess, union-attr]
    dict_content = {"Job": dict_content}
    assert NomadJobModel.model_validate_json(
        json.dumps(dict_content)
    ) == mgr.parse_template_content(hcl_content)
    assert NomadJobModel.model_validate_json(json.dumps(dict_content)) == mgr.parse_template_hcl(
        hcl_content
    )
