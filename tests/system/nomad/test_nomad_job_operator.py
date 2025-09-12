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

from airflow.providers.nomad.operators.nomad_job import NomadJobOperator
from airflow.sdk import Context


def test_nomad_job_operator_execute(test_datadir):
    file_path = test_datadir / "simple_batch.hcl"
    content = open(file_path).read()

    op = NomadJobOperator(task_id="task_id")

    context = Context({"params": {"template_content": content}})

    op.execute(context)

    assert op.nomad_mgr.nomad.job.get_summary("simple-job-template")  # type: ignore[optionalMemberAccess, union-attr]
