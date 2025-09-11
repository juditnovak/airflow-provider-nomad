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

from airflow.providers.nomad.models import (
    NomadJobAllocations,
    NomadJobEvaluation,
    NomadJobModel,
    NomadJobSubmission,
    NomadJobSummary,
)


def test_parse_nomad_job(test_datadir):
    file_path = test_datadir / "nomad_provider_job_template.json"
    assert NomadJobModel.model_validate(json.loads(open(file_path).read()))


def test_parse_nomad_job_evaluation(test_datadir):
    file_path = test_datadir / "nomad_job_evaluation_failed.json"
    evaluations = NomadJobEvaluation.validate_python(json.loads(open(file_path).read()))
    assert evaluations

    eval_errors = [
        evaluation.FailedTGAllocs["airflow-execution-taskgroup"].errors()
        for evaluation in evaluations
        if evaluation.FailedTGAllocs
    ]
    assert eval_errors == [
        ["{'missing compatible host volumes': 1}"],
    ]


def test_parse_nomad_job_info(test_datadir):
    file_path = test_datadir / "nomad_job_info.json"
    assert NomadJobSubmission.model_validate(json.loads(open(file_path).read()))


def test_parse_nomad_job_allocations(test_datadir):
    file_path = test_datadir / "nomad_job_allocations_pending.json"

    allocations = NomadJobAllocations.validate_python(json.loads(open(file_path).read()))
    assert allocations
    assert [allocation.errors() for allocation in allocations] == [
        {
            "example_bash_operator_judit_new-runme_0": {
                "Driver Failure": [
                    "Failed to pull `novakjudi/af_nomad_test:latest`: "
                    "Error response from daemon: pull access denied for novakjudi/af_nomad_test, "
                    "repository does not exist or may require 'docker login': denied: requested access to the resource is denied",
                    "Failed to pull `novakjudi/af_nomad_test:latest`: Error response from daemon: "
                    "pull access denied for novakjudi/af_nomad_test, repository does not exist or may require 'docker login': denied: "
                    "requested access to the resource is denied",
                ],
            },
        }
    ]


def test_parse_nomad_job_summary():
    summary_data = {
        "Children": {"Dead": 0, "Pending": 0, "Running": 0},
        "CreateIndex": 324,
        "JobID": "example_bash_operator_judit_new-runme_2-manual__2025-09-10T09:01:15.900701+00:00-7--1",
        "ModifyIndex": 351,
        "Namespace": "default",
        "Summary": {
            "airflow-execution-taskgroup": {
                "Complete": 0,
                "Failed": 2,
                "Lost": 0,
                "Queued": 0,
                "Running": 0,
                "Starting": 0,
                "Unknown": 0,
            }
        },
    }

    summary = NomadJobSummary.model_validate(summary_data)
    assert summary
    assert summary.all_failed()

    summary_data["Summary"]["airflow-execution-taskgroup"]["Running"] = 1
    summary = NomadJobSummary.model_validate(summary_data)
    assert summary
    assert not summary.all_failed()
