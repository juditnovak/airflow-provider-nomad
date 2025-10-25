import json

from airflow.providers.nomad.models import (
    NomadEphemeralDisk,
    NomadJobAllocations,
    NomadJobEvaluation,
    NomadJobModel,
    NomadJobSubmission,
    NomadJobSummary,
    NomadVolumeMounts,
    NomadVolumes,
)


def test_parse_nomad_job(test_datadir):
    file_path = test_datadir / "nomad_provider_job_template.json"
    job_model = NomadJobModel.model_validate(json.loads(open(file_path).read()))
    assert job_model
    assert job_model.tasknames() == ["airflow-task-1"]


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


def test_ephemeral():
    edisk_data = {"SizeMB": 200, "Migrate": True, "Sticky": True}
    edisk = NomadEphemeralDisk(**edisk_data)
    assert edisk.SizeMB == 200
    assert edisk.Migrate is True
    assert edisk.Sticky is True


def test_ephemeral_defaults():
    edisk = NomadEphemeralDisk()
    assert edisk.SizeMB is None
    assert edisk.Migrate is None
    assert edisk.Sticky is None


def test_volumes():
    volumes = {
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
    }

    nomad_volumes = NomadVolumes.validate_python(volumes)
    assert list(nomad_volumes) == ["config", "dags"]


def test_volume_mounts():
    volumes_data = {
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
    }

    volume_mounts_data = [
        {
            "Destination": "/opt/airflow/config",
            "PropagationMode": "private",
            "ReadOnly": False,
            "SELinuxLabel": "",
            "Volume": "config",
        },
        {
            "Destination": "/opt/airflow/dags",
            "PropagationMode": "private",
            "ReadOnly": False,
            "SELinuxLabel": "",
            "Volume": "dags",
        },
    ]

    volumes = NomadVolumes.validate_python(volumes_data)
    volume_mounts = NomadVolumeMounts.validate_python(volume_mounts_data)
    assert len(volume_mounts) == 2
    assert set([x.Volume for x in volume_mounts]) == set(["config", "dags"])
    assert all(x.from_volumes(volumes) for x in volume_mounts)


def test_volume_mounts_fails():
    volumes_data = {
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
    }

    volume_mounts_data = [
        {
            "Destination": "/opt/airflow/config",
            "PropagationMode": "private",
            "ReadOnly": False,
            "SELinuxLabel": "",
            "Volume": "non-existent",
        },
    ]

    volumes = NomadVolumes.validate_python(volumes_data)
    volume_mounts = NomadVolumeMounts.validate_python(volume_mounts_data)
    assert not volume_mounts[0].from_volumes(volumes)
