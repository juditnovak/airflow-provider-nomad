import json
import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from airflow.sdk import Context
from nomad.api.exceptions import BaseNomadException  # type: ignore[import-untyped]
from tests_common.test_utils.config import conf_vars

from airflow.providers.nomad.exceptions import (
    NomadOperatorError,
    NomadProviderException,
    NomadValidationError,
)
from airflow.providers.nomad.models import (
    NomadJobModel,
    NomadVolumeMounts,
    Resource,
    NomadEphemeralDisk,
)
from airflow.providers.nomad.operators.task import NomadTaskOperator
from airflow.providers.nomad.templates.job_template import default_image


@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_nomad_task_operator_execute_ok(filename, test_datadir, mock_nomad_client):
    file_path = test_datadir / filename
    content = open(file_path).read()

    # The outer job will be running happily, the inner one fails
    file_path2 = test_datadir / "nomad_job_info_dead.json"
    mock_nomad_client.job.get_job.side_effect = [None, json.loads(open(file_path2).read())]

    file_path3 = test_datadir / "nomad_job_allocations.json"
    file_path4 = test_datadir / "nomad_job_summary_success.json"
    file_path5 = test_datadir / "nomad_job_evaluation.json"
    mock_nomad_client.job.get_allocations.return_value = json.loads(open(file_path3).read())
    mock_nomad_client.job.get_summary.return_value = json.loads(open(file_path4).read())
    mock_nomad_client.job.get_evaluations.return_value = json.loads(open(file_path5).read())
    # The first read is on stderr, second is on stdout
    mock_nomad_client.client.cat.read_file.side_effect = [str({"Summary": 30}), ""]

    mock_job_register = mock_nomad_client.job.register_job
    runtime_ti = MagicMock(
        task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
    )
    context = Context({"params": {"template_content": content}, "ti": runtime_ti})

    op = NomadTaskOperator(task_id="task_id")
    retval = op.execute(context)

    job_id = op.template.Job.ID  # type: ignore [reportOptionalMemberAccess]
    template = NomadJobModel.model_validate_json(content).model_dump(exclude_unset=True)
    template["Job"]["ID"] = job_id
    mock_job_register.assert_called_once_with(job_id, template)
    assert retval == str({"Summary": 30})


@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_nomad_task_operator_execute_ok_with_task_logs(
    filename, test_datadir, mock_nomad_client, caplog
):
    file_path = test_datadir / filename
    content = open(file_path).read()

    # The outer job will be running happily, the inner one fails
    file_path2 = test_datadir / "nomad_job_info_dead.json"
    mock_nomad_client.job.get_job.side_effect = [None, json.loads(open(file_path2).read())]

    file_path3 = test_datadir / "nomad_job_allocations.json"
    file_path4 = test_datadir / "nomad_job_summary_success.json"
    file_path5 = test_datadir / "nomad_job_evaluation.json"
    mock_nomad_client.job.get_allocations.return_value = json.loads(open(file_path3).read())
    mock_nomad_client.job.get_summary.return_value = json.loads(open(file_path4).read())
    mock_nomad_client.job.get_evaluations.return_value = json.loads(open(file_path5).read())
    # The first read is on stderr, second is on stdout
    job_log = "Submitted task is saying hello"
    job_err = "Minor error"
    mock_nomad_client.client.cat.read_file.side_effect = [job_log, str({"Summary": 30}), job_err]

    mock_job_register = mock_nomad_client.job.register_job
    runtime_ti = MagicMock(
        task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
    )
    context = Context({"params": {"template_content": content}, "ti": runtime_ti})

    op = NomadTaskOperator(task_id="task_id", job_log_file="locallog.out")
    with caplog.at_level(logging.INFO):
        retval = op.execute(context)

        job_id = op.template.Job.ID  # type: ignore [reportOptionalMemberAccess]
        template = NomadJobModel.model_validate_json(content).model_dump(exclude_unset=True)
        template["Job"]["ID"] = job_id
        mock_job_register.assert_called_once_with(job_id, template)
        assert retval == str({"Summary": 30})
        assert any([job_log in record.message for record in caplog.records])
        assert any([job_err in record.message for record in caplog.records])


def test_nomad_task_operator_template_multi_task(test_datadir):
    file_path = test_datadir / "batch_multi_task.json"
    content = open(file_path).read()

    runtime_ti = MagicMock(
        task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
    )
    context = Context({"params": {"template_content": content}, "ti": runtime_ti})

    op = NomadTaskOperator(task_id="task_id")
    with pytest.raises(NomadValidationError) as err:
        op.execute(context)

    assert str(err.value) == "NomadTaskOperator only allows for a single task"


def test_nomad_task_operator_template_multi_tg(test_datadir):
    file_path = test_datadir / "batch_multi_tg.json"
    content = open(file_path).read()

    runtime_ti = MagicMock(
        task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
    )
    context = Context({"params": {"template_content": content}, "ti": runtime_ti})

    op = NomadTaskOperator(task_id="task_id")
    with pytest.raises(NomadValidationError) as err:
        op.execute(context)

    assert str(err.value) == "NomadTaskOperator only allows for a single taskgroup"


def test_nomad_task_operator_template_multi_count(test_datadir):
    file_path = test_datadir / "batch_multi_count.json"
    content = open(file_path).read()

    runtime_ti = MagicMock(
        task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
    )
    context = Context({"params": {"template_content": content}, "ti": runtime_ti})

    op = NomadTaskOperator(task_id="task_id")
    with pytest.raises(NomadValidationError) as err:
        op.execute(context)

    assert str(err.value) == "Only a single execution is allowed (count=1)"


@conf_vars({("nomad_provider", "alloc_pending_timeout"): "0"})
@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_nomad_task_operator_execute_job_submission_fails(
    filename, test_datadir, mock_nomad_client
):
    file_path = test_datadir / filename
    content = open(file_path).read()

    mock_job_register = mock_nomad_client.job.register_job
    mock_job_register.side_effect = BaseNomadException("Job submission error")
    runtime_ti = MagicMock(
        task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
    )
    context = Context({"params": {"template_content": content}, "ti": runtime_ti})

    # Job output
    mock_nomad_client.client.cat.read_file.side_effect = ["", ""]

    op = NomadTaskOperator(task_id="task_id")
    with pytest.raises(NomadOperatorError) as err:
        op.execute(context)

    job_id = op.template.Job.ID  # type: ignore [reportOptionalMemberAccess]
    template = NomadJobModel.model_validate_json(content).model_dump(exclude_unset=True)
    template["Job"]["ID"] = job_id
    mock_job_register.assert_called_once_with(job_id, template)
    assert str(err.value).startswith("Job submission failed")


@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_nomad_task_operator_execute_failed(filename, test_datadir, mock_nomad_client):
    file_path = test_datadir / filename
    content = open(file_path).read()

    # The outer job will be running happily, the inner one fails
    file_path2 = test_datadir / "nomad_job_info_dead.json"
    file_path3 = test_datadir / "nomad_job_allocations_pending.json"
    file_path4 = test_datadir / "nomad_job_summary_failed.json"
    mock_nomad_client.job.get_job.return_Value = [None, json.loads(open(file_path2).read())]
    mock_nomad_client.job.get_allocations.return_value = json.loads(open(file_path3).read())
    mock_nomad_client.job.get_summary.return_value = json.loads(open(file_path4).read())
    # Job output
    mock_nomad_client.client.cat.read_file.side_effect = ["", ""]

    mock_job_register = mock_nomad_client.job.register_job
    runtime_ti = MagicMock(
        task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
    )
    context = Context({"params": {"template_content": content}, "ti": runtime_ti})

    op = NomadTaskOperator(task_id="task_id")
    with pytest.raises(NomadOperatorError) as err:
        op.execute(context)

    job_id = op.template.Job.ID  # type: ignore [reportOptionalMemberAccess]
    template = NomadJobModel.model_validate_json(content).model_dump(exclude_unset=True)
    template["Job"]["ID"] = job_id
    mock_job_register.assert_called_once_with(job_id, template)
    # assert str(err.value).startswith(f"Job summary:Job {job_id} got killed due to error")
    assert "Error response from daemon: pull access denied for novakjudi/af_nomad_test" in str(
        err.value
    )


def test_sanitize_logs():
    file_path = Path("/tmp/alloc_id-task_name.log")
    op = NomadTaskOperator(task_id="task_id")
    try:
        log_content = "log\nfile\ncontent"
        logs = log_content
        assert log_content == op.sanitize_logs("alloc_id", "task_name", log_content)  # type: ignore [reportAttributeAccessIssue]
        assert file_path.is_file()

        more_log_content = "more\nlog\ncontent"
        logs += more_log_content
        assert more_log_content == op.sanitize_logs("alloc_id", "task_name", logs)  # type: ignore [reportAttributeAccessIssue]

        even_more_log_content = "even\nmuch\nmore\nlog\ncontent"
        logs += even_more_log_content
        assert even_more_log_content == op.sanitize_logs("alloc_id", "task_name", logs)  # type: ignore [reportAttributeAccessIssue]
    finally:
        file_path.unlink()


@conf_vars({("nomad_provider", "runner_log_dir"): "./"})
def test_sanitize_logs_log_dir_param():
    file_path = Path("alloc_id-task_name.log")
    op = NomadTaskOperator(task_id="task_id")
    try:
        log_content = "log\nfile\ncontent"
        logs = log_content
        assert log_content == op.sanitize_logs("alloc_id", "task_name", log_content)  # type: ignore [reportAttributeAccessIssue]
        assert file_path.is_file()

        more_log_content = "more\nlog\ncontent"
        logs += more_log_content
        assert more_log_content == op.sanitize_logs("alloc_id", "task_name", logs)  # type: ignore [reportAttributeAccessIssue]

        even_more_log_content = "even\nmuch\nmore\nlog\ncontent"
        logs += even_more_log_content
        assert even_more_log_content == op.sanitize_logs("alloc_id", "task_name", logs)  # type: ignore [reportAttributeAccessIssue]
    finally:
        file_path.unlink()


def test_params():
    op = NomadTaskOperator(task_id="task_id")

    context = Context(
        {
            "params": {
                "image": "alpine:3.21",
                "entrypoint": ["/bin/sh", "-c"],
                "args": ["date"],
                "env": {"TEMPDIR": "$HOME/tmp"},
            },
            "ti": MagicMock(
                task_id="task_op_test",
                dag_id="task_op_dag",
                run_id="run_id",
                try_number=1,
                map_index=-1,
            ),
        }
    )
    op.prepare_job_template(context)
    assert op.template
    assert op.template.Job.TaskGroups[0].Tasks[0].Config.entrypoint == ["/bin/sh", "-c"]
    assert op.template.Job.TaskGroups[0].Tasks[0].Config.image == "alpine:3.21"
    assert op.template.Job.TaskGroups[0].Tasks[0].Config.args == ["date"]
    assert op.template.Job.TaskGroups[0].Tasks[0].Env == {"TEMPDIR": "$HOME/tmp"}
    assert len(op.template.Job.TaskGroups[0].Tasks[0].Config.model_dump(exclude_unset=True)) == 3


def test_params_defaults():
    op = NomadTaskOperator(task_id="task_id")

    context = Context(
        {
            "params": {"args": ["date"]},
            "ti": MagicMock(
                task_id="task_op_test",
                dag_id="task_op_dag",
                run_id="run_id",
                try_number=1,
                map_index=-1,
            ),
        }
    )
    op.prepare_job_template(context)
    assert op.template
    assert op.template.Job.TaskGroups[0].Tasks[0].Config.image == default_image
    assert op.template.Job.TaskGroups[0].Tasks[0].Config.args == ["date"]
    assert len(op.template.Job.TaskGroups[0].Tasks[0].Config.model_dump(exclude_unset=True)) == 2


def test_args_params_priority():
    op = NomadTaskOperator(
        task_id="task_id",
        image="image",
        args=["arg1", "arg2"],
        env={"ENV_VAR1": "value1", "ENV_VAR2": "value2"},
    )

    context = Context(
        {
            "params": {"image": "unusued_img", "args": ["unused_arg"]},
            "ti": MagicMock(
                task_id="task_op_test",
                dag_id="task_op_dag",
                run_id="run_id",
                try_number=1,
                map_index=-1,
            ),
        }
    )
    op.prepare_job_template(context)
    assert op.template
    assert op.template.Job.TaskGroups[0].Tasks[0].Config.image == "image"
    assert op.template.Job.TaskGroups[0].Tasks[0].Config.args == ["arg1", "arg2"]
    assert op.template.Job.TaskGroups[0].Tasks[0].Env == {
        "ENV_VAR1": "value1",
        "ENV_VAR2": "value2",
    }


@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_args_template_content(filename, test_datadir):
    file_path = test_datadir / filename
    content = open(file_path).read()

    op = NomadTaskOperator(task_id="task_id", template_content=content)
    context = Context(
        {
            "ti": MagicMock(
                task_id="task_op_test",
                dag_id="task_op_dag",
                run_id="run_id",
                try_number=1,
                map_index=-1,
            )
        }
    )
    op.prepare_job_template(context)
    assert op.template
    assert op.template.Job.TaskGroups == NomadJobModel.model_validate_json(content).Job.TaskGroups
    assert op.template.Job.ID != NomadJobModel.model_validate_json(content).Job.ID


@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_args_template_path(filename, test_datadir):
    file_path = test_datadir / filename
    content = open(file_path).read()

    op = NomadTaskOperator(task_id="task_id", template_path=file_path)
    context = Context(
        {
            "ti": MagicMock(
                task_id="task_op_test",
                dag_id="task_op_dag",
                run_id="run_id",
                try_number=1,
                map_index=-1,
            )
        }
    )
    op.prepare_job_template(context)
    assert op.template
    assert op.template.Job.TaskGroups == NomadJobModel.model_validate_json(content).Job.TaskGroups
    assert op.template.Job.ID != NomadJobModel.model_validate_json(content).Job.ID


@pytest.mark.parametrize(
    "param, value",
    [("args", ["arg1", "arg2"]), ("command", "cmd"), ("entrypoint", ["/bin/sh", "-c"])],
)
def test_args_args(param, value):
    op = NomadTaskOperator(task_id="task_id", **{param: value})
    context = Context(
        {
            "ti": MagicMock(
                task_id="task_op_test",
                dag_id="task_op_dag",
                run_id="run_id",
                try_number=1,
                map_index=-1,
            )
        }
    )
    op.prepare_job_template(context)
    assert op.template
    assert getattr(op.template.Job.TaskGroups[0].Tasks[0].Config, param) == value


@pytest.mark.parametrize(
    "paramdict", [{"args": "badarg"}, {"command": ["cmd"]}, {"entrypoint": "bla"}]
)
def test_args_args_invalid(paramdict):
    op = NomadTaskOperator(task_id="task_id", **paramdict)  # type: ignore [reportArgumentType]
    with pytest.raises(NomadProviderException):
        op.prepare_job_template({})


def test_args_conflict():
    op = NomadTaskOperator(task_id="task_id", entrypoint=["/bin/sh", "-c"], command="uptime")

    with pytest.raises(NomadValidationError) as err:
        op.prepare_job_template({})
    assert "Both 'entrypoint' and 'command' specified" in str(err.value)


def test_args_template_path_invalid(caplog):
    op = NomadTaskOperator(task_id="task_id", template_path="bla")
    with caplog.at_level(logging.ERROR):
        with pytest.raises(NomadProviderException) as err:
            assert not op.prepare_job_template({})
    assert "No such file or directory:" in caplog.text

    with pytest.raises(NomadProviderException) as err:
        op.execute({})
    assert "Nothing to execute" in str(err.value)


def test_args_template_content_invalid(caplog):
    op = NomadTaskOperator(task_id="task_id", template_content="bla")
    with caplog.at_level(logging.ERROR):
        with pytest.raises(NomadProviderException) as err:
            op.prepare_job_template({})
    assert "Couldn't parse template 'bla'" in caplog.text
    assert "Nothing to execute" in str(err.value)


def test_args_invalid_template():
    with pytest.raises(ValueError) as err:
        NomadTaskOperator(task_id="task_id", template_path="/some/path", template_content="<HCL>")
    assert "Only one of 'template_content' and 'template_path' can be specified" in str(err.value)


def test_args_invalid_env():
    op = NomadTaskOperator(
        task_id="task_id",
        command="date",
        env="TZ: 'Europe/Paris'",  # type: ignore [reportArgumentType]
    )

    with pytest.raises(NomadValidationError) as err:
        op.prepare_job_template({})
    assert "Input should be a valid dictionary" in str(err.value)


def test_args_task_ephemeral():
    edisk_data = {
        "Size": 200,
        "Migrate": True,
    }
    context = Context(
        {
            "ti": MagicMock(
                task_id="task_op_test",
                dag_id="task_op_dag",
                run_id="run_id",
                try_number=1,
                map_index=-1,
            )
        }
    )
    op = NomadTaskOperator(task_id="task_id", ephemeral_disk=edisk_data)
    op.prepare_job_template(context)
    assert op.template
    assert op.template.Job.TaskGroups[0].EphemeralDisk
    assert op.template.Job.TaskGroups[0].EphemeralDisk == NomadEphemeralDisk.model_validate(
        edisk_data
    )


def test_args_task_resources1():
    res_data = {
        "MemoryMB": 600,
        "DiskMB": 800,
        "Cores": 8,
        "MemoryMaxMB": 800,
    }
    context = Context(
        {
            "ti": MagicMock(
                task_id="task_op_test",
                dag_id="task_op_dag",
                run_id="run_id",
                try_number=1,
                map_index=-1,
            )
        }
    )
    op = NomadTaskOperator(task_id="task_id", task_resources=res_data)
    op.prepare_job_template(context)
    assert op.template
    assert op.template.Job.TaskGroups[0].Tasks[0].Resources == Resource.model_validate(res_data)


def test_args_task_resources2(caplog):
    res_data = {
        "CPU": 3000,  # MHz
        "MemoryMB": 600,
        "DiskMB": 800,
        "MemoryMaxMB": 800,
        "Networks": ["192.168.1.0"],
        "Devices": ["GPU", "tape"],
        "MBits": 2500,
    }
    context = Context(
        {
            "ti": MagicMock(
                task_id="task_op_test",
                dag_id="task_op_dag",
                run_id="run_id",
                try_number=1,
                map_index=-1,
            )
        }
    )
    op = NomadTaskOperator(task_id="task_id", task_resources=res_data)

    with caplog.at_level(logging.WARNING):
        op.prepare_job_template(context)

    assert op.template
    assert op.template.Job.TaskGroups[0].Tasks[0].Resources == Resource.model_validate(res_data)

    assert op.template.Job.TaskGroups[0].Tasks[0].Resources.DiskMB is None
    assert "DiskMB is not supported" in caplog.text


def test_args_task_resources_defaults():
    res_data = {
        "MemoryMB": 600,
    }
    context = Context(
        {
            "ti": MagicMock(
                task_id="task_op_test",
                dag_id="task_op_dag",
                run_id="run_id",
                try_number=1,
                map_index=-1,
            )
        }
    )
    op = NomadTaskOperator(task_id="task_id", task_resources=res_data)
    op.prepare_job_template(context)

    assert op.template
    assert op.template.Job.TaskGroups[0].Tasks[0].Resources
    assert op.template.Job.TaskGroups[0].Tasks[0].Resources == Resource.model_validate(res_data)
    assert op.template.Job.TaskGroups[0].Tasks[0].Resources.CPU is None
    assert op.template.Job.TaskGroups[0].Tasks[0].Resources.Cores is None
    assert op.template.Job.TaskGroups[0].Tasks[0].Resources.MemoryMB == 600


def test_args_task_volumes():
    vol_data = {
        "test_cfg": {
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
        "test_dags_folder": {
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
        "test_logs_dest": {
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
    }
    vol_mounts_data = [
        {
            "Destination": "/opt/airflow/config",
            "PropagationMode": "private",
            "ReadOnly": True,
            "SELinuxLabel": "",
            "Volume": "test_cfg",
        },
        {
            "Destination": "/opt/airflow/dags",
            "PropagationMode": "private",
            "ReadOnly": True,
            "SELinuxLabel": "",
            "Volume": "test_dags_folder",
        },
        {
            "Destination": "/opt/airflow/logs",
            "PropagationMode": "private",
            "ReadOnly": False,
            "SELinuxLabel": "",
            "Volume": "test_logs_dest",
        },
    ]
    context = Context(
        {
            "ti": MagicMock(
                task_id="task_op_test",
                dag_id="task_op_dag",
                run_id="run_id",
                try_number=1,
                map_index=-1,
            )
        }
    )
    op = NomadTaskOperator(task_id="task_id", volume_mounts=vol_mounts_data, volumes=vol_data)
    op.prepare_job_template(context)
    assert op.template
    assert op.template.Job.TaskGroups[0].Tasks[0].VolumeMounts == NomadVolumeMounts.validate_python(
        vol_mounts_data
    )
