import json
from pathlib import Path
import logging
from unittest.mock import MagicMock

import pytest

from nomad.api.exceptions import BaseNomadException  # type: ignore[import-untyped]

from airflow.sdk import Context
from airflow.providers.nomad.models import (
    NomadJobModel,
    NomadEphemeralDisk,
    Resource,
    NomadVolumeMounts,
)
from airflow.providers.nomad.exceptions import (
    NomadOperatorError,
    NomadProviderException,
    NomadValidationError,
)
from airflow.providers.nomad.operators.python import NomadPythonTaskOperator
from airflow.providers.nomad.templates.job_template import DEFAULT_IMAGE

from tests_common.test_utils.config import conf_vars


@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_nomad_python_task_operator_execute_ok(filename, test_datadir, mock_nomad_client):
    file_path = test_datadir / filename
    content = open(file_path).read()
    python_command = "print('Python job here!'"

    ########## preparation
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
    ########## \preparation

    mock_job_register = mock_nomad_client.job.register_job
    runtime_ti = MagicMock(
        task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
    )
    context = Context({"params": {"template_content": content}, "ti": runtime_ti})

    op = NomadPythonTaskOperator(task_id="task_id", python_command=python_command)
    retval = op.execute(context)

    job_id = op.template.Job.ID  # type: ignore [reportOptionalMemberAccess]
    template = NomadJobModel.model_validate_json(content).model_dump(exclude_unset=True)
    template["Job"]["ID"] = job_id
    template["Job"]["TaskGroups"][0]["Tasks"][0]["Config"]["args"] = [python_command]
    mock_job_register.assert_called_once_with(job_id, template)
    assert retval == str({"Summary": 30})


@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_nomad_python_task_operator_execute_ok_with_task_logs(
    filename, test_datadir, mock_nomad_client, caplog
):
    file_path = test_datadir / filename
    content = open(file_path).read()
    python_command = "print('Python job here!'"

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

    op = NomadPythonTaskOperator(
        task_id="task_id", job_log_file="locallog.out", python_command=python_command
    )
    with caplog.at_level(logging.INFO):
        retval = op.execute(context)

        job_id = op.template.Job.ID  # type: ignore [reportOptionalMemberAccess]
        template = NomadJobModel.model_validate_json(content).model_dump(exclude_unset=True)
        template["Job"]["ID"] = job_id
        template["Job"]["TaskGroups"][0]["Tasks"][0]["Config"]["args"] = [python_command]
        mock_job_register.assert_called_once_with(job_id, template)
        assert retval == str({"Summary": 30})
        assert any([job_log in record.message for record in caplog.records])
        assert any([job_err in record.message for record in caplog.records])


def test_nomad_python_task_operator_template_multi_task(test_datadir):
    file_path = test_datadir / "batch_multi_task.json"
    content = open(file_path).read()
    python_command = "print('Python job here!'"

    runtime_ti = MagicMock(
        task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
    )
    context = Context({"params": {"template_content": content}, "ti": runtime_ti})

    op = NomadPythonTaskOperator(task_id="task_id", python_command=python_command)
    with pytest.raises(NomadValidationError) as err:
        op.execute(context)

    assert str(err.value) == "Nomad Task Operators/Decorators only allows for a single task"


def test_nomad_python_task_operator_template_multi_tg(test_datadir):
    file_path = test_datadir / "batch_multi_tg.json"
    content = open(file_path).read()
    python_command = "print('Python job here!'"

    runtime_ti = MagicMock(
        task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
    )
    context = Context({"params": {"template_content": content}, "ti": runtime_ti})

    op = NomadPythonTaskOperator(task_id="task_id", python_command=python_command)
    with pytest.raises(NomadValidationError) as err:
        op.execute(context)

    assert str(err.value) == "Nomad Task Operators/Decorators only allows for a single taskgroup"


def test_nomad_python_task_operator_template_multi_count(test_datadir):
    file_path = test_datadir / "batch_multi_count.json"
    content = open(file_path).read()
    python_command = "print('Python job here!'"

    runtime_ti = MagicMock(
        task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
    )
    context = Context({"params": {"template_content": content}, "ti": runtime_ti})

    op = NomadPythonTaskOperator(task_id="task_id", python_command=python_command)
    with pytest.raises(NomadValidationError) as err:
        op.execute(context)

    assert str(err.value) == "Only a single execution is allowed (count=1)"


@conf_vars({("nomad_provider", "alloc_pending_timeout"): "0"})
@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_nomad_python_task_operator_execute_job_submission_fails(
    filename, test_datadir, mock_nomad_client
):
    file_path = test_datadir / filename
    content = open(file_path).read()
    python_command = "print('Python job here!'"

    mock_job_register = mock_nomad_client.job.register_job
    mock_job_register.side_effect = BaseNomadException("Job submission error")
    runtime_ti = MagicMock(
        task_id="task_id", dag_id="dag_id", run_id="run_id", try_number=1, map_index=-1
    )
    context = Context({"params": {"template_content": content}, "ti": runtime_ti})

    # Job output
    mock_nomad_client.client.cat.read_file.side_effect = ["", ""]

    op = NomadPythonTaskOperator(task_id="task_id", python_command=python_command)
    with pytest.raises(NomadOperatorError) as err:
        op.execute(context)

    job_id = op.template.Job.ID  # type: ignore [reportOptionalMemberAccess]
    template = NomadJobModel.model_validate_json(content).model_dump(exclude_unset=True)
    template["Job"]["ID"] = job_id
    template["Job"]["TaskGroups"][0]["Tasks"][0]["Config"]["args"] = [python_command]
    mock_job_register.assert_called_once_with(job_id, template)
    assert str(err.value).startswith("Job submission failed")


@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_nomad_python_task_operator_execute_failed(filename, test_datadir, mock_nomad_client):
    file_path = test_datadir / filename
    content = open(file_path).read()
    python_command = "print('Python job here!'"

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

    op = NomadPythonTaskOperator(task_id="task_id", python_command=python_command)
    with pytest.raises(NomadOperatorError) as err:
        op.execute(context)

    job_id = op.template.Job.ID  # type: ignore [reportOptionalMemberAccess]
    template = NomadJobModel.model_validate_json(content).model_dump(exclude_unset=True)
    template["Job"]["ID"] = job_id
    template["Job"]["TaskGroups"][0]["Tasks"][0]["Config"]["args"] = [python_command]
    mock_job_register.assert_called_once_with(job_id, template)
    # assert str(err.value).startswith(f"Job summary:Job {job_id} got killed due to error")
    assert "Error response from daemon: pull access denied for novakjudi/af_nomad_test" in str(
        err.value
    )


def test_sanitize_logs():
    python_command = "print('Python job here!'"
    file_path = Path("/tmp/alloc_id-task_name.log")
    op = NomadPythonTaskOperator(task_id="task_id", python_command=python_command)
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
    python_command = "print('Python job here!'"
    file_path = Path("alloc_id-task_name.log")
    op = NomadPythonTaskOperator(task_id="task_id", python_command=python_command)
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
    python_command = "print('Python job here!'"
    op = NomadPythonTaskOperator(task_id="task_id", python_command=python_command)

    context = Context(
        {
            "params": {
                "image": "python:3.12-alpine",
                "entrypoint": ["python", "-c"],
                "args": ["print('hello')\n"],
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
    assert op.template.Job.TaskGroups[0].Tasks[0].Config.entrypoint == ["python", "-c"]
    assert op.template.Job.TaskGroups[0].Tasks[0].Config.image == "python:3.12-alpine"
    assert op.template.Job.TaskGroups[0].Tasks[0].Config.args == [
        "print('hello')\n",
        python_command,
    ]
    assert op.template.Job.TaskGroups[0].Tasks[0].Env == {
        "TEMPDIR": "$HOME/tmp",
        # From the job definition
        "AIRFLOW_CONFIG": "/opt/airflow/config/airflow.cfg",
        "AIRFLOW_HOME": "/opt/airflow/",
    }
    assert len(op.template.Job.TaskGroups[0].Tasks[0].Config.model_dump(exclude_unset=True)) == 3


def test_params_defaults():
    python_command = "print('Python job here!'"
    op = NomadPythonTaskOperator(task_id="task_id", python_command=python_command)

    context = Context(
        {
            "params": {"args": ["print('hello')\n"]},
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
    assert op.template.Job.TaskGroups[0].Tasks[0].Config.image == DEFAULT_IMAGE
    assert op.template.Job.TaskGroups[0].Tasks[0].Config.args == [
        "print('hello')\n",
        python_command,
    ]
    assert len(op.template.Job.TaskGroups[0].Tasks[0].Config.model_dump(exclude_unset=True)) == 3


def test_args_params_priority():
    python_command = "print('Python job here!'"
    op = NomadPythonTaskOperator(
        task_id="task_id",
        image="image",
        args=["print('Hello')\n", "print('World')'\n"],
        env={
            "ENV_VAR1": "value1",
            "ENV_VAR2": "value2",
        },
        python_command=python_command,
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
    assert op.template.Job.TaskGroups[0].Tasks[0].Config.args == [
        "print('Hello')\n",
        "print('World')'\n",
        python_command,
    ]
    assert op.template.Job.TaskGroups[0].Tasks[0].Env == {
        "ENV_VAR1": "value1",
        "ENV_VAR2": "value2",
        # From the job definition
        "AIRFLOW_CONFIG": "/opt/airflow/config/airflow.cfg",
        "AIRFLOW_HOME": "/opt/airflow/",
    }


@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_args_template_content(filename, test_datadir):
    file_path = test_datadir / filename
    content = open(file_path).read()
    python_command = "print('Python job here!'"

    op = NomadPythonTaskOperator(
        task_id="task_id", template_content=content, python_command=python_command
    )
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

    file_taskgroups = NomadJobModel.model_validate_json(content).Job.TaskGroups
    file_taskgroups[0].Tasks[0].Config.args = [python_command]

    op.prepare_job_template(context)
    assert op.template
    assert op.template.Job.TaskGroups == file_taskgroups
    assert op.template.Job.ID != NomadJobModel.model_validate_json(content).Job.ID


@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_args_template_path(filename, test_datadir):
    file_path = test_datadir / filename
    content = open(file_path).read()
    python_command = "print('Python job here!'"

    op = NomadPythonTaskOperator(
        task_id="task_id", template_path=file_path, python_command=python_command
    )
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

    file_taskgroups = NomadJobModel.model_validate_json(content).Job.TaskGroups
    file_taskgroups[0].Tasks[0].Config.args = [python_command]

    op.prepare_job_template(context)
    assert op.template
    assert op.template.Job.TaskGroups == file_taskgroups
    assert op.template.Job.ID != NomadJobModel.model_validate_json(content).Job.ID


@pytest.mark.parametrize(
    "param, value",
    [("command", "cmd"), ("entrypoint", ["python", "-c"])],
)
def test_args_args(param, value):
    python_command = "print('Python job here!'"
    op = NomadPythonTaskOperator(task_id="task_id", **{param: value}, python_command=python_command)
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
    python_command = "print('Python job here!'"
    op = NomadPythonTaskOperator(task_id="task_id", **paramdict, python_command=python_command)  # type: ignore [reportArgumentType]
    with pytest.raises(NomadProviderException):
        op.prepare_job_template({})


def test_args_template_path_invalid(caplog):
    python_command = "print('Python job here!'"
    op = NomadPythonTaskOperator(
        task_id="task_id", template_path="bla", python_command=python_command
    )
    with caplog.at_level(logging.ERROR):
        with pytest.raises(NomadProviderException) as err:
            assert not op.prepare_job_template({})
    assert "No such file or directory:" in caplog.text

    with pytest.raises(NomadProviderException) as err:
        op.execute({})
    assert "Nothing to execute" in str(err.value)


def test_args_template_content_invalid(caplog):
    python_command = "print('Python job here!'"
    op = NomadPythonTaskOperator(
        task_id="task_id", template_content="bla", python_command=python_command
    )
    with caplog.at_level(logging.ERROR):
        with pytest.raises(NomadProviderException) as err:
            op.prepare_job_template({})
    assert "Couldn't parse template 'bla'" in caplog.text
    assert "Nothing to execute" in str(err.value)


def test_args_invalid_template():
    python_command = "print('Python job here!'"
    with pytest.raises(ValueError) as err:
        NomadPythonTaskOperator(
            task_id="task_id",
            template_path="/some/path",
            template_content="<HCL>",
            python_command=python_command,
        )
    assert "Only one of 'template_content' and 'template_path' can be specified" in str(err.value)


def test_args_invalid_env():
    python_command = "print('Python job here!'"
    op = NomadPythonTaskOperator(
        task_id="task_id",
        command="date",
        env="TZ: 'Europe/Paris'",  # type: ignore [reportArgumentType]
        python_command=python_command,
    )

    with pytest.raises(NomadValidationError) as err:
        op.prepare_job_template({})
    assert "'env': Input should be a valid dictionary" in str(err.value)


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
    python_command = "print('Python job here!'"
    op = NomadPythonTaskOperator(
        task_id="task_id", ephemeral_disk=edisk_data, python_command=python_command
    )
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
    python_command = "print('Python job here!'"
    op = NomadPythonTaskOperator(
        task_id="task_id", task_resources=res_data, python_command=python_command
    )
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
    python_command = "print('Python job here!'"
    op = NomadPythonTaskOperator(
        task_id="task_id", task_resources=res_data, python_command=python_command
    )

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
    python_command = "print('Python job here!'"
    op = NomadPythonTaskOperator(
        task_id="task_id", task_resources=res_data, python_command=python_command
    )
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
    python_command = "print('Python job here!'"
    op = NomadPythonTaskOperator(
        task_id="task_id",
        volume_mounts=vol_mounts_data,
        volumes=vol_data,
        python_command=python_command,
    )
    op.prepare_job_template(context)
    assert op.template
    assert op.template.Job.TaskGroups[0].Tasks[0].VolumeMounts == NomadVolumeMounts.validate_python(
        vol_mounts_data
    )
