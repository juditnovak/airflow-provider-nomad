import json
import pytest
import logging
from time import sleep

from nomad.api.exceptions import BaseNomadException  # type: ignore[import-untyped]
from tests_common.test_utils.config import conf_vars

from airflow.providers.nomad.constants import CONFIG_SECTION
from airflow.providers.nomad.manager import NomadManager
from airflow.providers.nomad.models import (
    NomadJobAllocationInfo,
    NomadJobAllocations,
    NomadJobEvaluation,
    NomadJobEvaluationInfo,
    NomadJobModel,
    NomadJobSubmission,
    NomadJobSummary,
)


@conf_vars({})
def test_base_defaults():
    nomad_mgr = NomadManager()
    assert nomad_mgr
    assert nomad_mgr.nomad_server == "127.0.0.1"
    assert nomad_mgr.cert_path == ""
    assert nomad_mgr.key_path == ""
    assert nomad_mgr.verify == ""
    assert nomad_mgr.secure == False  # noqa: E712


@conf_vars(
    {
        ("nomad_provider", "agent_host"): "1.2.3.4",
    }
)
def test_base_fallback_default_params():
    nomad_mgr = NomadManager()
    assert nomad_mgr
    assert nomad_mgr.nomad_server == "1.2.3.4"
    assert nomad_mgr.cert_path == ""
    assert nomad_mgr.key_path == ""
    assert nomad_mgr.verify == ""
    assert nomad_mgr.secure == False  # noqa: E712


@conf_vars(
    {
        ("nomad_provider", "agent_host"): "1.2.3.4",
        ("nomad_provider", "agent_cert_path"): "certs_absolute_path/global-cli-nomad.pem",
        ("nomad_provider", "agent_key_path"): "certs_absolute_path/global-cli-nomad-key.pem",
        ("nomad_provider", "agent_verify"): "certs_absolute_path/nomad-agent-ca.pem",
        ("nomad_provider", "agent_secure"): "true",
    }
)
def test_base_params_secure():
    nomad_mgr = NomadManager()
    assert nomad_mgr
    assert nomad_mgr.nomad_server == "1.2.3.4"
    assert nomad_mgr.cert_path == "certs_absolute_path/global-cli-nomad.pem"
    assert nomad_mgr.key_path == "certs_absolute_path/global-cli-nomad-key.pem"
    assert nomad_mgr.verify == "certs_absolute_path/nomad-agent-ca.pem"
    assert nomad_mgr.secure == True  # noqa: E712


def test_base_params_secure_verify_bool():
    with conf_vars({("nomad_provider", "agent_verify"): "cacert_path"}):
        nomad_mgr = NomadManager()
        assert nomad_mgr.verify == "cacert_path"

    with conf_vars({("nomad_provider", "agent_verify"): "true"}):
        nomad_mgr = NomadManager()
        assert nomad_mgr.verify == True  # noqa: E712

    with conf_vars({("nomad_provider", "agent_verify"): "false"}):
        nomad_mgr = NomadManager()
        assert nomad_mgr.verify == False  # noqa: E712


def test_job_get_stderr(mock_nomad_client):
    mock_nomad_client.client.cat.read_file.return_value = "bla"

    nomad_mgr = NomadManager()
    nomad_mgr.initialize()

    assert nomad_mgr.get_job_stderr("alloc_id", "task_id") == "bla"


def test_job_get_stdout(mock_nomad_client):
    mock_nomad_client.client.cat.read_file.return_value = "bla"

    nomad_mgr = NomadManager()
    nomad_mgr.initialize()

    assert nomad_mgr.get_job_stdout("alloc_id", "task_id") == "bla"


def test_job_get_file(mock_nomad_client):
    mock_nomad_client.client.cat.read_file.return_value = "bla"

    nomad_mgr = NomadManager()
    nomad_mgr.initialize()

    assert nomad_mgr.get_job_file("alloc_id", "path") == "bla"


@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_parse_template_json(filename, test_datadir):
    nomad_mgr = NomadManager()
    nomad_mgr.initialize()

    file_path = test_datadir / filename
    with open(file_path) as file:
        content = file.read()

    assert NomadJobModel.model_validate_json(content) == nomad_mgr.parse_template_json(content)
    assert NomadJobModel.model_validate_json(content) == nomad_mgr.parse_template_content(content)


@pytest.mark.parametrize("filename", ["simple_job.json", "complex_job.json"])
def test_parse_template_dict(filename, test_datadir):
    nomad_mgr = NomadManager()
    nomad_mgr.initialize()

    file_path = test_datadir / filename
    with open(file_path) as file:
        content = file.read()

    data = json.loads(content)
    assert NomadJobModel.model_validate(data) == nomad_mgr.parse_template_json(content)
    assert NomadJobModel.model_validate(data) == nomad_mgr.parse_template_content(content)


def test_parse_template_hcl(test_datadir, mock_nomad_client):
    nomad_mgr = NomadManager()
    nomad_mgr.initialize()

    file_path1 = test_datadir / "simple_batch.hcl"
    file_path2 = test_datadir / "simple_batch_api_retval.json"
    with open(file_path1) as file1, open(file_path2) as file2:
        hcl = file1.read()
        client_resp = file2.read()

        mock_nomad_client.jobs.parse.return_value = json.loads(client_resp)
        json_content = '{ "Job": ' + client_resp + "}"

        assert NomadJobModel.model_validate_json(json_content) == nomad_mgr.parse_template_hcl(hcl)
        assert NomadJobModel.model_validate_json(json_content) == nomad_mgr.parse_template_content(
            hcl
        )


def test_parse_template_error(caplog):
    nomad_mgr = NomadManager()
    nomad_mgr.initialize()

    with caplog.at_level(logging.ERROR):
        assert not nomad_mgr.parse_template_content("bad content")
    assert "Couldn't parse template 'bad content'" in caplog.text


def test_job_all_info_str(mock_nomad_client, test_datadir):
    nomad_mgr = NomadManager()
    nomad_mgr.initialize()

    file_path1 = test_datadir / "nomad_job_evaluation.json"
    file_path2 = test_datadir / "nomad_job_allocations.json"
    file_path3 = test_datadir / "nomad_job_summary_running.json"
    with open(file_path1) as file1, open(file_path2) as file2, open(file_path3) as file3:
        job_eval_data = file1.read()
        job_alloc_data = file2.read()
        job_summary_data = file3.read()
    mock_get_eval = mock_nomad_client.job.get_evaluations
    mock_get_eval.return_value = json.loads(job_eval_data)
    mock_get_alloc = mock_nomad_client.job.get_allocations
    mock_get_alloc.return_value = json.loads(job_alloc_data)
    mock_get_summary = mock_nomad_client.job.get_summary
    mock_get_summary.return_value = json.loads(job_summary_data)

    job_eval = NomadJobEvaluation.validate_json(job_eval_data)
    job_alloc = NomadJobAllocations.validate_json(job_alloc_data)
    job_summary = NomadJobSummary.model_validate_json(job_summary_data)

    # Call with pre-fetched data
    infostr1 = nomad_mgr.job_all_info_str(
        "job_id", job_summary=job_summary, job_alloc=job_alloc, job_eval=job_eval
    )

    assert infostr1[0] == "Job summary:"
    firstind = 1
    lastind = firstind + len(job_summary_data.splitlines())
    assert infostr1[firstind:lastind] == job_summary_data.splitlines()

    firstind = lastind + 1
    assert infostr1[lastind] == "Job allocations info:"
    lastind = firstind + len(job_alloc_data.splitlines())
    assert infostr1[firstind:lastind] == job_alloc_data.splitlines()

    firstind = lastind + 1
    assert infostr1[lastind] == "Job evaluations:"
    lastind = firstind + len(job_eval_data.splitlines())
    assert infostr1[firstind:lastind] == job_eval_data.splitlines()

    # Call without pre-fetched data
    infostr2 = nomad_mgr.job_all_info_str("job_id")
    assert infostr1 == infostr2

    mock_get_alloc.assert_called_once()
    mock_get_eval.assert_called_once()
    mock_get_summary.assert_called_once()


def test_get_allocations(mock_nomad_client, test_datadir):
    nomad_mgr = NomadManager()
    nomad_mgr.initialize()

    file_path = test_datadir / "nomad_job_allocations.json"
    with open(file_path) as file:
        mock_nomad_client.job.get_allocations.return_value = json.loads(file.read())

    allocations = nomad_mgr.get_nomad_job_allocation("somejob")
    assert allocations
    assert all(isinstance(alloc, NomadJobAllocationInfo) for alloc in allocations)

    mock_nomad_client.job.get_allocations.return_value = {"wrong": "input"}
    assert not nomad_mgr.get_nomad_job_allocation("somejob")

    mock_nomad_client.job.get_allocations.return_value = None
    assert not nomad_mgr.get_nomad_job_allocation("somejob")

    mock_nomad_client.job.get_allocations.side_effect = BaseNomadException("")
    assert not nomad_mgr.get_nomad_job_allocation("somejob")


def test_get_job_status(mock_nomad_client, test_datadir):
    nomad_mgr = NomadManager()
    nomad_mgr.initialize()

    file_path = test_datadir / "nomad_job_summary_running.json"
    with open(file_path) as file:
        mock_nomad_client.job.get_summary.return_value = json.loads(file.read())

    assert isinstance(nomad_mgr.get_nomad_job_summary("somejob"), NomadJobSummary)

    mock_nomad_client.job.get_summary.return_value = {"wrong": "input"}
    assert not nomad_mgr.get_nomad_job_summary("somejob")

    mock_nomad_client.job.get_summary.return_value = None
    assert not nomad_mgr.get_nomad_job_summary("somejob")

    mock_nomad_client.job.get_summary.side_effect = BaseNomadException("")
    assert not nomad_mgr.get_nomad_job_summary("somejob")


def test_get_job_evaluations(mock_nomad_client, test_datadir):
    nomad_mgr = NomadManager()
    nomad_mgr.initialize()

    file_path = test_datadir / "nomad_job_evaluation.json"
    with open(file_path) as file:
        mock_nomad_client.job.get_evaluations.return_value = json.loads(file.read())

    assert all(
        isinstance(evalu, NomadJobEvaluationInfo)
        for evalu in nomad_mgr.get_nomad_job_evaluations("somejob")  # type: ignore[reportOperationalIterable]
    )

    mock_nomad_client.job.get_evaluations.return_value = {"wrong": "input"}
    assert not nomad_mgr.get_nomad_job_evaluations("somejob")

    mock_nomad_client.job.get_evaluations.return_value = None
    assert not nomad_mgr.get_nomad_job_evaluations("somejob")

    mock_nomad_client.job.get_evaluations.side_effect = BaseNomadException("")
    assert not nomad_mgr.get_nomad_job_evaluations("somejob")


def test_get_job_submission(mock_nomad_client, test_datadir):
    nomad_mgr = NomadManager()
    nomad_mgr.initialize()

    file_path = test_datadir / "nomad_job_info.json"
    with open(file_path) as file:
        mock_nomad_client.job.get_job.return_value = json.loads(file.read())

    assert isinstance(nomad_mgr.get_nomad_job_submission("somejob"), NomadJobSubmission)

    mock_nomad_client.job.get_job.return_value = {"wrong": "input"}
    assert not nomad_mgr.get_nomad_job_submission("somejob")

    mock_nomad_client.job.get_job.return_value = None
    assert not nomad_mgr.get_nomad_job_submission("somejob")

    mock_nomad_client.job.get_job.side_effect = BaseNomadException("")
    assert not nomad_mgr.get_nomad_job_submission("somejob")


@conf_vars({("nomad_provider", "alloc_pending_timeout"): "0"})
def test_remove_job_if_hanging_good_job(mock_nomad_client, test_datadir):
    """Job submission is all good, job is not to be killed"""
    nomad_mgr = NomadManager()
    nomad_mgr.initialize()

    file_path1 = test_datadir / "nomad_job_evaluation.json"
    file_path2 = test_datadir / "nomad_job_info.json"
    file_path3 = test_datadir / "nomad_job_allocations.json"
    with open(file_path1) as file1, open(file_path2) as file2, open(file_path3) as file3:
        mock_nomad_client.job.get_evaluations.return_value = json.loads(file1.read())
        mock_nomad_client.job.get_job.return_value = json.loads(file2.read())
        mock_nomad_client.job.get_allocations.return_value = json.loads(file3.read())
        mock_kill = mock_nomad_client.job.deregister_job

    assert nomad_mgr.remove_job_if_hanging("somejob") == (False, "")
    sleep(1)
    assert nomad_mgr.remove_job_if_hanging("somejob") == (False, "")
    mock_kill.assert_not_called()


@conf_vars({("nomad_provider", "alloc_pending_timeout"): "0"})
def test_remove_job_if_hanging_evaluation_timeout(mock_nomad_client, test_datadir):
    """Job allocation failed, and it's passing configured timeout"""
    nomad_mgr = NomadManager()
    nomad_mgr.initialize()

    file_path1 = test_datadir / "nomad_job_evaluation_failed.json"
    file_path2 = test_datadir / "nomad_job_info_pending.json"
    file_path3 = test_datadir / "nomad_job_allocations.json"
    with open(file_path1) as file1, open(file_path2) as file2, open(file_path3) as file3:
        mock_nomad_client.job.get_evaluations.return_value = json.loads(file1.read())
        mock_nomad_client.job.get_job.return_value = json.loads(file2.read())
        mock_nomad_client.job.get_allocations.return_value = json.loads(file3.read())
        mock_kill = mock_nomad_client.job.deregister_job

    assert nomad_mgr.remove_job_if_hanging("somejob") == (False, "")
    sleep(1)
    assert nomad_mgr.remove_job_if_hanging("somejob") == (
        True,
        "[\"{'missing compatible host volumes': 1}\"]",
    )
    mock_kill.assert_called_once()


@conf_vars({("nomad_provider", "alloc_pending_timeout"): "100"})
def test_remove_job_if_hanging_no_evaluation_timeout(mock_nomad_client, test_datadir):
    """Job allocation failed, but we've been within the timeout so the job isn't killed"""
    nomad_mgr = NomadManager()
    nomad_mgr.initialize()

    file_path1 = test_datadir / "nomad_job_evaluation_failed.json"
    file_path2 = test_datadir / "nomad_job_info_pending.json"
    file_path3 = test_datadir / "nomad_job_allocations.json"
    with open(file_path1) as file1, open(file_path2) as file2, open(file_path3) as file3:
        mock_nomad_client.job.get_evaluations.return_value = json.loads(file1.read())
        mock_nomad_client.job.get_job.return_value = json.loads(file2.read())
        mock_nomad_client.job.get_allocations.return_value = json.loads(file3.read())
    mock_kill = mock_nomad_client.job.deregister_job

    assert nomad_mgr.remove_job_if_hanging("somejob") == (False, "")
    sleep(1)
    assert nomad_mgr.remove_job_if_hanging("somejob") == (False, "")
    mock_kill.assert_not_called()


@conf_vars({("nomad_provider", "alloc_pending_timeout"): "0"})
def test_remove_job_if_hanging_alloc_failure(mock_nomad_client, test_datadir):
    """Job evaluation failed, Nomad status is 'dead', job is killed ultimately"""
    nomad_mgr = NomadManager()
    nomad_mgr.initialize()

    file_path1 = test_datadir / "nomad_job_evaluation.json"
    file_path2 = test_datadir / "nomad_job_info_dead.json"
    file_path3 = test_datadir / "nomad_job_allocations_pending.json"
    file_path4 = test_datadir / "nomad_job_summary_failed.json"
    with (
        open(file_path1) as file1,
        open(file_path2) as file2,
        open(file_path3) as file3,
        open(file_path4) as file4,
    ):
        mock_nomad_client.job.get_evaluations.return_value = json.loads(file1.read())
        mock_nomad_client.job.get_job.return_value = json.loads(file2.read())
        mock_nomad_client.job.get_allocations.return_value = json.loads(file3.read())
        mock_nomad_client.job.get_summary.return_value = json.loads(file4.read())
    mock_kill = mock_nomad_client.job.deregister_job

    res = nomad_mgr.remove_job_if_hanging("somejob")
    assert res and res[0]
    assert "Error response from daemon: pull access denied for novakjudi/af_nomad_test" in res[1]
    mock_kill.assert_called_once()


@pytest.mark.parametrize("retry_num", ["1", "3"])
def test_retry_job_submission(retry_num, mock_nomad_client, test_datadir, caplog):
    """This test requiers full relaod of the 'manager' module, so module constants get re-evaluate"""
    with conf_vars({(CONFIG_SECTION, "job_submission_retry_num"): retry_num}):
        from importlib import reload
        import airflow.providers.nomad.manager

        reload(airflow.providers.nomad.manager)

        from airflow.providers.nomad.manager import NomadManager

        nomad_mgr = NomadManager()
        nomad_mgr.initialize()

        with open(test_datadir / "simple_batch.json") as tpl_file:
            template = nomad_mgr.parse_template_content(tpl_file.read())

        assert template

        msg = "Can't submit job"
        mock_nomad_client.job.register_job.get_job = json.dumps({})
        mock_nomad_client.job.register_job.side_effect = [
            BaseNomadException(msg),
            BaseNomadException(msg),
            BaseNomadException(msg),
        ]
        mock_kill = mock_nomad_client.job.deregister_job

        with caplog.at_level(logging.ERROR):
            res = nomad_mgr.register_job(template)

        assert res == f"The BaseNomadException was raised due to the following error: {msg}"
        assert caplog.text.count(msg) == int(retry_num)
        assert mock_kill.call_count == int(retry_num)


@pytest.mark.parametrize("retry_num", ["1", "3"])
def test_retry_job_deregister(retry_num, mock_nomad_client, caplog):
    """This test requiers full relaod of the 'manager' module, so module constants get re-evaluate"""
    with conf_vars({(CONFIG_SECTION, "job_submission_retry_num"): retry_num}):
        from importlib import reload
        import airflow.providers.nomad.manager

        reload(airflow.providers.nomad.manager)

        from airflow.providers.nomad.manager import NomadManager

        nomad_mgr = NomadManager()
        nomad_mgr.initialize()

        msg = "Can't stop job"
        mock_nomad_client.job.deregister_job.side_effect = [
            BaseNomadException(msg),
            BaseNomadException(msg),
            BaseNomadException(msg),
        ]
        mock_kill = mock_nomad_client.job.deregister_job

        with caplog.at_level(logging.ERROR):
            res = nomad_mgr.deregister_job("somejob")

        assert res == f"The BaseNomadException was raised due to the following error: {msg}"
        assert caplog.text.count(msg) == int(retry_num)
        assert mock_kill.call_count == int(retry_num)
