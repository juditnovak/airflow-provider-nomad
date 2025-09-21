###############################################################################
# These tests assume that a Nomad service is available as appears
# in the test configuration
###############################################################################

import json
import logging

from airflow.providers.nomad.manager import NomadManager
from airflow.providers.nomad.models import NomadJobModel


def test_nomad_mgr_parse_template_hcl(test_datadir):
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


def test_nomad_mgr_retry_register_job(test_datadir, caplog):
    mgr = NomadManager()
    mgr.initialize()

    with open(test_datadir / "simple_batch.hcl") as tpl_file:
        template = mgr.parse_template_content(tpl_file.read())

    assert template, "Error retrieving template"
    # Ruining the template
    template.Job.ID = " "

    with caplog.at_level(logging.ERROR):
        error = mgr.register_job(template)

    assert (
        error
        == "The BaseNomadException was raised with following response: 1 error occurred:\n\t* Job ID contains a space\n\n."
    )
    assert caplog.text.count("Job ID contains a space") == 3


def test_nomad_mgr_retry_deregister_job(caplog):
    mgr = NomadManager()
    mgr.initialize()

    with caplog.at_level(logging.ERROR):
        error = mgr.deregister_job("")

    assert (
        error
        == "The BaseNomadException was raised with following response: missing job ID for deregistering."
    )
    assert caplog.text.count("missing job ID for deregistering") == 3
