###############################################################################
# These tests assume that a Nomad service is available as appears
# in the test configuration
###############################################################################

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
