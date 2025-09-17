###############################################################################
# These tests assume that a Nomad service is available as appears
# in the test configuration
###############################################################################

import pytest

from airflow.providers.nomad.executors.nomad_executor import NomadExecutor


@pytest.mark.usefixtures("nomad_agent")
def test_connect():
    """Connection to the Nomad cluster"""
    nomad_executor = NomadExecutor()
    nomad_executor.start()

    assert nomad_executor.nomad_mgr.nomad
    assert nomad_executor.nomad_mgr.nomad.agent.get_members()
