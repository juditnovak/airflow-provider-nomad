from __future__ import annotations
import pendulum

import logging
import os
import sys
import time
from pprint import pprint
import datetime

import attrs
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.definitions.param import ParamsDict

log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "test-python-operator"
JOB_NAME = "test-python-operator"
JOB_NAMESPACE = "default"

##############################################################################
# DAG with relative location
#
# Airflow SDK DAGs are constucted with their absolute path ('fileloc' attr)
# instead of the relative location within the 'dags-folder'.
# In order to allow for remote job submission, this requires a patch
##############################################################################


def _default_fileloc() -> str:
    return os.path.basename(__file__)


def _all_after_dag_id_to_kw_only(cls, fields: list[attrs.Attribute]):
    i = iter(fields)
    f = next(i)
    if f.name != "dag_id":
        raise RuntimeError("dag_id was not the first field")
    yield f

    for f in i:
        yield f.evolve(kw_only=True)


@attrs.define(repr=False, field_transformer=_all_after_dag_id_to_kw_only, slots=False)  # pyright: ignore[reportArgumentType]
class myDAG(DAG):
    fileloc: str = attrs.field(init=False, factory=_default_fileloc)

    def __hash__(self):
        return super().__hash__()


##############################################################################

with myDAG(
    dag_id=DAG_ID,
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    dagrun_timeout=datetime.timedelta(minutes=10),
    disable_bundle_versioning=True,
    catchup=False,
    tags=["nomad", "nomadexecutor", "nomad-provider-test"],
    params=ParamsDict({"example_key": "example_value"}),
) as dag:
    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )

    # [START howto_operator_python]
    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        print("::group::All kwargs")
        pprint(kwargs)
        print("::endgroup::")
        print("::group::Context variable ds")
        print(ds)
        print("::endgroup::")
        return "Whatever you return gets printed in the logs"

    run_this = PythonOperator(task_id="print_the_context", python_callable=print_context)
    # [END howto_operator_python]

    # [START howto_operator_python_render_sql]
    def log_sql(**kwargs):
        log.info("Python task decorator query: %s", str(kwargs["templates_dict"]["query"]))

    log_the_sql = PythonOperator(
        task_id="log_sql_query",
        python_callable=log_sql,
        templates_dict={"query": "sql/sample.sql"},
        templates_exts=[".sql"],
    )
    # [END howto_operator_python_render_sql]

    # [START howto_operator_python_kwargs]
    # Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
    def my_sleeping_function(random_base):
        """This is a function that will run within the DAG execution"""
        time.sleep(random_base)
        # time.sleep(100000000)

    for i in range(5):
        sleeping_task = PythonOperator(
            task_id=f"sleep_for_{i}",
            python_callable=my_sleeping_function,
            op_kwargs={"random_base": i / 10},
        )

        run_this >> log_the_sql >> sleeping_task  # type: ignore [reportUnusedExpression]
    # [END howto_operator_python_kwargs]

    run_this >> sleeping_task  # type: ignore[reportPossiblyUnbound]


try:
    from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

    # # Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
    test_run = get_test_run(dag)
except ImportError:
    pass
