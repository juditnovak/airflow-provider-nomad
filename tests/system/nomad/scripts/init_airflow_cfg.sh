#! /bin/bash

if [[ -z "$AIRFLOW_HOME" ]]; then
    echo "AIRFLOW_HOME must be defined"
fi

cat tests/system/nomad/config/unit_tests.cfg.template | \
    sed "s|<SYSTEST_ROOT>|$PWD/tests/system/nomad|g"  | \
    sed "s|<SYSTEST_AIRFLOW_HOME>|$AIRFLOW_HOME|g" > $AIRFLOW_HOME/airflow.cfg
