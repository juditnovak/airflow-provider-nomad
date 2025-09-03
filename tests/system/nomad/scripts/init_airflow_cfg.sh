#! /bin/bash

if [[ -z "$AIRFLOW_HOME" ]]; then
    echo "AIRFLOW_HOME must be defined"
fi

AIRFLOW_CFG=$AIRFLOW_HOME/airflow.cfg

if [ -f "$AIRFLOW_CFG" ]; then
    echo "[ERROR] $AIRFLOW_CFG is an existing file, exiting..."
    exit 1
fi

# Due to occasional CI errors on the file missing
touch $AIRFLOW_HOME/airflow.cfg

cat tests/system/nomad/config/unit_tests.cfg.template | \
    sed "s|<SYSTEST_ROOT>|$PWD/tests/system/nomad|g"  | \
    sed "s|<SYSTEST_AIRFLOW_HOME>|$AIRFLOW_HOME|g" > $AIRFLOW_CFG

echo "$AIRFLOW_CFG was generated successfully"
