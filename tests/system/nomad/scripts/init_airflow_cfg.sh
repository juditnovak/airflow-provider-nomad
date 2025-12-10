#! /bin/bash

TPL_FILE=unit_tests.cfg.template
if [[ $1 ]]
then
    TPL_FILE=unit_tests_$1.cfg.template
fi

TPL_PATH=tests/system/nomad/config/$TPL_FILE

if [[ ! -f $TPL_PATH ]]
then
    echo "$TPL_PATH doesn't exist, exiting"
    exit 2
fi

if [[ -z "$AIRFLOW_HOME" ]]; then
    echo "AIRFLOW_HOME must be defined"
fi

AIRFLOW_CFG=$AIRFLOW_HOME/airflow.cfg

if [ -f "$AIRFLOW_CFG" ]; then
    echo "[ERROR] $AIRFLOW_CFG is an existing file, exiting..."
    exit 1
fi

# Due to occasional CI errors on the file missing
touch $AIRFLOW_CFG

cat $TPL_PATH | \
    sed "s|<SYSTEST_ROOT>|$PWD/tests/system/nomad|g"  | \
    sed "s|<SYSTEST_AIRFLOW_HOME>|$AIRFLOW_HOME|g" > $AIRFLOW_CFG

echo "$AIRFLOW_CFG was generated successfully"
