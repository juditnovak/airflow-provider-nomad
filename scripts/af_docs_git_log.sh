#! /bin/bash


if [[ -z $1 ]]
then
    git log '--pretty=format:%H %h %cd %s' --date=short  -- src/airflow/providers
else
    git log '--pretty=format:%H %h %cd %s' --date=short ${1}..HEAD -- src/airflow/providers
fi

if [ $? -ne 0 ]
then
    echo "Usage: $0 [<valid_tag>]"
fi
