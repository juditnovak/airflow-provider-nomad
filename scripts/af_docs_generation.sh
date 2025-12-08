#! /bin/bash

cur_dir=$(basename $PWD)

if [[ $cur_dir == "nomad_provider" ]]
then
    echo "This script is to be run from the docs creation path"
    exit 1
fi

cp -r ../nomad_provider/* providers/nomad/ 
rm -fr providers/nomad/docs/_api
rm -fr providers/nomad/docs
cp -r ../nomad_provider/docs-rst/ ./providers/nomad/docs/
uv run  --group docs build-docs --package-filter apache-airflow-providers-nomad nomad


# Fixing navigation bar links
# Done on the pipeline, commented here
#
# grep -r '<a href="/">' docs | cut -d: -f1 | uniq | xargs sed -i 's!<a href="/">!<a href="https://airflow.apache.org/">!g'
# grep -r '<a class="navbar__text-link" href="/' docs | cut -d: -f1 | uniq | xargs sed -i 's!<a class="navbar__text-link" href="/!<a class="navbar__text-link" href="https://airflow.apache.org/!g'
#
#
# Replace version in case broken throughout doc generation
# grep -r  '"version">$OLD' docs/docs/apache-airflow-providers-nomad/0.0.6.dev1/ | cut -d: -f1 | sort | uniq | xargs sed -i 's/"version">$OLD/"version">%NEW/'
