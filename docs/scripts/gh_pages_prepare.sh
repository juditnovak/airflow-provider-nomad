#! /bin/bash

# NOTE!
# Airflow normally stores these docs under /docs/. A number of references are hardcoded to this location
# or may expect http(s)://<host>/docs/ structure (with no further references in-between)
#
# The hacks below ensure docs availability from GH pages published sources

set -x

mv docs/docs/* docs/
rmdir docs/docs

grep -r '"/external/' docs | cut -d: -f1 | uniq | xargs sed -i 's!"/external/!"/airflow-provider-nomad/external/!g'
grep -r '"/_gen/' docs | cut -d: -f1 | uniq | xargs sed -i 's!"/_gen/!"/airflow-provider-nomad/_gen/!g'
grep -r  '"/docs/"' ./ | cut -d: -f1 | uniq | xargs sed -i 's!"/docs/"!"/airflow-provider-nomad/"!g'


# In addition: fixing the Navigation Bar

grep -r '<a href="/">' docs | cut -d: -f1 | uniq | xargs sed -i 's!<a href="/">!<a href="https://airflow.apache.org/">!g'
grep -r '<a class="navbar__text-link" href="/' docs | cut -d: -f1 | uniq | xargs sed -i 's!<a class="navbar__text-link" href="/!<a class="navbar__text-link" href="https://airflow.apache.org/!g'

# Fixing the Github suggest button
grep -r "https://github.com/apache/airflow/edit/main/providers/nomad/docs" docs/ | cut -d: -f1 | uniq | xargs sed -i 's!https://github.com/apache/airflow/edit/main/providers/nomad/docs!https://github.com/juditnovak/airflow-provider-nomad/edit/main/docs-rst!g'
