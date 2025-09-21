#! /bin/bash

# NOTE!
# Airflow normally stores these docs under /docs/. A number of references are hardcoded to this location
# or may expect http(s)://<host>/docs/ structure (with no further references in-between)
#
# The hacks below ensure docs availability from GH pages published sources

mv docs/docs/* docs/
rmdir docs/docs

grep -r '"/external/' docs | cut -d: -f1 | uniq | xargs sed -i 's!"/external/!"/airflow-provider-nomad/external/!g'
grep -r '"/_gen/' docs | cut -d: -f1 | uniq | xargs sed -i 's!"/_gen/!"/airflow-provider-nomad/_gen/!g'
grep -r  '"/docs/"' ./ | cut -d: -f1 | uniq | xargs sed -i 's!"/docs/"!"/airflow-provider-nomad/"!g'
