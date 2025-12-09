#!/bin/sh

# Assuming that uv version bump similar to
#    uv version --bump patch --bump beta
# was run (to maintain pyproject.yaml)


help () {
    echo "Usage: $0 <OLD_RELEASE> <NEW_RELEASE>"
}

OLD=$1
NEW=$2


if [[ -z $1  || -z $2 ]]
then
    help
    exit 1
fi

OLD_PAT=$OLD
REPL_PAT="s!$OLD!$NEW!g"

IGNORE_SRC=""
IGNORE_TESTS="/logs/\|.cfg:"
IGNORE_DOCS="changelog.rst\|commits.rst"

# We olny leave trace of old release in provicer.yaml if it was not a pre-release
echo "Updating provider.yaml"
FULL_RELEASE=0
[ ${OLD#[0-9]*.[0-9]*.[0-9]*} == "" ] || FULL_RELEASE=$?


echo "Updating provider.yaml"

if [[ $FULL_RELEASE ]]
then
    sed -i -r "s!$OLD!$NEW!g" provider.yaml
else
    sed -i -r "s!$OLD!$NEW"'\n  - '"$OLD!g" provider.yaml
fi

echo "Updating docs configuration"

if [[ ! $FULL_RELEASE ]]
then
    sed -i "s/\"stable-version\": \"$OLD\"/\"stable-version\": \"$NEW\"/"  docs/_gen/packages-metadata.json
fi

sed -i "s/\"all-versions\": \[\"$OLD\"/\"all-versions\": \[\"$NEW\"/"  docs/_gen/packages-metadata.json


echo "Updating src"
grep -r "$OLD_PAT" src/ | cut -d: -f1 | uniq | xargs sed -i $REPL_PAT
echo "Updating tests"
grep -r "$OLD_PAT" tests/ | grep -v $IGNORE_TESTS | cut -d: -f1 | uniq | xargs sed -i $REPL_PAT
echo "Updating docs-rst"
grep -r "$OLD_PAT" docs-rst/ | grep -v $IGNORE_DOCS | cut -d: -f1 | uniq | xargs sed -i $REPL_PAT

echo "[RELEASE] Don't forget to update CHANGELOG and the list of commits in docs-rst"
echo "You probably want to run: ./src/airflow/providers/nomad/docker/build.sh -t $NEW <nomad_runner_docker_img>"
