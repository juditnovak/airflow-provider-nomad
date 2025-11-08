# Guidelines for contributors


## Issues

...are welcome :-)

(With or without proposal on the fix.)


## Code contributions

...coming soon...


## Running the tests

### Pre-requisites

The `nomad_provider` test-suite is using the same approach as Airflow itself (including the usage of Airflow `pytest` plugins).

Therefore it's necessary to have the Airflow code base available

```
git clone git@github.com:apache/airflow.git
export AIRFLOW_SOURCE=$PWD/airflow
```

### Unit tests

The project is using `tox` for tests execution.

Running the Unit Tests goes the following
```
uv run tox run -e unit_test -- --with-db-init
```

NOTE:

In case running into issues, you could give it a try to reset the test DB.

However, before doing so, make sure that `AIRFLOW_HOME` is **indeed** pointing to the test DB.

```
uv run airflow db reset -y
```



### System Tests

System Tests are described in detail as a part of the [documentation](https://juditnovak.github.io/airflow-provider-nomad/apache-airflow-providers-nomad/stable/system_tests.html)


## Documentation

### Pre-requisites

The `nomad_provider` documentation is built using Airflow's documentation generation system.

Therefore it's necessary to have the Airflow code base available

```
git clone git@github.com:apache/airflow.git
export AIRFLOW_SOURCE=$PWD/airflow
```

### Building the documentation

To start with, go to the docs build `airflow` repository
```
cd $AIRFLOW_SOURCE
```
assuming that `$AIRFLOW_SOURCE` is where your `airflow` code repository clone resides.


Building the docs for the first time starts with:
```
cp -r <PATH_TO>/nomad_provder /providers/
```

In case there were changes in the code or you build the docs for the first time:

```
cp -r <PATH_TO>/nomad_provider/* providers/nomad/
mv providers/nomad/docs-rst providers/nomad/docs
```

Otherwise it's enough to update the docs
```
cp -r ../nomad_provider/docs-rst/* providers/nomad/docs/
```

And then build the documentation
```
uv run --group docs build-docs --package-filter apache-airflow-providers-nomad
```

### Adding generated docs to the repository

At the end, copy the freshly generated documentation from `$AIRFLOW_SOURCE` to the corresponding destination.

```
cp -r $AIRFLOW_SOURCE/generated/_build/docs/apache-airflow-providers-nomad/stable/* <PATH_TO>/nomad_provider/docs/docs/apache-airflow-providers-nomad/<VERSION>/
```
