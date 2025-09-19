 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


.. NOTE TO CONTRIBUTORS:
   Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
   and you want to add an explanation to the users on how they are supposed to deal with them.
   The changelog is updated and maintained semi-automatically by release manager.

``apache-airflow-providers-nomad``


Changelog
##########


0.0.2
......

* Operators: `NomadJobOperator` and `NomadTaskOperator` by @juditnovak in https://github.com/juditnovak/airflow-provider-nomad/pull/15
* [CI] Timeout increase by @juditnovak in https://github.com/juditnovak/airflow-provider-nomad/pull/16
* Enabling support for Airflow templating by @juditnovak in https://github.com/juditnovak/airflow-provider-nomad/pull/17


**Full Changelog**: https://github.com/juditnovak/airflow-provider-nomad/compare/0.0.1...0.0.2
**Release Notes**: https://github.com/juditnovak/airflow-provider-nomad/releases/tag/0.0.2


0.0.1
......

* Nomad executor with bash operator by @juditnovak in https://github.com/juditnovak/airflow-provider-nomad/pull/1
* System tests by @juditnovak in https://github.com/juditnovak/airflow-provider-nomad/pull/3
* Tests completed and cleaned up by @juditnovak in https://github.com/juditnovak/airflow-provider-nomad/pull/4
* Cleanup and small executor enhancements by @juditnovak in https://github.com/juditnovak/airflow-provider-nomad/pull/5
* Pipeline on merge to `main` by @juditnovak in https://github.com/juditnovak/airflow-provider-nomad/pull/6
* Logging finalized by @juditnovak in https://github.com/juditnovak/airflow-provider-nomad/pull/7
* Base documentation by @juditnovak in https://github.com/juditnovak/airflow-provider-nomad/pull/8
* Modification on logging by @juditnovak in https://github.com/juditnovak/airflow-provider-nomad/pull/9
* Enabling secure Nomad connections by @juditnovak in https://github.com/juditnovak/airflow-provider-nomad/pull/10
* Configuration parameter `default_job_template` by @juditnovak in https://github.com/juditnovak/airflow-provider-nomad/pull/11
* Handling Job submission errors by @juditnovak in https://github.com/juditnovak/airflow-provider-nomad/pull/13

**Full Changelog**: https://github.com/juditnovak/airflow-provider-nomad/commits/0.0.1
**Release Notes**: https://github.com/juditnovak/airflow-provider-nomad/releases/tag/0.0.1


Commits
########

.. include:: commits.rst
