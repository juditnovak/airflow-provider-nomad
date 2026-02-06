.. caution::

    The Nomad Task Operator (Decorator) works alongside the same concept as other Airflow operators, yet, the behavior may seem misleading first. Airflow users may be much used to the ``@task`` notation invoking the PythonOperator_, which complies to the "intuitive" expectation to execute the Operator function Python code block. 

    However Airflow Operators in general (for example: BashOperator_) *only execute the return value of the Operator function within the expected context* (in this case: in ``bash``).

    The same is true for ``NomadTaksOperator``, ``NomadJobOpratro`` (and corresponding decorators ``@task.nomad_task`` and ``@task.nomad_job``). (See :ref:`examples`, also highlighting on Python code submission to Nomad.)

.. _PythonOperator: https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/python.html

.. _BashOperator: https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/bash.html

