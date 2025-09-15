from airflow.exceptions import AirflowException


class NomadProviderException(AirflowException):
    pass


class NomadValidationError(NomadProviderException):
    """Used when can't parse Nomad job input."""


class NomadOperatorError(NomadProviderException):
    """Errors for NomadJobOperator"""


class NomadJobOperatorError(NomadOperatorError):
    """Errors for NomadJobOperator"""


class NomadTaskOperatorError(NomadOperatorError):
    """Errors for NomadJobOperator"""
