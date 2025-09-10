from airflow.exceptions import AirflowException


class NomadProviderException(AirflowException):
    pass


class NomadValidationError(NomadProviderException):
    """Used when can't parse Nomad job input."""
