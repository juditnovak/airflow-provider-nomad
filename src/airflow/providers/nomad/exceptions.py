from airflow.exceptions import AirflowException


class NomadPRoviderException(AirflowException):
    pass


class NomadValidationError(NomadPRoviderException):
    """Used when can't parse Nomad job input."""
