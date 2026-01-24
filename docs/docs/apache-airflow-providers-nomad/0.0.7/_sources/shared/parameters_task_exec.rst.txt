Execution
*************

``env (dict[str, str])``: Environment variables specified as a Python dictionary. This will update any existing environment that may already exist in the Nomad job specification

``image (str)``: The docker image to be run

``entrypoint (list[str])``: Entrypoint to the Docker image (incompatible with ``command``)

``command (dict[str])``: Command to be run by the Docker image (incompatible with ``entrypoint``)
