Template
************

``template_path (str)``: Path to a Nomad job JSON or HCL file. Otherwise the default nomad executor template is used.

``template_content (str)``: A JSON or HCL string, or a Python dictionary. Otherwise, the default Nomad Executor template is used.

    In case a template was specified, it must have a single ``TaskGroup`` with a single ``Task`` within,
    and can only have a single execution (``Count`` is ``1``).
