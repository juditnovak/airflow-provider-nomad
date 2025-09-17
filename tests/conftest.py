from __future__ import annotations

import os
import sys

pytest_plugins = "tests_common.pytest_plugin"

sys.path.append(os.path.join(os.path.dirname(__file__), "src"))
