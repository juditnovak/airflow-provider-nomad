# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
import multiprocessing
import select
import subprocess
import sys
import time
from pathlib import Path

import requests  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


def update_template(file_path: Path, replacements: dict[str, str]) -> None:
    """Update placeholders in a template file with actual values."""
    infile_path = f"{file_path}.template"
    outfile_path = file_path

    with open(infile_path, "r") as file:
        content = file.read()
    for placeholder, actual in replacements.items():
        content = content.replace(placeholder, actual)
    with open(outfile_path, "w") as file:
        file.write(content)
    logger.debug("Rendered template for %s", file_path)


def check_service_available(ip: str, port: int, protocol: str = "http", timeout: int = 10) -> bool:
    logger.info("Checking service availability at %s://%s:%d...", protocol, ip, port)
    for _ in range(timeout, 0, -1):
        try:
            response = requests.get(f"{protocol}://{ip}:{port}")
            if response.status_code == 200:
                return True
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(1)
    return False


def say_yes_or_no(timeout: int = 10):
    """NOTE: This function by design may raise exceptions such as 'select.error'."""

    def timer(timeout: int):
        for i in range(timeout, 0, -1):
            time.sleep(1)
            sys.stdout.write("\rWaiting {} seconds (y/n): ".format(i))
            sys.stdout.flush()

    userinput = None
    timer_proc = multiprocessing.Process(target=timer, args=(timeout,))
    timer_proc.start()

    answer, _, _ = select.select([sys.stdin], [], [], timeout)
    if answer:
        userinput = sys.stdin.readline()
        timer_proc.terminate()

    timer_proc.join()

    if userinput and userinput.strip().lower() in ["y", "yes"]:
        return True
    return False


def stream_subprocess_output(cmd: list[str]):
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
    if not popen or not popen.stdout:
        raise subprocess.SubprocessError(
            "Failed to run command (no process or no stdout). Command: {})".format(cmd)
        )

    for stdout_line in iter(popen.stdout.readline, ""):
        yield stdout_line
    if return_code := popen.wait():
        raise subprocess.CalledProcessError(return_code, cmd)
