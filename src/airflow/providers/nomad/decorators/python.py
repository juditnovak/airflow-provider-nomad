# This file is part of apache-airflow-providers-nomad which is
# released under Apache License 2.0. See file LICENSE or go to
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# for full license details.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


from __future__ import annotations

import re
import warnings
import inspect
from collections.abc import Collection, Mapping, Sequence
from typing import Any, Callable, ClassVar

from airflow.sdk import Context
from airflow.sdk.bases.decorator import DecoratedOperator, TaskDecorator, task_decorator_factory
from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION
from airflow.utils.context import context_merge
# from airflow.utils.operator_helpers import determine_kwargs

from airflow.providers.nomad.operators.python import NomadPythonTaskOperator


class _NomadPythonTaskDecoratedOperator(DecoratedOperator, NomadPythonTaskOperator):
    """
    Wraps a Python callable and uses the callable return value as the Docker 'args' to be executed.

    :param python_callable: A reference to an object that is callable.
    :param op_kwargs: A dictionary of keyword arguments that will get unpacked
        in your function (templated).
    :param op_args: A list of positional arguments that will get unpacked when
        calling your callable (templated).
    """

    template_fields: Sequence[str] = (  # type: ignore [reportIncompatibleVariableOverride]
        *DecoratedOperator.template_fields,
        *NomadPythonTaskOperator.template_fields,
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
        **NomadPythonTaskOperator.template_fields_renderers,
    }

    custom_operator_name: str = "@task.nomad"
    overwrite_rtif_after_execution: bool = True

    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None:
        if kwargs.pop("multiple_outputs", None):
            warnings.warn(
                f"`multiple_outputs=True` is not supported in {self.custom_operator_name} tasks. Ignoring.",
                UserWarning,
                stacklevel=3,
            )

        if args := kwargs.pop("args", []):
            warnings.warn(
                "Use 'args' for Nomad Decorator with caution. Note that the output of the decorated function "
                + "is passed concatenated to 'args'",
                UserWarning,
                stacklevel=3,
            )

        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            args=args,
            multiple_outputs=False,
            python_command=SET_DURING_EXECUTION,
            **kwargs,
        )

    def _remove_docstring_from_source(self, lines: list[str]):
        if self.python_callable.__doc__:
            firstline = lines[0].strip()
            quotes = r"('''|\"\"\")"
            pat = re.compile(quotes)
            if pat.match(firstline):
                lines.pop(0)
                while lines and not pat.search(lines[0]):
                    lines.pop(0)
                lines.pop(0)
            elif re.match(r"^['|\"]", firstline):
                lines.pop(0)

    def _remove_header(self, lines):
        while lines and not re.match(" *def ", lines[0]):
            lines.pop(0)
        if lines:
            lines.pop(0)

    def _untab_lines(self, header: str, body_lines: list[str]) -> list[str]:
        spaces = ""
        if m := re.match(r"(?P<spaces>\s*)", header):
            spaces = m.groupdict().get("spaces", "")

        rmstr = spaces + " " * 4
        return [line.removeprefix(rmstr) for line in body_lines]

    def get_callable_source(self, fn: Callable) -> str | None:
        if not (source := inspect.getsource(fn)):
            self.log.error("No Python executable was found")
            return  # type: ignore[return-value]

        if not (lines := source.split("\n")):
            self.log.error(f"No Python executable {source} body was found")
            return  # type: ignore[return-value]

        if len(lines) == 1:
            lines = source.split(";")
            if not len(lines) > 1:
                self.log.error(
                    f"Python executable {lines} seems to consist of no more but a header"
                )
                return  # type: ignore[return-value]

        if not (head := lines[0]):
            self.log.error("Function definition seems wrong, header is empty")
            return  # type: ignore[return-value]

        self._remove_header(lines)
        self._remove_docstring_from_source(lines)
        lines = self._untab_lines(head, lines)

        return "\n".join(lines)

    def execute(self, context: Context) -> Any:
        context_merge(context, self.op_kwargs)
        # kwargs = determine_kwargs(self.python_callable, self.op_args, context)

        if not (cmd := self.get_callable_source(self.python_callable)):
            self.log.error("Nothing to execute")
            return  # type: ignore[return-value]

        self.python_command = cmd

        context["ti"].render_templates()  # type: ignore[attr-defined]

        return super().execute(context)


def nomad_python_task(
    python_callable: Callable | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Wrap a function into a NomadPythonTaskOperator.

    Accepts kwargs for operator kwargs. Can be reused in a single DAG. This function is only used only used
    during type checking or auto-completion.

    :param python_callable: Function to decorate.

    :meta private:
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_NomadPythonTaskDecoratedOperator,
        **kwargs,
    )
