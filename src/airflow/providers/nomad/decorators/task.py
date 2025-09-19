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

import warnings
from collections.abc import Collection, Mapping, Sequence
from typing import Any, Callable, ClassVar

from airflow.sdk import Context
from airflow.sdk.bases.decorator import DecoratedOperator, TaskDecorator, task_decorator_factory
from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION
from airflow.utils.context import context_merge
from airflow.utils.operator_helpers import determine_kwargs

from airflow.providers.nomad.operators.task import NomadTaskOperator


class _NomadTaskDecoratedOperator(DecoratedOperator, NomadTaskOperator):
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
        *NomadTaskOperator.template_fields,
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
        **NomadTaskOperator.template_fields_renderers,
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

        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            args=SET_DURING_EXECUTION,
            multiple_outputs=False,
            **kwargs,
        )

    def execute(self, context: Context) -> Any:
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)

        self.args = self.python_callable(*self.op_args, **kwargs)

        if not self.args or not isinstance(self.args, list):
            raise TypeError(
                "The returned value from the TaskFlow callable must be a list of string(s)."
            )

        context["ti"].render_templates()  # type: ignore[attr-defined]

        return super().execute(context)


def nomad_task(
    python_callable: Callable | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Wrap a function into a NomadTaskOperator.

    Accepts kwargs for operator kwargs. Can be reused in a single DAG. This function is only used only used
    during type checking or auto-completion.

    :param python_callable: Function to decorate.

    :meta private:
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_NomadTaskDecoratedOperator,
        **kwargs,
    )
