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

import time
from pathlib import Path

from airflow.configuration import conf
from airflow.models import BaseOperator
from airflow.sdk import Context
from nomad.api.exceptions import BaseNomadException  # type: ignore[import-untyped]

from airflow.providers.nomad.constants import CONFIG_SECTION
from airflow.providers.nomad.exceptions import NomadOperatorError
from airflow.providers.nomad.manager import NomadManager
from airflow.providers.nomad.models import (
    JobEvalStatus,
    JobInfoStatus,
    NomadJobAllocList,
    NomadJobModel,
)


class NomadOperator(BaseOperator):
    """Abstract class to provide shared functionality across Nomad Operators"""

    def __init__(self, observe: bool = True, job_log_file: str | None = None, **kwargs):
        self.nomad_mgr = NomadManager()
        self.nomad_mgr.initialize()
        self.observe = observe
        self.job_log_file = job_log_file
        self.operator_poll_delay: int = conf.getint(
            CONFIG_SECTION, "operator_poll_delay", fallback=10
        )
        self.runner_log_dir: str = conf.get(CONFIG_SECTION, "runner_log_dir", fallback="/tmp")
        self.template: NomadJobModel | None = None
        super().__init__(**kwargs)

    def sanitize_logs(self, alloc_id: str, task_name: str, logs: str) -> str:
        if not logs:
            return logs

        sanitized_logs = logs
        fileloc = Path(f"{self.runner_log_dir}/{alloc_id}-{task_name}.log")
        if fileloc.is_file():
            with fileloc.open("r") as file:
                prefix = file.read()
                sanitized_logs = logs[len(prefix) :]

        with fileloc.open("w") as file:
            file.write(logs)
            file.flush()

        return sanitized_logs

    def get_children_output(
        self, job_id, alloc_info: NomadJobAllocList | None = None
    ) -> tuple[str, str]:
        if not alloc_info and not (alloc_info := self.nomad_mgr.get_nomad_job_allocation(job_id)):
            return "", ""

        all_logs = ""
        all_output = []
        for allocation in alloc_info:
            for task_name in allocation.TaskStates:
                logs = ""
                if self.job_log_file:
                    logs += self.nomad_mgr.get_job_file(allocation.ID, self.job_log_file)

                if output := self.nomad_mgr.get_job_stdout(allocation.ID, task_name):
                    all_output.append(output.strip())
                    logs += output

                logs += self.nomad_mgr.get_job_stderr(allocation.ID, task_name)

                all_logs += self.sanitize_logs(allocation.ID, task_name, logs)

                for line in logs.splitlines():
                    self.log.info("[job: %s][alloc: %s] %s", job_id, allocation.ID, line)

        return ",".join(all_output), all_logs

    @staticmethod
    def figure_path(path_str: str):
        path = Path(path_str)
        if not path.is_absolute():
            path = Path(conf.get_mandatory_value("core", "dags_folder")) / path
        return path

    def prepare_job_template(self, context: Context):
        raise NotImplementedError

    def execute(self, context: Context):
        if not self.template:
            self.prepare_job_template(context)

        if not self.template:
            raise NomadOperatorError("Nothing to execute")

        job_id = self.template.Job.ID
        try:
            response = self.nomad_mgr.nomad.job.register_job(  # type: ignore[optionalMemberAccess, union-attr]
                job_id, self.template.model_dump(exclude_unset=True)
            )
        except BaseNomadException as err:
            raise NomadOperatorError(
                f"Job submission failed ({err}), job template: {self.template.model_dump_json()}"
            )

        if not response:
            self.log.warning("No response on job submission attempt, may suggest an error")

        if not self.observe:
            return

        status: JobInfoStatus | None = JobInfoStatus.pending
        job_info: list[str] = []
        logs = ""
        job_status, job_alloc, job_eval, job_summary = None, None, None, None
        job_snapshot = {}
        output = ""
        all_done = False
        while status != JobInfoStatus.dead and not all_done:
            job_info = []

            # Snapshotting state
            job_status = self.nomad_mgr.get_nomad_job_submission(job_id)
            job_alloc = self.nomad_mgr.get_nomad_job_allocation(job_id)
            job_eval = self.nomad_mgr.get_nomad_job_evaluations(job_id)
            job_summary = self.nomad_mgr.get_nomad_job_summary(job_id)
            job_snapshot = {
                "job_summary": job_summary,
                "job_alloc": job_alloc,
                "job_eval": job_eval,
            }

            output, logs = self.get_children_output(job_id, alloc_info=job_alloc)

            if not job_status or job_status.Status != JobInfoStatus.running:
                if (
                    result := self.nomad_mgr.remove_job_if_hanging(
                        job_id, ignore_dead=True, job_status=job_status, **job_snapshot
                    )
                ) and result[0]:
                    _, error = result
                    if job_info := self.nomad_mgr.job_all_info_str(job_id, **job_snapshot):
                        raise NomadOperatorError(
                            f"Job {job_id} got killed due to error: {error}\n"
                            "Additional info:\n"
                            "\n".join(job_info)
                        )
                    break

            time.sleep(self.operator_poll_delay)
            status = job_status.Status if job_status else None
            all_done = job_summary.all_done() if job_summary else False

        # Collecting final status
        job_alloc = self.nomad_mgr.get_nomad_job_allocation(job_id)
        job_eval = self.nomad_mgr.get_nomad_job_evaluations(job_id)
        job_summary = self.nomad_mgr.get_nomad_job_summary(job_id)
        job_info = self.nomad_mgr.job_all_info_str(
            job_id, job_alloc=job_alloc, job_eval=job_eval, job_summary=job_summary
        )
        job_info_str = "\n".join(job_info)
        if job_eval and any(evalu.Status == JobEvalStatus.complete for evalu in job_eval):
            if output:
                return output
            if job_summary and not job_summary.all_failed():
                return (
                    f"No output from job. Logs/stderr: {logs.splitlines()}\nJob info: \n"
                    + "\n".join(job_info)
                )

        raise NomadOperatorError(f"Job submission failed {job_info_str}")
