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

from enum import Enum
from typing import Any, TypeAlias
import logging

from pydantic import BaseModel, ConfigDict, TypeAdapter, model_validator
from typing_extensions import Self


logger = logging.getLogger(__name__)


class JobType(str, Enum):
    batch = "batch"
    service = "service"


class JobInfoStatus(str, Enum):
    dead = "dead"
    pending = "pending"
    running = "running"


class JobEvalStatus(str, Enum):
    complete = "complete"
    blocked = "blocked"
    pending = "pending"
    canceled = "canceled"
    failed = "failed"

    @classmethod
    def done_states(cls) -> list["JobEvalStatus"]:
        return [cls.failed, cls.canceled, cls.complete]


class Resource(BaseModel):
    model_config = ConfigDict(extra="allow", validate_assignment=True)

    CPU: int | None = None
    MemoryMB: int | None = 256
    DiskMB: int | None = None
    Cores: int | None = None
    MemoryMaxMB: int | None = None
    Networks: list[Any] | None = None
    Devices: list[Any] | None = None
    MBits: int | None = None

    @model_validator(mode="after")
    def compatible_fields(self) -> Self:
        if self.CPU and self.Cores:
            raise ValueError("Both CPU and Cores can't be specified")
        if self.DiskMB and self.DiskMB > 0:
            logger.warning(
                "According to https://github.com/hashicorp/nomad/issues/10009#issuecomment-777445224,"
                " DiskMB is not supported"
            )
            self.DiskMB = None
        return self


class NomadEphemeralDisk(BaseModel):
    Migrate: bool | None = None
    SizeMB: int | None = None
    Sticky: bool | None = None


class TaskConfig(BaseModel):
    model_config = ConfigDict(extra="allow", validate_assignment=True)

    image: str | None = None
    entrypoint: list[str] | None = None
    args: list[str] | None = None
    command: str | list[str] | None = None

    @model_validator(mode="after")
    def compatible_fields(self) -> Self:
        if self.entrypoint and self.command:
            raise ValueError("Both 'entrypoint' and 'command' specified")
        return self


class Volume(BaseModel):
    model_config = ConfigDict(extra="allow", validate_assignment=True)

    AccessMode: str = ""
    AttachmentMode: str = ""
    MountOptions: Any | None = None
    Name: str
    PerAlloc: bool
    ReadOnly: bool
    Source: str
    Sticky: bool
    Type: str


NomadVolList: TypeAlias = dict[str, Volume]
NomadVolumes = TypeAdapter(NomadVolList)


class VolumeMount(BaseModel):
    model_config = ConfigDict(extra="allow", validate_assignment=True)

    Destination: str
    PropagationMode: str
    ReadOnly: bool
    SELinuxLabel: str = ""
    Volume: str

    def from_volumes(self, volumes: NomadVolList) -> bool:
        return self.Volume in volumes


NomadVolMntList: TypeAlias = list[VolumeMount]
NomadVolumeMounts = TypeAdapter(NomadVolMntList)


class Task(BaseModel):
    model_config = ConfigDict(extra="allow", validate_assignment=True)

    Config: TaskConfig
    Name: str
    Resources: Resource | None = None
    Driver: str
    Env: dict[str, str] | None = None
    VolumeMounts: NomadVolMntList | None = None


class TaskGroup(BaseModel):
    model_config = ConfigDict(extra="allow", validate_assignment=True)

    Tasks: list[Task]
    Name: str
    Count: int | None = None
    Volumes: NomadVolList | None = None
    EphemeralDisk: NomadEphemeralDisk | None = None


class Job(BaseModel):
    model_config = ConfigDict(extra="allow", validate_assignment=True)
    TaskGroups: list[TaskGroup]
    ID: str
    Name: str
    Namespace: str | None = None
    Type: JobType


class NomadJobModel(BaseModel):
    model_config = ConfigDict(extra="allow", validate_assignment=True)
    Job: Job

    def tasknames(self) -> list[str]:
        return [task.Name for group in self.Job.TaskGroups for task in group.Tasks]


class NomadFailedAllocInfo(BaseModel):
    AllocationTime: int
    ClassExhausted: Any | None = None
    ClassFiltered: Any | None = None
    CoalescedFailures: int
    ConstraintFiltered: dict[str, int] | None = None
    DimensionExhausted: dict[str, int] | None = None
    NodePool: str
    NodesAvailable: dict[str, int]
    NodesEvaluated: int
    NodesExhausted: int
    NodesFiltered: int
    NodesInPool: int
    QuotaExhausted: Any | None = None
    ResourcesExhausted: dict[str, dict] | None = None
    ScoreMetaData: Any | None = None
    Scores: Any | None = None

    def errors(self) -> list[Any]:
        """Turn an evaluation failure record into an error message"""
        errors: set[Any] = set()
        if self.ClassExhausted:
            errors.add(str(self.ClassExhausted))
        if self.ClassFiltered:
            errors.add(str(self.ClassFiltered))
        if self.ConstraintFiltered:
            errors.add(str(self.ConstraintFiltered))
        if self.DimensionExhausted:
            errors.add(str(self.DimensionExhausted))
        if self.QuotaExhausted:
            errors.add(str(self.QuotaExhausted))
        if self.ResourcesExhausted:
            errors.add(str(self.ResourcesExhausted))
        return list(errors)


class NomadJobEvaluationInfo(BaseModel):
    BlockedEval: str | None = None
    ClassEligibility: dict[str, bool] | None = None
    CreateIndex: int
    CreateTime: int
    FailedTGAllocs: dict[str, NomadFailedAllocInfo] | None = None
    ID: str
    JobID: str
    JobModifyIndex: int | None = None
    ModifyIndex: int
    ModifyTime: int
    Namespace: str
    PreviousEval: str | None = None
    Priority: int
    SnapshotIndex: int | None = None
    Status: JobEvalStatus
    StatusDescription: str | None = None
    TriggeredBy: str
    Type: JobType


NomadJobEvalList: TypeAlias = list[NomadJobEvaluationInfo]
NomadJobEvaluation = TypeAdapter(NomadJobEvalList)


class NomadJobSubmission(BaseModel):
    model_config = ConfigDict(extra="allow", validate_assignment=True)

    Status: JobInfoStatus
    Namespace: str
    ID: str
    Name: str
    Type: JobType
    Priority: int
    AllAtOnce: bool
    Datacenters: list[str]
    NodePool: str
    TaskGroups: list[TaskGroup]


class NomadEvent(BaseModel):
    Details: dict
    DiskLimit: int
    DisplayMessage: str
    DownloadError: str
    DriverError: str
    DriverMessage: str
    ExitCode: int
    FailedSibling: str
    FailsTask: bool
    GenericSource: str
    KillError: str
    KillReason: str
    KillTimeout: int
    Message: str
    RestartReason: str
    SetupError: str
    Signal: int
    StartDelay: int
    TaskSignal: str
    TaskSignalReason: str
    Time: int
    Type: str
    ValidationError: str
    VaultError: str


class NomadTaskState(BaseModel):
    model_config = ConfigDict(extra="allow", validate_assignment=True)

    Events: list[NomadEvent]
    Failed: bool
    State: str


class NomadJobAllocationInfo(BaseModel):
    model_config = ConfigDict(extra="allow", validate_assignment=True)

    ClientStatus: str
    EvalID: str
    FollowupEvalID: str
    ID: str
    JobID: str
    JobType: JobType
    JobVersion: int
    Name: str
    Namespace: str
    NextAllocation: str
    NodeID: str
    TaskGroup: str
    TaskStates: dict[str, NomadTaskState]

    def errors(self) -> dict[str, dict[str, list[Any]]]:
        """Turn an evaluation failure record into an error message"""
        errors: dict[str, dict[str, list[Any]]] = {}
        for task in self.TaskStates:
            errors[task] = {}
            for event in self.TaskStates[task].Events:
                errors[task][event.Type] = []
                if event.DownloadError:
                    errors[task][event.Type].append(event.DownloadError)
                if event.DriverError:
                    errors[task][event.Type].append(event.DriverError)
                if event.DriverError:
                    errors[task][event.Type].append(event.DriverError)
                if event.KillError:
                    errors[task][event.Type].append(event.KillError)
                if event.KillReason:
                    errors[task][event.Type].append(event.KillReason)
                if event.ValidationError:
                    errors[task][event.Type].append(event.ValidationError)
                if event.VaultError:
                    errors[task][event.Type].append(event.VaultError)
                if not errors[task][event.Type]:
                    errors[task].pop(event.Type)
            if not errors[task]:
                errors.pop(task)
        return errors


NomadJobAllocList: TypeAlias = list[NomadJobAllocationInfo]
NomadJobAllocations = TypeAdapter(NomadJobAllocList)


class NomadChildrenSummary(BaseModel):
    Dead: int
    Pending: int
    Running: int


class NomadJobSummaryInfo(BaseModel):
    Complete: int
    Failed: int
    Lost: int
    Queued: int
    Running: int
    Starting: int
    Unknown: int


class NomadJobSummary(BaseModel):
    model_config = ConfigDict(extra="allow", validate_assignment=True)

    Children: NomadChildrenSummary
    JobID: str
    Namespace: str
    Summary: dict[str, NomadJobSummaryInfo]

    def all_failed(self) -> bool:
        for taskgroup in self.Summary.values():
            if not (
                (taskgroup.Failed > 0 or taskgroup.Lost > 0 or taskgroup.Unknown > 0)
                and taskgroup.Complete == 0
                and taskgroup.Queued == 0
                and taskgroup.Running == 0
                and taskgroup.Starting == 0
            ):
                return False
        return True

    def all_done(self) -> bool:
        for taskgroup in self.Summary.values():
            if not (
                (taskgroup.Failed > 0 or taskgroup.Lost > 0 or taskgroup.Complete > 0)
                and taskgroup.Queued == 0
                and taskgroup.Running == 0
                and taskgroup.Starting == 0
                and taskgroup.Unknown == 0
            ):
                return False
        return True
