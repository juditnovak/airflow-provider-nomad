from enum import Enum
from typing import Any, TypeAlias

from pydantic import BaseModel, ConfigDict, TypeAdapter, model_validator
from typing_extensions import Self


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
    model_config = ConfigDict(extra="allow")

    CPU: int | None = 500
    MemoryMB: int | None = 256


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


class Task(BaseModel):
    model_config = ConfigDict(extra="allow")
    Config: TaskConfig
    Name: str
    Resources: Resource | None = None


class TaskGroup(BaseModel):
    model_config = ConfigDict(extra="allow")
    Tasks: list[Task]
    Name: str


class Job(BaseModel):
    model_config = ConfigDict(extra="allow")
    TaskGroups: list[TaskGroup]
    ID: str
    Name: str
    Namespace: str | None = None
    Type: JobType


class NomadJobModel(BaseModel):
    model_config = ConfigDict(extra="allow")
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
        errors: list[Any] = []
        if self.ClassExhausted:
            errors.append(str(self.ClassExhausted))
        if self.ClassFiltered:
            errors.append(str(self.ClassFiltered))
        if self.ConstraintFiltered:
            errors.append(str(self.ConstraintFiltered))
        if self.DimensionExhausted:
            errors.append(str(self.DimensionExhausted))
        if self.QuotaExhausted:
            errors.append(str(self.QuotaExhausted))
        if self.ResourcesExhausted:
            errors.append(str(self.ResourcesExhausted))
        return errors


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
    model_config = ConfigDict(extra="allow")

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
    model_config = ConfigDict(extra="allow")

    Events: list[NomadEvent]
    Failed: bool
    State: str


class NomadJobAllocationInfo(BaseModel):
    model_config = ConfigDict(extra="allow")

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
    model_config = ConfigDict(extra="allow")

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
