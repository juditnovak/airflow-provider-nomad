from typing import Literal

from pydantic import BaseModel, ConfigDict


class Resource(BaseModel):
    model_config = ConfigDict(extra="allow")

    CPU: int | None = 500
    MemoryMB: int | None = 256


class TaskConfigImglessCmd(BaseModel):
    model_config = ConfigDict(extra="allow")

    command: str | list[str] | None = None


class TaskConfigCmd(BaseModel):
    model_config = ConfigDict(extra="allow")

    image: str
    command: str | list[str] | None = None


class TaskConfigEntrypoint(BaseModel):
    model_config = ConfigDict(extra="allow")

    image: str | None = None
    entrypoint: list[str]
    args: list[str] | None = None


class TaskConfigArgs(BaseModel):
    model_config = ConfigDict(extra="allow")

    image: str | None = None
    args: list[str] | None = None


class TaskConfigRaw(BaseModel):
    model_config = ConfigDict(extra="allow")

    image: str | None = None


class Task(BaseModel):
    model_config = ConfigDict(extra="allow")
    Config: (
        TaskConfigRaw | TaskConfigEntrypoint | TaskConfigArgs | TaskConfigCmd | TaskConfigImglessCmd
    )
    Name: str
    Resources: Resource


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
    Type: Literal["batch", "service"]


class NomadJobModel(BaseModel):
    model_config = ConfigDict(extra="allow")
    Job: Job
