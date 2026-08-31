from pydantic import BaseModel, ConfigDict, Field


class TaskDefinition(BaseModel):
    id: str
    name: str | None = None
    depends_on: tuple[str, ...] = Field(default_factory=tuple)

    model_config = ConfigDict(frozen=True)


class WorkflowDefinition(BaseModel):
    id: str
    name: str
    tasks: tuple[TaskDefinition, ...]

    model_config = ConfigDict(frozen=True)
