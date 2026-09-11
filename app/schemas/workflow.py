import keyword
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.engine.results import normalize_task_result

JSONPath = tuple[str | int, ...]


class RetryPolicy(BaseModel):
    max_attempts: int = Field(default=1, ge=1)
    initial_backoff_seconds: float = Field(default=0.0, ge=0)
    backoff_multiplier: float = Field(default=2.0, ge=1)
    max_backoff_seconds: float | None = Field(default=None, ge=0)

    model_config = ConfigDict(frozen=True)

    def delay_after_failure(self, attempt_number: int) -> float:
        delay = self.initial_backoff_seconds * (
            self.backoff_multiplier ** (attempt_number - 1)
        )
        if self.max_backoff_seconds is not None:
            return min(delay, self.max_backoff_seconds)
        return delay


class WorkflowInputParameter(BaseModel):
    source: Literal["workflow_input"]
    path: JSONPath = Field(default_factory=tuple)

    model_config = ConfigDict(frozen=True)


class DependencyResultParameter(BaseModel):
    source: Literal["dependency_result"]
    task_id: str
    path: JSONPath = Field(default_factory=tuple)

    model_config = ConfigDict(frozen=True)


class LiteralParameter(BaseModel):
    source: Literal["literal"]
    value: Any

    model_config = ConfigDict(frozen=True)

    @model_validator(mode="after")
    def validate_literal(self) -> "LiteralParameter":
        normalize_task_result(self.value, 262144)
        return self


TaskParameter = WorkflowInputParameter | DependencyResultParameter | LiteralParameter


class TaskDefinition(BaseModel):
    id: str
    name: str | None = None
    depends_on: tuple[str, ...] = Field(default_factory=tuple)
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    parameters: dict[str, TaskParameter] = Field(default_factory=dict)

    model_config = ConfigDict(frozen=True)

    @field_validator("parameters")
    @classmethod
    def validate_parameter_names(
        cls,
        value: dict[str, TaskParameter],
    ) -> dict[str, TaskParameter]:
        for name in value:
            if not name.isidentifier() or keyword.iskeyword(name):
                raise ValueError(
                    f"Task parameter '{name}' must be a Python identifier."
                )
        return value


class WorkflowDefinition(BaseModel):
    id: str
    name: str
    tasks: tuple[TaskDefinition, ...]

    model_config = ConfigDict(frozen=True)

    @model_validator(mode="after")
    def validate_parameter_dependencies(self) -> "WorkflowDefinition":
        for task in self.tasks:
            direct_dependencies = set(task.depends_on)
            for parameter in task.parameters.values():
                if (
                    isinstance(parameter, DependencyResultParameter)
                    and parameter.task_id not in direct_dependencies
                ):
                    raise ValueError(
                        f"Task '{task.id}' parameter references non-direct "
                        f"dependency '{parameter.task_id}'."
                    )
        return self
