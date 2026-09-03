from pydantic import BaseModel, ConfigDict, Field


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


class TaskDefinition(BaseModel):
    id: str
    name: str | None = None
    depends_on: tuple[str, ...] = Field(default_factory=tuple)
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)

    model_config = ConfigDict(frozen=True)


class WorkflowDefinition(BaseModel):
    id: str
    name: str
    tasks: tuple[TaskDefinition, ...]

    model_config = ConfigDict(frozen=True)
