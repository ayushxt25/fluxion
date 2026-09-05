from pydantic import BaseModel, ConfigDict, Field, ValidationError

from app.engine.exceptions import InvalidDispatchMessageError


class TaskDispatchMessage(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    version: int = Field(default=1)
    workflow_id: str
    run_id: str
    task_id: str
    attempt_number: int
    attempt_key: str
    idempotency_key: str

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, payload: str | bytes) -> "TaskDispatchMessage":
        try:
            message = cls.model_validate_json(payload)
        except ValidationError as exc:
            raise InvalidDispatchMessageError("payload is not valid JSON.") from exc
        if message.version != 1:
            raise InvalidDispatchMessageError(
                f"unsupported message version {message.version}."
            )
        expected_attempt_key = (
            f"{message.run_id}:{message.task_id}:{message.attempt_number}"
        )
        if message.attempt_key != expected_attempt_key:
            raise InvalidDispatchMessageError("attempt_key does not match identity.")
        expected_idempotency_key = f"{message.run_id}:{message.task_id}"
        if message.idempotency_key != expected_idempotency_key:
            raise InvalidDispatchMessageError(
                "idempotency_key does not match identity."
            )
        return message
