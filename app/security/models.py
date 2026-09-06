from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class Role(StrEnum):
    VIEWER = "viewer"
    OPERATOR = "operator"
    ADMIN = "admin"


@dataclass(frozen=True)
class Principal:
    subject: str
    role: Role
    issued_at: datetime | None = None
    expires_at: datetime | None = None


ROLE_RANK = {
    Role.VIEWER: 1,
    Role.OPERATOR: 2,
    Role.ADMIN: 3,
}
