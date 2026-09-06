from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.audit import AuditEventRecord
from app.security.models import Principal


@dataclass(frozen=True)
class AuditEvent:
    id: str
    occurred_at: datetime
    request_id: str
    principal_subject: str | None
    principal_role: str | None
    action: str
    resource_type: str | None
    resource_id: str | None
    outcome: str
    metadata: dict | None


class AuditEventRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def record(
        self,
        *,
        request_id: str,
        principal: Principal | None,
        action: str,
        outcome: str,
        resource_type: str | None = None,
        resource_id: str | None = None,
        metadata: dict | None = None,
    ) -> None:
        async with self._session.begin():
            self._session.add(
                AuditEventRecord(
                    id=str(uuid4()),
                    request_id=request_id,
                    principal_subject=principal.subject if principal else None,
                    principal_role=principal.role.value if principal else None,
                    action=action,
                    resource_type=resource_type,
                    resource_id=resource_id,
                    outcome=outcome,
                    event_metadata=_sanitize_metadata(metadata),
                )
            )

    async def list(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        action: str | None = None,
        principal_subject: str | None = None,
        outcome: str | None = None,
    ) -> tuple[AuditEvent, ...]:
        async with self._session.begin():
            query = select(AuditEventRecord)
            if action is not None:
                query = query.where(AuditEventRecord.action == action)
            if principal_subject is not None:
                query = query.where(
                    AuditEventRecord.principal_subject == principal_subject
                )
            if outcome is not None:
                query = query.where(AuditEventRecord.outcome == outcome)
            result = await self._session.execute(
                query.order_by(
                    AuditEventRecord.occurred_at.desc(),
                    AuditEventRecord.id.desc(),
                )
                .limit(limit)
                .offset(offset)
            )
            return tuple(_event_from_record(record) for record in result.scalars())


class AuditService:
    def __init__(self, repository: AuditEventRepository) -> None:
        self._repository = repository

    async def record_success(
        self,
        *,
        request_id: str,
        principal: Principal | None,
        action: str,
        resource_type: str | None = None,
        resource_id: str | None = None,
        metadata: dict | None = None,
    ) -> None:
        await self._repository.record(
            request_id=request_id,
            principal=principal,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            outcome="SUCCESS",
            metadata=metadata,
        )

    async def record_denied(
        self,
        *,
        request_id: str,
        principal: Principal | None,
        action: str,
        resource_type: str | None = None,
        resource_id: str | None = None,
        metadata: dict | None = None,
    ) -> None:
        await self._repository.record(
            request_id=request_id,
            principal=principal,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            outcome="DENIED",
            metadata=metadata,
        )


def _event_from_record(record: AuditEventRecord) -> AuditEvent:
    return AuditEvent(
        id=record.id,
        occurred_at=record.occurred_at,
        request_id=record.request_id,
        principal_subject=record.principal_subject,
        principal_role=record.principal_role,
        action=record.action,
        resource_type=record.resource_type,
        resource_id=record.resource_id,
        outcome=record.outcome,
        metadata=record.event_metadata,
    )


def _sanitize_metadata(metadata: dict | None) -> dict | None:
    if metadata is None:
        return None
    blocked = {"authorization", "token", "jwt", "jwt_secret", "lease_token"}
    return {key: value for key, value in metadata.items() if key.lower() not in blocked}
