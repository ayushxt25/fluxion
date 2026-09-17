import hashlib
import ipaddress
import json
import logging
import socket
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse
from uuid import uuid4

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.events import RunEventRecord
from app.db.models.webhooks import WebhookDeliveryRecord, WebhookSubscriptionRecord
from app.security.webhooks import sign_webhook_payload

SUPPORTED_WEBHOOK_EVENTS = frozenset(
    {"run.succeeded", "run.failed", "run.cancelled", "task.failed"}
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WebhookSubscription:
    id: str
    name: str
    target_url: str
    enabled: bool
    event_types: tuple[str, ...]
    workflow_id: str | None
    secret_configured: bool = True


@dataclass(frozen=True)
class WebhookDelivery:
    id: str
    subscription_id: str
    run_event_id: int
    status: str
    attempt_count: int
    claim_token: str | None = None
    target_url: str | None = None
    secret: str | None = None
    event: dict | None = None


@dataclass(frozen=True)
class WebhookDeliveryInfo:
    id: str
    subscription_id: str
    run_event_id: int
    status: str
    attempt_count: int
    next_attempt_at: datetime | None
    last_status_code: int | None
    last_error_type: str | None
    delivered_at: datetime | None


class WebhookRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_subscription(
        self,
        name: str,
        target_url: str,
        secret: str,
        event_types: tuple[str, ...],
        workflow_id: str | None = None,
    ) -> WebhookSubscription:
        _validate_events(event_types)
        if not name.strip() or not secret:
            raise ValueError("Webhook name and secret are required.")
        row = WebhookSubscriptionRecord(
            id=str(uuid4()),
            name=name,
            target_url=target_url,
            secret=secret,
            enabled=True,
            event_types=list(event_types),
            workflow_id=workflow_id,
        )
        async with self._session.begin():
            self._session.add(row)
        return _subscription(row)

    async def list_subscriptions(self) -> tuple[WebhookSubscription, ...]:
        async with self._session.begin():
            return tuple(
                _subscription(x)
                for x in (
                    await self._session.execute(
                        select(WebhookSubscriptionRecord).order_by(
                            WebhookSubscriptionRecord.id
                        )
                    )
                ).scalars()
            )

    async def get_subscription(self, subscription_id: str) -> WebhookSubscription:
        async with self._session.begin():
            row = await self._session.get(WebhookSubscriptionRecord, subscription_id)
            if row is None:
                raise KeyError(subscription_id)
            return _subscription(row)

    async def update_subscription(
        self,
        subscription_id: str,
        *,
        name: str | None = None,
        target_url: str | None = None,
        event_types: tuple[str, ...] | None = None,
        workflow_id: str | None = None,
        secret: str | None = None,
    ) -> WebhookSubscription:
        if event_types is not None:
            _validate_events(event_types)
        async with self._session.begin():
            row = await self._session.get(WebhookSubscriptionRecord, subscription_id)
            if row is None:
                raise KeyError(subscription_id)
            if name is not None:
                if not name.strip():
                    raise ValueError("Webhook name is required.")
                row.name = name
            if target_url is not None:
                row.target_url = target_url
            if event_types is not None:
                row.event_types = list(event_types)
            if workflow_id is not None:
                row.workflow_id = workflow_id
            if secret is not None:
                if not secret:
                    raise ValueError("Webhook secret is required.")
                row.secret = secret
            await self._session.flush()
            return _subscription(row)

    async def list_deliveries(
        self, subscription_id: str, limit: int = 100
    ) -> tuple[WebhookDeliveryInfo, ...]:
        async with self._session.begin():
            rows = await self._session.execute(
                select(WebhookDeliveryRecord)
                .where(WebhookDeliveryRecord.subscription_id == subscription_id)
                .order_by(WebhookDeliveryRecord.created_at.desc())
                .limit(limit)
            )
            return tuple(_delivery_info(row) for row in rows.scalars())

    async def get_delivery(self, delivery_id: str) -> WebhookDeliveryInfo:
        async with self._session.begin():
            row = await self._session.get(WebhookDeliveryRecord, delivery_id)
            if row is None:
                raise KeyError(delivery_id)
            return _delivery_info(row)

    async def disable(self, subscription_id: str) -> None:
        async with self._session.begin():
            row = await self._session.get(WebhookSubscriptionRecord, subscription_id)
            if row is None:
                raise KeyError(subscription_id)
            row.enabled = False

    async def reconcile(self, limit: int = 100) -> int:
        async with self._session.begin():
            subscriptions = tuple(
                (
                    await self._session.execute(
                        select(WebhookSubscriptionRecord).where(
                            WebhookSubscriptionRecord.enabled.is_(True)
                        )
                    )
                ).scalars()
            )
            events = tuple(
                (
                    await self._session.execute(
                        select(RunEventRecord).order_by(RunEventRecord.id)
                    )
                ).scalars()
            )
            count = 0
            for event in events:
                for sub in subscriptions:
                    if (
                        event.event_type not in sub.event_types
                        or (sub.workflow_id and sub.workflow_id != event.workflow_id)
                        or event.created_at < sub.created_at
                    ):
                        continue
                    exists = await self._session.scalar(
                        select(WebhookDeliveryRecord.id).where(
                            WebhookDeliveryRecord.subscription_id == sub.id,
                            WebhookDeliveryRecord.run_event_id == event.id,
                        )
                    )
                    if exists is not None:
                        continue
                    self._session.add(
                        WebhookDeliveryRecord(
                            id=str(uuid4()),
                            subscription_id=sub.id,
                            run_event_id=event.id,
                            delivery_key=hashlib.sha256(
                                f"{sub.id}:{event.id}".encode()
                            ).hexdigest(),
                            status="PENDING",
                            attempt_count=0,
                            next_attempt_at=datetime.now(UTC),
                        )
                    )
                    count += 1
            return count

    async def claim_due(
        self, worker_id: str, claim_seconds: float, limit: int
    ) -> tuple[WebhookDelivery, ...]:
        now = datetime.now(UTC)
        async with self._session.begin():
            query = (
                select(WebhookDeliveryRecord, WebhookSubscriptionRecord, RunEventRecord)
                .join(
                    WebhookSubscriptionRecord,
                    WebhookDeliveryRecord.subscription_id
                    == WebhookSubscriptionRecord.id,
                )
                .join(
                    RunEventRecord,
                    WebhookDeliveryRecord.run_event_id == RunEventRecord.id,
                )
                .where(
                    WebhookDeliveryRecord.status.in_(
                        ("PENDING", "RETRY_WAITING", "CLAIMED")
                    )
                )
                .where(
                    (WebhookDeliveryRecord.next_attempt_at.is_(None))
                    | (WebhookDeliveryRecord.next_attempt_at <= now)
                )
                .where(
                    (WebhookDeliveryRecord.claim_token.is_(None))
                    | (WebhookDeliveryRecord.claim_expires_at < now)
                )
                .order_by(WebhookDeliveryRecord.created_at, WebhookDeliveryRecord.id)
                .with_for_update(skip_locked=True)
                .limit(limit)
            )
            result = await self._session.execute(query)
            claimed = []
            for row, sub, event in result.tuples():
                token = str(uuid4())
                row.status = "CLAIMED"
                row.claimed_by = worker_id
                row.claim_token = token
                row.claim_expires_at = now + timedelta(seconds=claim_seconds)
                claimed.append(
                    WebhookDelivery(
                        row.id,
                        row.subscription_id,
                        row.run_event_id,
                        row.status,
                        row.attempt_count,
                        token,
                        sub.target_url,
                        sub.secret,
                        _event_payload(event),
                    )
                )
            return tuple(claimed)

    async def complete(
        self, delivery_id: str, worker_id: str, token: str, status_code: int
    ) -> None:
        async with self._session.begin():
            row = await self._claimed(delivery_id, worker_id, token)
            row.status = "DELIVERED"
            row.attempt_count += 1
            row.delivered_at = datetime.now(UTC)
            row.last_status_code = status_code
            row.last_error_type = row.last_error_message = None
            _clear_claim(row)

    async def retry_or_dead(
        self,
        delivery_id: str,
        worker_id: str,
        token: str,
        *,
        status_code: int | None,
        error_type: str,
        error_message: str,
        next_attempt_at: datetime,
        max_attempts: int,
    ) -> str:
        async with self._session.begin():
            row = await self._claimed(delivery_id, worker_id, token)
            row.attempt_count += 1
            row.last_status_code = status_code
            row.last_error_type = error_type
            row.last_error_message = error_message[:2048]
            row.status = (
                "DEAD" if row.attempt_count >= max_attempts else "RETRY_WAITING"
            )
            row.next_attempt_at = None if row.status == "DEAD" else next_attempt_at
            _clear_claim(row)
            return row.status

    async def _claimed(
        self, delivery_id: str, worker_id: str, token: str
    ) -> WebhookDeliveryRecord:
        row = await self._session.get(WebhookDeliveryRecord, delivery_id)
        if (
            row is None
            or row.status != "CLAIMED"
            or row.claimed_by != worker_id
            or row.claim_token != token
            or row.claim_expires_at is None
            or row.claim_expires_at < datetime.now(UTC)
        ):
            raise RuntimeError("Webhook delivery claim was lost.")
        return row


class WebhookDeliveryService:
    def __init__(
        self,
        repository: WebhookRepository,
        *,
        worker_id: str | None = None,
        claim_seconds: float = 30,
        max_attempts: int = 8,
        initial_backoff_seconds: float = 1,
        backoff_multiplier: float = 2,
        max_backoff_seconds: float = 300,
        allow_insecure_http: bool = False,
        allow_private_networks: bool = False,
        timeout_seconds: float = 10,
    ) -> None:
        self.repository, self.worker_id, self.claim_seconds, self.max_attempts = (
            repository,
            worker_id or str(uuid4()),
            claim_seconds,
            max_attempts,
        )
        (
            self.initial_backoff_seconds,
            self.backoff_multiplier,
            self.max_backoff_seconds,
        ) = initial_backoff_seconds, backoff_multiplier, max_backoff_seconds
        self.allow_insecure_http, self.allow_private_networks, self.timeout_seconds = (
            allow_insecure_http,
            allow_private_networks,
            timeout_seconds,
        )

    async def deliver_pending(self, limit: int = 100) -> tuple[int, int, int]:
        await self.repository.reconcile(limit)
        rows = await self.repository.claim_due(
            self.worker_id, self.claim_seconds, limit
        )
        delivered = retried = dead = 0
        async with httpx.AsyncClient(
            follow_redirects=False, timeout=self.timeout_seconds
        ) as client:
            for row in rows:
                try:
                    outcome = await self._deliver_one(client, row)
                except Exception:
                    logger.exception("Webhook delivery state update failed.")
                    continue
                delivered += outcome == "DELIVERED"
                retried += outcome == "RETRY_WAITING"
                dead += outcome == "DEAD"
        return delivered, retried, dead

    async def _deliver_one(
        self, client: httpx.AsyncClient, row: WebhookDelivery
    ) -> str:
        assert row.target_url and row.secret and row.claim_token and row.event
        try:
            validate_webhook_target(
                row.target_url, self.allow_insecure_http, self.allow_private_networks
            )
            body = json.dumps(row.event, separators=(",", ":"), sort_keys=True).encode()
            timestamp = str(int(time.time()))
            response = await client.post(
                row.target_url,
                content=body,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "Fluxion-Webhooks/1",
                    "X-Fluxion-Delivery": row.id,
                    "X-Fluxion-Event": str(row.event["event_type"]),
                    "X-Fluxion-Timestamp": timestamp,
                    "X-Fluxion-Signature": sign_webhook_payload(
                        body, timestamp, row.secret
                    ),
                },
            )
            if 200 <= response.status_code < 300:
                await self.repository.complete(
                    row.id, self.worker_id, row.claim_token, response.status_code
                )
                return "DELIVERED"
            return await self._failed(
                row,
                response.status_code,
                "http_status",
                f"HTTP {response.status_code}",
                _retry_after(response.headers.get("Retry-After")),
                terminal=400 <= response.status_code < 500
                and response.status_code not in {408, 429},
            )
        except Exception as exc:
            return await self._failed(row, None, type(exc).__name__, str(exc), None)

    async def _failed(
        self,
        row: WebhookDelivery,
        code: int | None,
        error_type: str,
        message: str,
        retry_after: float | None,
        terminal: bool = False,
    ) -> str:
        delay = min(
            self.max_backoff_seconds,
            (
                retry_after
                if retry_after is not None
                else min(
                    self.max_backoff_seconds,
                    self.initial_backoff_seconds
                    * self.backoff_multiplier**row.attempt_count,
                )
            ),
        )
        return await self.repository.retry_or_dead(
            row.id,
            self.worker_id,
            row.claim_token or "",
            status_code=code,
            error_type=error_type,
            error_message=message,
            next_attempt_at=datetime.now(UTC) + timedelta(seconds=max(0, delay)),
            max_attempts=1 if terminal else self.max_attempts,
        )


def validate_webhook_target(
    url: str, allow_insecure_http: bool = False, allow_private_networks: bool = False
) -> None:
    parsed = urlparse(url)
    if (
        parsed.scheme not in ({"https", "http"} if allow_insecure_http else {"https"})
        or not parsed.hostname
        or parsed.username
        or parsed.password
    ):
        raise ValueError("Webhook target must be an absolute safe HTTPS URL.")
    if not allow_private_networks:
        try:
            addresses = {
                entry[4][0]
                for entry in socket.getaddrinfo(
                    parsed.hostname,
                    parsed.port or (443 if parsed.scheme == "https" else 80),
                    type=socket.SOCK_STREAM,
                )
            }
        except socket.gaierror as exc:
            raise ValueError("Webhook target host cannot be resolved.") from exc
        if not addresses or any(
            not ipaddress.ip_address(address).is_global for address in addresses
        ):
            raise ValueError("Webhook target resolves to a non-public address.")


def _retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0, float(value))
    except ValueError:
        try:
            return max(
                0, (parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds()
            )
        except (TypeError, ValueError):
            return None


def _validate_events(events: tuple[str, ...]) -> None:
    if not events or set(events) - SUPPORTED_WEBHOOK_EVENTS:
        raise ValueError("Webhook event types are invalid.")


def _subscription(row: WebhookSubscriptionRecord) -> WebhookSubscription:
    return WebhookSubscription(
        row.id,
        row.name,
        row.target_url,
        row.enabled,
        tuple(row.event_types),
        row.workflow_id,
    )


def _event_payload(row: RunEventRecord) -> dict:
    return {
        "id": row.id,
        "version": row.version,
        "event_type": row.event_type,
        "workflow_id": row.workflow_id,
        "run_id": row.run_id,
        "task_id": row.task_id,
        "attempt_number": row.attempt_number,
        "created_at": row.created_at.isoformat(),
        "payload": row.payload,
    }


def _clear_claim(row: WebhookDeliveryRecord) -> None:
    row.claimed_by = row.claim_token = row.claim_expires_at = None


def _delivery_info(row: WebhookDeliveryRecord) -> WebhookDeliveryInfo:
    return WebhookDeliveryInfo(
        row.id,
        row.subscription_id,
        row.run_event_id,
        row.status,
        row.attempt_count,
        row.next_attempt_at,
        row.last_status_code,
        row.last_error_type,
        row.delivered_at,
    )
