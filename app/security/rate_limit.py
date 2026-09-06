from dataclasses import dataclass
from datetime import UTC, datetime

from app.core.config import Settings, get_settings
from app.security.auth import RateLimitExceededError, RateLimitUnavailableError
from app.security.models import Principal, Role


@dataclass(frozen=True)
class RateLimitDecision:
    limit: int
    remaining: int
    retry_after: int


class RedisRateLimiter:
    def __init__(self, redis_url: str, *, namespace: str = "fluxion:ratelimit") -> None:
        self._redis_url = redis_url
        self._namespace = namespace
        self._client = None

    async def _get_client(self):
        if self._client is None:
            from redis.asyncio import Redis

            self._client = Redis.from_url(self._redis_url, decode_responses=True)
        return self._client

    async def check(
        self,
        principal: Principal,
        *,
        scope: str,
        limit: int,
        now: datetime | None = None,
    ) -> RateLimitDecision:
        now = now or datetime.now(UTC)
        window = int(now.timestamp() // 60)
        key = f"{self._namespace}:{scope}:{principal.subject}:{window}"
        try:
            client = await self._get_client()
            count = await client.incr(key)
            if count == 1:
                await client.expire(key, 120)
        except Exception as exc:
            raise RateLimitUnavailableError() from exc
        retry_after = 60 - int(now.timestamp() % 60)
        remaining = max(limit - int(count), 0)
        if count > limit:
            raise RateLimitExceededError(retry_after, limit, remaining)
        return RateLimitDecision(
            limit=limit,
            remaining=remaining,
            retry_after=retry_after,
        )

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()


def limit_for_role(
    principal: Principal,
    *,
    ops: bool = False,
    settings: Settings | None = None,
) -> int:
    settings = settings or get_settings()
    if ops:
        return settings.rate_limit_ops_per_minute
    if principal.role == Role.ADMIN:
        return settings.rate_limit_admin_per_minute
    if principal.role == Role.OPERATOR:
        return settings.rate_limit_operator_per_minute
    return settings.rate_limit_viewer_per_minute
