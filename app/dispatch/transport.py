import asyncio
from typing import Protocol

from app.dispatch.messages import TaskDispatchMessage
from app.engine.exceptions import DispatchError


class TaskDispatcher(Protocol):
    async def dispatch(self, message: TaskDispatchMessage) -> None: ...

    async def receive(self, timeout: float | None = None) -> TaskDispatchMessage | None:
        ...


class InMemoryTaskDispatcher:
    def __init__(self) -> None:
        self._queue: asyncio.Queue[str] = asyncio.Queue()

    async def dispatch(self, message: TaskDispatchMessage) -> None:
        await self._queue.put(message.to_json())

    async def receive(self, timeout: float | None = None) -> TaskDispatchMessage | None:
        try:
            if timeout is None:
                payload = await self._queue.get()
            else:
                payload = await asyncio.wait_for(self._queue.get(), timeout)
        except TimeoutError:
            return None
        return TaskDispatchMessage.from_json(payload)


class RedisTaskDispatcher:
    def __init__(self, redis_url: str, queue_name: str) -> None:
        self._redis_url = redis_url
        self._queue_name = queue_name
        self._client = None

    async def _get_client(self):
        if self._client is None:
            from redis.asyncio import Redis

            self._client = Redis.from_url(self._redis_url, decode_responses=True)
        return self._client

    async def dispatch(self, message: TaskDispatchMessage) -> None:
        try:
            client = await self._get_client()
            await client.rpush(self._queue_name, message.to_json())
        except Exception as exc:
            raise DispatchError("Failed to publish dispatch message.") from exc

    async def receive(self, timeout: float | None = None) -> TaskDispatchMessage | None:
        try:
            client = await self._get_client()
            item = await client.blpop(self._queue_name, timeout=timeout or 0)
        except Exception as exc:
            raise DispatchError("Failed to receive dispatch message.") from exc
        if item is None:
            return None
        _, payload = item
        return TaskDispatchMessage.from_json(payload)

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
