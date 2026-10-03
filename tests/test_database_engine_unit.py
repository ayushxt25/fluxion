from app.core.config import Settings
from app.db import engine as database_engine


def test_database_engine_uses_configured_bounded_pool(monkeypatch) -> None:
    captured: dict[str, object] = {}
    sentinel = object()

    def fake_create_async_engine(url: str, **kwargs: object) -> object:
        captured["url"] = url
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(
        database_engine,
        "create_async_engine",
        fake_create_async_engine,
    )
    settings = Settings(
        database_url="postgresql+asyncpg://user:pass@db:5432/fluxion",
        database_pool_size=2,
        database_max_overflow=0,
        database_pool_timeout_seconds=10,
    )

    assert database_engine.create_database_engine(settings) is sentinel
    assert captured == {
        "url": settings.database_url,
        "pool_size": 2,
        "max_overflow": 0,
        "pool_timeout": 10,
        "pool_pre_ping": True,
    }
