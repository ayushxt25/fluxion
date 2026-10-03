from alembic.config import Config


def test_alembic_config_preserves_percent_encoded_database_url() -> None:
    database_url = (
        "postgresql+asyncpg://postgres.project:%40%40password@pooler:5432/postgres"
    )
    config = Config()

    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))

    assert config.get_main_option("sqlalchemy.url") == database_url
