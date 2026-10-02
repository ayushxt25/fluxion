from app.version import SCHEMA_REVISION, __version__


def test_release_version_and_schema_revision_are_stable() -> None:
    assert __version__ == "1.0.0"
    assert SCHEMA_REVISION == "20261001_0024"
