import pytest


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark only tests physically located in the integration package."""
    for item in items:
        if "tests/integration/" in str(item.path).replace("\\", "/"):
            item.add_marker(pytest.mark.integration)
