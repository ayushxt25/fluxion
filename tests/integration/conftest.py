import pytest


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Keep every test under this package in the integration CI lane."""
    for item in items:
        item.add_marker(pytest.mark.integration)
