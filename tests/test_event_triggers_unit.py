import pytest
from pydantic import ValidationError

from app.schemas.triggers import EventSubscriptionCreate
from app.services.triggers import _matches


def test_exact_top_level_filters_are_deterministic() -> None:
    assert _matches(None, {"environment": "prod"})
    assert _matches({"environment": "prod"}, {"environment": "prod"})
    assert _matches({"environment": "prod", "severity": "critical"}, {"environment": "prod", "severity": "critical"})
    assert not _matches({"environment": "prod"}, {})
    assert not _matches({"environment": "prod"}, {"environment": "dev"})
    assert not _matches({"enabled": True}, {"enabled": 1})
    assert _matches({"value": None}, {"value": None})


def test_nested_filter_values_are_rejected() -> None:
    with pytest.raises(ValidationError):
        EventSubscriptionCreate(
            workflow_id="workflow",
            event_type="deployment.completed",
            filter_json={"environment": {"name": "prod"}},
        )
