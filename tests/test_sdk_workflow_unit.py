import pytest

from app.sdk import Retry, WorkflowBuilder, dependency_result, literal, workflow_input


def test_builder_serializes_parameter_helpers_and_retry() -> None:
    workflow = (
        WorkflowBuilder("score")
        .task("prepare", parameters={"customer": workflow_input("customer", "id")})
        .task(
            "score",
            depends_on=["prepare"],
            retry=Retry(max_attempts=3, initial_delay_seconds=1, max_delay_seconds=30),
            parameters={
                "prepared": dependency_result("prepare", "value"),
                "limit": literal(100),
            },
        )
        .build()
    )

    payload = workflow.model_dump(mode="json", by_alias=True)
    assert payload["tasks"][1]["retry_policy"]["initial_backoff_seconds"] == 1
    assert payload["tasks"][1]["parameters"]["prepared"] == {
        "source": "dependency_result",
        "task_id": "prepare",
        "path": ["value"],
    }


@pytest.mark.parametrize(
    "builder",
    [
        lambda: WorkflowBuilder("x").task("a").task("a"),
        lambda: WorkflowBuilder("x").task("a", depends_on=["missing"]).build(),
        lambda: WorkflowBuilder("x").task("a", depends_on=["a"]),
        lambda: WorkflowBuilder("x")
        .task("a", depends_on=["b"])
        .task("b", depends_on=["a"])
        .build(),
        lambda: WorkflowBuilder("x").task(
            "b",
            depends_on=["a"],
            parameters={"value": dependency_result("not-a")},
        ),
    ],
)
def test_builder_rejects_invalid_dags_and_parameter_dependencies(builder) -> None:
    with pytest.raises(ValueError):
        builder()


def test_builder_rejects_non_json_literal_and_invalid_parameter_name() -> None:
    with pytest.raises(ValueError):
        literal({"value": {1, 2}})
    with pytest.raises(ValueError):
        WorkflowBuilder("x").task("a", parameters={"not-valid": literal(1)})
