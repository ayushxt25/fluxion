from app.engine.exceptions import TaskParameterResolutionError
from app.engine.results import JSONValue, clone_json_value
from app.schemas.workflow import (
    DependencyResultParameter,
    LiteralParameter,
    TaskDefinition,
    WorkflowInputParameter,
)


def resolve_task_parameters(
    task: TaskDefinition,
    *,
    workflow_input: JSONValue,
    workflow_input_present: bool,
    dependency_results: dict[str, JSONValue],
) -> dict[str, JSONValue]:
    resolved: dict[str, JSONValue] = {}
    for name, parameter in task.parameters.items():
        try:
            if isinstance(parameter, LiteralParameter):
                value = parameter.value
            elif isinstance(parameter, WorkflowInputParameter):
                if not workflow_input_present:
                    raise KeyError("workflow input was not supplied")
                value = _resolve_path(workflow_input, parameter.path)
            elif isinstance(parameter, DependencyResultParameter):
                if parameter.task_id not in dependency_results:
                    raise KeyError("dependency result is not available")
                value = _resolve_path(
                    dependency_results[parameter.task_id],
                    parameter.path,
                )
            else:  # pragma: no cover - Pydantic prevents this.
                raise KeyError("unsupported parameter source")
        except (KeyError, IndexError, TypeError) as exc:
            raise TaskParameterResolutionError(task.id, name, str(exc)) from exc
        resolved[name] = clone_json_value(value)
    return resolved


def _resolve_path(value: JSONValue, path: tuple[str | int, ...]) -> JSONValue:
    current = value
    for component in path:
        if isinstance(component, str):
            if not isinstance(current, dict) or component not in current:
                raise KeyError(f"path component '{component}' was not found")
            current = current[component]
        else:
            if not isinstance(current, list):
                raise TypeError(f"path component {component} requires a list")
            current = current[component]
    return clone_json_value(current)
