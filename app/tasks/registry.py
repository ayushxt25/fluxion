from app.engine.registry import TaskRegistry


def build_task_registry() -> TaskRegistry:
    """Return task implementations available to this worker process."""
    return TaskRegistry({})
