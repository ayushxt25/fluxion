from app.engine.registry import TaskRegistry
from app.tasks.demo import build_demo_tasks


def build_task_registry() -> TaskRegistry:
    """Return task implementations available to this worker process."""
    return TaskRegistry(build_demo_tasks())
