from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    script = ScriptDirectory.from_config(config)
    heads = script.get_heads()
    if len(heads) != 1:
        raise SystemExit(f"Expected exactly one Alembic head, found: {heads}")
    for revision in script.walk_revisions(base="base", head=heads[0]):
        if revision.module is None:
            raise SystemExit(f"Could not import migration {revision.revision}.")
    print(f"Alembic chain is valid at head {heads[0]}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
