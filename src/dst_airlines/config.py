from pathlib import Path
import os

def get_project_root() -> Path:
    env_root = os.getenv("DST_AIRLINES_HOME")
    if env_root:
        return Path(env_root).resolve()

    cwd = Path.cwd().resolve()
    if (cwd / "pyproject.toml").exists():
        return cwd

    raise RuntimeError(
        "Project root not found. Set DST_AIRLINES_HOME or run from the project root."
    )

PROJECT_ROOT = get_project_root()
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
INCOMING_DIR = DATA_DIR / "incoming"
PROCESSED_DIR = DATA_DIR / "processed"