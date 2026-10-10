"""Project-local Ivy cache for Spark packages (the JDBC driver jar) and a helper to clear it.

The cache folder comes from SPARK_IVY_DIR (relative to the repo root). Keeping it inside the
project means clear_spark_cache() never touches ~/.ivy2* used by other tools.
Call clear_spark_cache() only after the SparkSession has been stopped.
"""

import shutil
from pathlib import Path

from src.config.settings import get_env

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def ivy_dir() -> Path:
    return (PROJECT_ROOT / get_env("SPARK_IVY_DIR")).resolve()


def clear_spark_cache() -> None:
    """Delete the project-local Ivy cache. The jar is downloaded again on the next run."""
    target = ivy_dir()
    if PROJECT_ROOT not in target.parents:
        raise RuntimeError(f"Refusing to delete {target}: SPARK_IVY_DIR must be inside the project")
    shutil.rmtree(target, ignore_errors=True)
