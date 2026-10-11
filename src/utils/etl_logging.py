"""Logging helpers for ETL load operations."""

from datetime import datetime
from typing import Literal

LoadStatus = Literal["SUCCESS", "FAILED"]


def _format_timestamp(value: datetime) -> str:
    return value.isoformat(sep=" ", timespec="seconds")


def log_load_start(
    layer: str,
    source: str,
    filename: str,
    table: str,
    start_time: datetime,
) -> None:
    print(
        f"\n[{layer.upper()} LOAD START]\n"
        f"  Source : {source}/{filename}\n"
        f"  Target : {table}\n"
        f"  Started: {_format_timestamp(start_time)}"
    )


def log_load_end(
    layer: str,
    source: str,
    filename: str,
    table: str,
    start_time: datetime,
    end_time: datetime,
    status: LoadStatus,
    duration: float,
) -> None:
    print(
        f"[{layer.upper()} LOAD {status}]\n"
        f"  Source   : {source}/{filename}\n"
        f"  Target   : {table}\n"
        f"  Started  : {_format_timestamp(start_time)}\n"
        f"  Finished : {_format_timestamp(end_time)}\n"
        f"  Duration : {duration:.2f} seconds\n"
        f"{'-' * 60}"
    )
