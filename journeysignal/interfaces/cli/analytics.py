"""Offline analysis only; no dataframe work in the live processor."""

from importlib import import_module
from pathlib import Path

import polars as pl


def analyse_samples(path: Path, use_duckdb: bool = False) -> dict[str, float | int]:
    if path.stat().st_size > 16 * 1024 * 1024:
        raise ValueError("Sample file exceeds bounded offline analysis budget")
    frame = pl.read_ndjson(path)
    latencies = frame.get_column("processing_ms")
    result: dict[str, float | int] = {
        "records": frame.height,
        "processing_p50_ms": float(latencies.quantile(0.5) or 0),
        "processing_p95_ms": float(latencies.quantile(0.95) or 0),
    }
    if use_duckdb:
        duckdb = import_module("duckdb")

        # Parameterised file input; no user SQL or identifiers are interpolated.
        with duckdb.connect() as connection:
            row = connection.execute(
                "SELECT count(*) FROM read_ndjson_auto(?)", [str(path)]
            ).fetchone()
            if row is None:
                raise RuntimeError("Analytical count returned no row")
            result["duckdb_records"] = int(row[0])
    return result
