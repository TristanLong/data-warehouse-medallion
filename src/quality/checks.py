"""Shared helpers for Spark SQL data quality checks."""

from collections.abc import Sequence

from pyspark.sql import SparkSession

from src.config.settings import jdbc_properties, jdbc_url

Check = tuple[str, str]


def _register_tables(
    spark: SparkSession,
    schema: str,
    tables: Sequence[str],
) -> None:
    for table in tables:
        (
            spark.read.jdbc(
                jdbc_url(),
                f"{schema}.{table}",
                properties=jdbc_properties(),
            )
            .createOrReplaceTempView(f"{schema}_{table}")
        )


def _run_checks(spark: SparkSession, layer: str, checks: Sequence[Check]) -> None:
    print(f"\n{'=' * 48}\nRunning {layer} quality checks\n{'=' * 48}")
    failures: list[str] = []

    for name, query in checks:
        result = spark.sql(query)
        count = result.count()
        if count:
            failures.append(name)
            print(f"[FAIL] {name}: {count} row(s)")
            result.show(20, truncate=False)
        else:
            print(f"[PASS] {name}")

    if failures:
        failed_checks = ", ".join(failures)
        raise RuntimeError(f"{layer} quality checks failed: {failed_checks}")


def _show_distinct_values(
    spark: SparkSession,
    heading: str,
    columns: Sequence[tuple[str, str, str]],
) -> None:
    print(f"\n{heading}:")
    for label, table, column in columns:
        print(f"- {label}")
        spark.sql(
            f"SELECT DISTINCT `{column}` AS value FROM `{table}` ORDER BY value"
        ).show(truncate=False)
