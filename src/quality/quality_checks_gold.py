"""Spark SQL quality checks based on tests/quality_checks_gold.sql."""

from pyspark.sql import SparkSession

from src.quality.checks import _register_tables, _run_checks
from src.utils.spark_session import get_spark_session, stop_spark_session


def check_gold(spark: SparkSession) -> None:
    """Run Gold dimension-key and fact-to-dimension relationship checks."""
    _register_tables(
        spark,
        "gold",
        ("dim_customers_view", "dim_products_view", "fact_sales_view"),
    )
    checks = (
        (
            "Customer surrogate key is present and unique",
            """
            SELECT customer_key, COUNT(*) AS row_count
            FROM gold_dim_customers_view
            GROUP BY customer_key
            HAVING customer_key IS NULL OR COUNT(*) > 1
            """,
        ),
        (
            "Product surrogate key is present and unique",
            """
            SELECT product_key, COUNT(*) AS row_count
            FROM gold_dim_products_view
            GROUP BY product_key
            HAVING product_key IS NULL OR COUNT(*) > 1
            """,
        ),
        (
            "Every sales row resolves to a customer and product",
            """
            SELECT
                f.order_number,
                f.customer_key,
                f.product_key
            FROM gold_fact_sales_view f
            LEFT JOIN gold_dim_customers_view c
                ON c.customer_key = f.customer_key
            LEFT JOIN gold_dim_products_view p
                ON p.product_key = f.product_key
            WHERE c.customer_key IS NULL
               OR p.product_key IS NULL
            """,
        ),
    )
    _run_checks(spark, "Gold", checks)


def main() -> None:
    spark = get_spark_session()
    try:
        check_gold(spark)
    finally:
        stop_spark_session()


if __name__ == "__main__":
    main()
