"""Gold layer: business-ready star schema, built as views over silver.*.

Views live in data/scripts/gold/ddl_gold.sql (dim_customers, dim_products, fact_sales).
"""

from pyspark.sql import SparkSession


def load_gold(spark: SparkSession) -> None:
    raise NotImplementedError
