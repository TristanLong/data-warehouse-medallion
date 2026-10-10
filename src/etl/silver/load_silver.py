"""Silver layer: clean and standardize bronze.* into silver.* tables.

Full load (truncate and insert): cleansing, standardization, derived columns.
"""

from pyspark.sql import SparkSession


def load_silver(spark: SparkSession) -> None:
    raise NotImplementedError
