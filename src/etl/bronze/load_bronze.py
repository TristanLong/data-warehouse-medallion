"""Bronze layer: load raw source CSVs (CRM, ERP) as-is into bronze.* tables.

Full load (truncate and insert), no transformation. Sources: data/datasets/.
"""

from pyspark.sql import SparkSession


def load_bronze(spark: SparkSession) -> None:
    raise NotImplementedError
