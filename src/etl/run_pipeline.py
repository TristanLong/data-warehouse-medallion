"""Run the full medallion pipeline: bronze -> silver -> gold."""

from src.etl.bronze.load_bronze import load_bronze
from src.etl.gold.load_gold import load_gold
from src.etl.silver.load_silver import load_silver
from src.utils.spark_session import get_spark_session, stop_spark_session


def run_pipeline() -> None:
    spark = get_spark_session()
    try:
        load_bronze(spark)
        load_silver(spark)
        load_gold(spark)
    finally:
        stop_spark_session()
