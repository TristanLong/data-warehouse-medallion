"""Create (or reuse) the shared SparkSession.

Usage:
    from src.utils.spark_session import get_spark_session, stop_spark_session
    from src.config.settings import jdbc_url, jdbc_properties

    spark = get_spark_session()
    df = spark.read.jdbc(jdbc_url(), "gold.dim_customers", properties=jdbc_properties())
    stop_spark_session()

Variables (see .env.example): SPARK_APP_NAME, SPARK_MASTER,
SPARK_SHUFFLE_PARTITIONS, SPARK_IVY_DIR (local package cache, see spark_cache.py),
DB_JDBC_PACKAGE (Maven coordinate of the SQL Server
JDBC driver; Spark downloads it on the first run, so internet is needed once).
Spark itself also needs a JDK (JAVA_HOME).
"""

from pyspark.sql import SparkSession

from src.config.settings import get_env
from src.utils.spark_cache import ivy_dir


def get_spark_session() -> SparkSession:
    """Return the active SparkSession, creating it from env settings on first call."""
    return (
        SparkSession.builder.appName(get_env("SPARK_APP_NAME"))
        .master(get_env("SPARK_MASTER"))
        .config("spark.jars.ivy", str(ivy_dir()))
        .config("spark.jars.packages", get_env("DB_JDBC_PACKAGE"))
        .config("spark.sql.shuffle.partitions", get_env("SPARK_SHUFFLE_PARTITIONS"))
        .getOrCreate()
    )


def stop_spark_session() -> None:
    """Stop the active SparkSession, if any."""
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
