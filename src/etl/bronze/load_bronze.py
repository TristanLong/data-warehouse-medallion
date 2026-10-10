"""Bronze layer: load raw source CSVs (CRM, ERP) as-is into bronze.* tables.

Full load (truncate and insert), no cleaning. Run data/scripts/bronze/ddl_bronze.sql first so
the tables exist. CSV columns are mapped by position, so header names don't matter.
"""

from pathlib import Path

from pyspark.sql import SparkSession

from src.config.settings import jdbc_properties, jdbc_url

DATASETS = Path(__file__).resolve().parents[3] / "data" / "datasets"


def load_csv(spark: SparkSession, source: str, filename: str, table: str, schema: str) -> None:
    """Read data/datasets/<source>/<filename> with the given schema and full-load it into table."""
    df = spark.read.csv(str(DATASETS / source / filename), header=True, schema=schema)
    (
        df.write.mode("overwrite")
        .option("truncate", "true")
        .jdbc(jdbc_url(), table, properties=jdbc_properties())
    )
    print(f"Loaded {filename} -> {table}")


def load_bronze(spark: SparkSession) -> None:
    load_csv(
        spark, "source_crm", "cust_info.csv", "bronze.crm_cust_info",
        "cst_id INT, cst_key STRING, cst_firstname STRING, cst_lastname STRING, "
        "cst_marital_status STRING, cst_gndr STRING, cst_create_date DATE",
    )
    load_csv(
        spark, "source_crm", "prd_info.csv", "bronze.crm_prd_info",
        "prd_id INT, prd_key STRING, prd_nm STRING, prd_cost INT, prd_line STRING, "
        "prd_start_dt DATE, prd_end_dt DATE",
    )
    load_csv(
        spark, "source_crm", "sales_details.csv", "bronze.crm_sales_details",
        "sls_ord_num STRING, sls_prd_key STRING, sls_cust_id INT, sls_order_dt INT, "
        "sls_ship_dt INT, sls_due_dt INT, sls_sales INT, sls_quantity INT, sls_price INT",
    )
    load_csv(spark, "source_erp", "LOC_A101.csv", "bronze.erp_loc_a101", "cid STRING, cntry STRING")
    load_csv(
        spark, "source_erp", "CUST_AZ12.csv", "bronze.erp_cust_az12",
        "cid STRING, bdate DATE, gen STRING",
    )
    load_csv(
        spark, "source_erp", "PX_CAT_G1V2.csv", "bronze.erp_px_cat_g1v2",
        "id STRING, cat STRING, subcat STRING, maintenance STRING",
    )
