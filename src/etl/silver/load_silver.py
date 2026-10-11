"""Silver layer: clean and standardize bronze.* into silver.* tables."""

from collections.abc import Callable
from datetime import datetime
from time import perf_counter

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType

from src.config.settings import jdbc_properties, jdbc_url
from src.utils.etl_logging import LoadStatus, log_load_end, log_load_start


def load_silver(spark: SparkSession) -> None:
    """Run the Silver transformations in source-table order."""
    print("=" * 48)
    print("Loading Silver Layer")
    print("=" * 48)

    crm_tables = (
        ("crm_cust_info", "silver.crm_cust_info", _clean_crm_customers),
        ("crm_prd_info", "silver.crm_prd_info", _clean_crm_products),
        ("crm_sales_details", "silver.crm_sales_details", _clean_crm_sales),
    )
    erp_tables = (
        ("erp_cust_az12", "silver.erp_cust_az12", _clean_erp_customers),
        ("erp_loc_a101", "silver.erp_loc_a101", _clean_erp_locations),
        ("erp_px_cat_g1v2", "silver.erp_px_cat_g1v2", _clean_erp_categories),
    )

    print("-" * 48)
    print("Loading CRM Tables")
    print("-" * 48)
    for source_table, target_table, clean in crm_tables:
        _load_silver_table(spark, source_table, target_table, clean)

    print("-" * 48)
    print("Loading ERP Tables")
    print("-" * 48)
    for source_table, target_table, clean in erp_tables:
        _load_silver_table(spark, source_table, target_table, clean)


def _read_bronze(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.jdbc(
        jdbc_url(), f"bronze.{table}", properties=jdbc_properties()
    )


def _trim_strings(df: DataFrame) -> DataFrame:
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            trimmed = F.trim(F.col(field.name))
            df = df.withColumn(
                field.name,
                F.when(trimmed == "", F.lit(None).cast(StringType())).otherwise(trimmed),
            )
    return df


def _load_silver_table(
    spark: SparkSession,
    source_table: str,
    target_table: str,
    clean: Callable[[SparkSession], DataFrame],
) -> None:
    start_time = datetime.now().astimezone()
    start_counter = perf_counter()
    status: LoadStatus = "FAILED"
    log_load_start("silver", "bronze", source_table, target_table, start_time)

    try:
        df = clean(spark)
        (
            df.write.mode("overwrite")
            .option("truncate", "true")
            .jdbc(jdbc_url(), target_table, properties=jdbc_properties())
        )
        status = "SUCCESS"
    except Exception as exc:
        raise RuntimeError(
            f"Failed to load bronze.{source_table} into {target_table}"
        ) from exc
    finally:
        end_time = datetime.now().astimezone()
        duration = perf_counter() - start_counter
        log_load_end(
            "silver",
            "bronze",
            source_table,
            target_table,
            start_time,
            end_time,
            status,
            duration,
        )


def _create_bronze_view(spark: SparkSession, table: str) -> str:
    view_name = f"bronze_{table}"
    _trim_strings(_read_bronze(spark, table)).createOrReplaceTempView(view_name)
    return view_name


def _clean_crm_customers(spark: SparkSession) -> DataFrame:
    view = _create_bronze_view(spark, "crm_cust_info")
    return spark.sql(
        f"""
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY cst_id
                    ORDER BY cst_create_date DESC NULLS LAST
                ) AS row_num
            FROM {view}
            WHERE cst_id IS NOT NULL
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE UPPER(TRIM(cst_marital_status))
                WHEN 'S' THEN 'Single'
                WHEN 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE UPPER(TRIM(cst_gndr))
                WHEN 'F' THEN 'Female'
                WHEN 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM ranked
        WHERE row_num = 1
        """
    )


def _clean_crm_products(spark: SparkSession) -> DataFrame:
    view = _create_bronze_view(spark, "crm_prd_info")
    return spark.sql(
        f"""
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
            prd_nm,
            COALESCE(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(
                DATE_SUB(
                    CAST(LEAD(prd_start_dt) OVER (
                        PARTITION BY prd_key ORDER BY prd_start_dt
                    ) AS DATE),
                    1
                ) AS DATE
            ) AS prd_end_dt
        FROM {view}
        """
    )


def _clean_crm_sales(spark: SparkSession) -> DataFrame:
    view = _create_bronze_view(spark, "crm_sales_details")
    return spark.sql(
        f"""
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE
                WHEN sls_order_dt = 0
                    OR LENGTH(CAST(sls_order_dt AS STRING)) != 8 THEN NULL
                ELSE TRY_CAST(
                    CONCAT(
                        SUBSTRING(CAST(sls_order_dt AS STRING), 1, 4), '-',
                        SUBSTRING(CAST(sls_order_dt AS STRING), 5, 2), '-',
                        SUBSTRING(CAST(sls_order_dt AS STRING), 7, 2)
                    ) AS DATE
                )
            END AS sls_order_dt,
            CASE
                WHEN sls_ship_dt = 0
                    OR LENGTH(CAST(sls_ship_dt AS STRING)) != 8 THEN NULL
                ELSE TRY_CAST(
                    CONCAT(
                        SUBSTRING(CAST(sls_ship_dt AS STRING), 1, 4), '-',
                        SUBSTRING(CAST(sls_ship_dt AS STRING), 5, 2), '-',
                        SUBSTRING(CAST(sls_ship_dt AS STRING), 7, 2)
                    ) AS DATE
                )
            END AS sls_ship_dt,
            CASE
                WHEN sls_due_dt = 0
                    OR LENGTH(CAST(sls_due_dt AS STRING)) != 8 THEN NULL
                ELSE TRY_CAST(
                    CONCAT(
                        SUBSTRING(CAST(sls_due_dt AS STRING), 1, 4), '-',
                        SUBSTRING(CAST(sls_due_dt AS STRING), 5, 2), '-',
                        SUBSTRING(CAST(sls_due_dt AS STRING), 7, 2)
                    ) AS DATE
                )
            END AS sls_due_dt,
            CASE
                WHEN sls_sales IS NULL
                    OR sls_sales <= 0
                    OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN CAST(sls_sales / NULLIF(sls_quantity, 0) AS INT)
                ELSE sls_price
            END AS sls_price
        FROM {view}
        """
    )


def _clean_erp_customers(spark: SparkSession) -> DataFrame:
    view = _create_bronze_view(spark, "erp_cust_az12")
    return spark.sql(
        f"""
        SELECT
            CASE
                WHEN UPPER(TRIM(cid)) LIKE 'NAS%'
                    THEN SUBSTRING(TRIM(cid), 4, LENGTH(TRIM(cid)))
                ELSE TRIM(cid)
            END AS cid,
            CASE
                WHEN bdate > CURRENT_DATE() THEN NULL
                ELSE bdate
            END AS bdate,
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM {view}
        """
    )


def _clean_erp_locations(spark: SparkSession) -> DataFrame:
    view = _create_bronze_view(spark, "erp_loc_a101")
    return spark.sql(
        f"""
        SELECT
            REPLACE(TRIM(cid), '-', '') AS cid,
            CASE
                WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
                WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM {view}
        """
    )


def _clean_erp_categories(spark: SparkSession) -> DataFrame:
    view = _create_bronze_view(spark, "erp_px_cat_g1v2")
    return spark.sql(
        f"""
        SELECT
            TRIM(id) AS id,
            TRIM(cat) AS cat,
            TRIM(subcat) AS subcat,
            TRIM(maintenance) AS maintenance
        FROM {view}
        """
    )
