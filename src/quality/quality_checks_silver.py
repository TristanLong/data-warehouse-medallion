"""Spark SQL quality checks based on tests/quality_checks_silver.sql."""

from pyspark.sql import SparkSession

from src.quality.checks import _register_tables, _run_checks, _show_distinct_values
from src.utils.spark_session import get_spark_session, stop_spark_session


def check_silver(spark: SparkSession) -> None:
    """Run Silver quality checks; print failing rows and raise if checks fail."""
    _register_tables(
        spark,
        "silver",
        (
            "crm_cust_info",
            "crm_prd_info",
            "crm_sales_details",
            "erp_cust_az12",
            "erp_loc_a101",
            "erp_px_cat_g1v2",
        ),
    )
    _register_tables(spark, "bronze", ("crm_sales_details",))

    checks = (
        (
            "Customer ID is not null or duplicated",
            """
            SELECT cst_id, COUNT(*) AS row_count
            FROM silver_crm_cust_info
            GROUP BY cst_id
            HAVING cst_id IS NULL OR COUNT(*) > 1
            """,
        ),
        (
            "Customer number has no surrounding whitespace",
            """
            SELECT cst_key
            FROM silver_crm_cust_info
            WHERE cst_key != TRIM(cst_key)
            """,
        ),
        (
            "Product ID is not null or duplicated",
            """
            SELECT prd_id, COUNT(*) AS row_count
            FROM silver_crm_prd_info
            GROUP BY prd_id
            HAVING prd_id IS NULL OR COUNT(*) > 1
            """,
        ),
        (
            "Product name has no surrounding whitespace",
            """
            SELECT prd_nm
            FROM silver_crm_prd_info
            WHERE prd_nm != TRIM(prd_nm)
            """,
        ),
        (
            "Product cost is present and non-negative",
            """
            SELECT prd_id, prd_cost
            FROM silver_crm_prd_info
            WHERE prd_cost IS NULL OR prd_cost < 0
            """,
        ),
        (
            "Product end date is not before start date",
            """
            SELECT prd_id, prd_start_dt, prd_end_dt
            FROM silver_crm_prd_info
            WHERE prd_end_dt < prd_start_dt
            """,
        ),
        (
            "Sales source dates have valid YYYYMMDD values",
            """
            SELECT sls_ord_num, sls_order_dt, sls_ship_dt, sls_due_dt
            FROM bronze_crm_sales_details
            WHERE (sls_order_dt IS NOT NULL AND
                   (LENGTH(CAST(sls_order_dt AS STRING)) != 8
                    OR sls_order_dt <= 0
                    OR sls_order_dt < 19000101 OR sls_order_dt > 20500101))
               OR (sls_ship_dt IS NOT NULL AND
                   (LENGTH(CAST(sls_ship_dt AS STRING)) != 8
                    OR sls_ship_dt <= 0
                    OR sls_ship_dt < 19000101 OR sls_ship_dt > 20500101))
               OR (sls_due_dt IS NOT NULL AND
                   (LENGTH(CAST(sls_due_dt AS STRING)) != 8
                    OR sls_due_dt <= 0
                    OR sls_due_dt < 19000101 OR sls_due_dt > 20500101))
            """,
        ),
        (
            "Order date is not after shipping or due date",
            """
            SELECT sls_ord_num, sls_order_dt, sls_ship_dt, sls_due_dt
            FROM silver_crm_sales_details
            WHERE sls_order_dt > sls_ship_dt
               OR sls_order_dt > sls_due_dt
            """,
        ),
        (
            "Sales amount equals quantity times price",
            """
            SELECT sls_ord_num, sls_sales, sls_quantity, sls_price
            FROM silver_crm_sales_details
            WHERE sls_sales IS NULL
               OR sls_quantity IS NULL
               OR sls_price IS NULL
               OR sls_sales <= 0
               OR sls_quantity <= 0
               OR sls_price <= 0
               OR sls_sales != sls_quantity * sls_price
            """,
        ),
        (
            "Customer birthdate is within the expected range",
            """
            SELECT cid, bdate
            FROM silver_erp_cust_az12
            WHERE bdate < DATE '1924-01-01'
               OR bdate > CURRENT_DATE()
            """,
        ),
        (
            "Category text fields have no surrounding whitespace",
            """
            SELECT id, cat, subcat, maintenance
            FROM silver_erp_px_cat_g1v2
            WHERE cat != TRIM(cat)
               OR subcat != TRIM(subcat)
               OR maintenance != TRIM(maintenance)
            """,
        ),
    )
    _run_checks(spark, "Silver", checks)

    _show_distinct_values(
        spark,
        "Silver standardization values",
        (
            ("Customer marital status", "silver_crm_cust_info", "cst_marital_status"),
            ("Customer gender", "silver_crm_cust_info", "cst_gndr"),
            ("Product line", "silver_crm_prd_info", "prd_line"),
            ("ERP customer gender", "silver_erp_cust_az12", "gen"),
            ("Country", "silver_erp_loc_a101", "cntry"),
            ("Maintenance", "silver_erp_px_cat_g1v2", "maintenance"),
        ),
    )


def main() -> None:
    spark = get_spark_session()
    try:
        check_silver(spark)
    finally:
        stop_spark_session()


if __name__ == "__main__":
    main()
