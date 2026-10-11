"""Gold layer: build business-ready dimension and fact tables with Spark SQL."""

from datetime import datetime
from time import perf_counter

from pyspark.sql import DataFrame, SparkSession

from src.config.settings import jdbc_properties, jdbc_url
from src.utils.etl_logging import LoadStatus, log_load_end, log_load_start


def load_gold(spark: SparkSession) -> None:
    """Build Gold dimensions and fact data, then full-load SQL Server tables."""
    print("=" * 48)
    print("Loading Gold Layer")
    print("=" * 48)

    _register_silver_views(spark)

    customers = _build_customers(spark)
    _load_gold_table(customers, "dim_customers_view")
    customers.createOrReplaceTempView("gold_dim_customers")

    products = _build_products(spark)
    _load_gold_table(products, "dim_products_view")
    products.createOrReplaceTempView("gold_dim_products")

    sales = _build_sales(spark)
    _load_gold_table(sales, "fact_sales_view")


def _register_silver_views(spark: SparkSession) -> None:
    for table in (
        "crm_cust_info",
        "erp_cust_az12",
        "erp_loc_a101",
        "crm_prd_info",
        "erp_px_cat_g1v2",
        "crm_sales_details",
    ):
        (
            spark.read.jdbc(
                jdbc_url(), f"silver.{table}", properties=jdbc_properties()
            )
            .createOrReplaceTempView(f"silver_{table}")
        )


def _build_customers(spark: SparkSession) -> DataFrame:
    return spark.sql(
        """
        SELECT
            CAST(ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS INT) AS customer_key,
            ci.cst_id AS customer_id,
            ci.cst_key AS customer_number,
            ci.cst_firstname AS first_name,
            ci.cst_lastname AS last_name,
            la.cntry AS country,
            ci.cst_marital_status AS marital_status,
            CASE
                WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
                ELSE COALESCE(ca.gen, 'n/a')
            END AS gender,
            ca.bdate AS birthdate,
            ci.cst_create_date AS create_date
        FROM silver_crm_cust_info ci
        LEFT JOIN silver_erp_cust_az12 ca
            ON ci.cst_key = ca.cid
        LEFT JOIN silver_erp_loc_a101 la
            ON ci.cst_key = la.cid
        """
    )


def _build_products(spark: SparkSession) -> DataFrame:
    return spark.sql(
        """
        SELECT
            CAST(
                ROW_NUMBER() OVER (
                    ORDER BY pn.prd_start_dt, pn.prd_key
                ) AS INT
            ) AS product_key,
            pn.prd_id AS product_id,
            pn.prd_key AS product_number,
            pn.prd_nm AS product_name,
            pn.cat_id AS category_id,
            pc.cat AS category,
            pc.subcat AS subcategory,
            pc.maintenance AS maintenance,
            pn.prd_cost AS cost,
            pn.prd_line AS product_line,
            pn.prd_start_dt AS start_date
        FROM silver_crm_prd_info pn
        LEFT JOIN silver_erp_px_cat_g1v2 pc
            ON pn.cat_id = pc.id
        WHERE pn.prd_end_dt IS NULL
        """
    )


def _build_sales(spark: SparkSession) -> DataFrame:
    return spark.sql(
        """
        SELECT
            sd.sls_ord_num AS order_number,
            pr.product_key AS product_key,
            cu.customer_key AS customer_key,
            sd.sls_order_dt AS order_date,
            sd.sls_ship_dt AS shipping_date,
            sd.sls_due_dt AS due_date,
            sd.sls_sales AS sales_amount,
            sd.sls_quantity AS quantity,
            sd.sls_price AS price
        FROM silver_crm_sales_details sd
        LEFT JOIN gold_dim_products pr
            ON sd.sls_prd_key = pr.product_number
        LEFT JOIN gold_dim_customers cu
            ON sd.sls_cust_id = cu.customer_id
        """
    )


def _load_gold_table(df: DataFrame, table: str) -> None:
    target_table = f"gold.{table}"
    start_time = datetime.now().astimezone()
    start_counter = perf_counter()
    status: LoadStatus = "FAILED"
    log_load_start("gold", "silver", table, target_table, start_time)

    try:
        (
            df.write.mode("overwrite")
            .option("truncate", "true")
            .jdbc(jdbc_url(), target_table, properties=jdbc_properties())
        )
        status = "SUCCESS"
    except Exception as exc:
        raise RuntimeError(f"Failed to load Gold table {target_table}") from exc
    finally:
        end_time = datetime.now().astimezone()
        log_load_end(
            "gold",
            "silver",
            table,
            target_table,
            start_time,
            end_time,
            status,
            perf_counter() - start_counter,
        )
