# Data Warehouse Medallion Architecture

A clean, comprehensive, and simple implementation guide for a Data Warehouse using the **Medallion Architecture** (Bronze, Silver, Gold layers), ETL pipeline processes, and strict database object naming rules.

---

## 📌 Project Overview
This project provides a structured design for building a scalable Data Warehouse. The data moves through three medallion layers:
1. **Bronze Layer**: Raw, unrefined source data.
2. **Silver Layer**: Cleaned, standardized, and enriched intermediate data.
3. **Gold Layer**: Business-ready modeled views for reporting and analytics.

---

## 🏗️ Medallion Architecture Overview

| Feature | 🟤 Bronze Layer | ⚪ Silver Layer | 🟡 Gold Layer |
| :--- | :--- | :--- | :--- |
| **Definition** | Raw, unprocessed data direct from sources | Cleaned & standardized data | Business-ready aggregated data |
| **Objective** | Traceability & debugging | Prepare data for analysis | Data consumption for reporting & BI |
| **Object Type** | Tables | Tables | Views |
| **Load Method** | Full Load (Truncate & Insert) | Full Load (Truncate & Insert) | None (Dynamic Views) |
| **Data Transformation** | None (as-is) | Cleaning, standardization, normalization, derived columns, enrichment | Business rules, integration, aggregations |
| **Data Modeling** | None (as-is) | None (as-is) | Star Schema, Aggregated Tables, Flat Tables |
| **Target Audience** | Data Engineers, Data Analysts | Data Engineers, Data Analysts | Data Analysts, Business Users |

---

## 🔄 ETL Pipeline & Operations

### 1. Data Extraction
* **Extract Types**: Full Extraction, Incremental Extraction
* **Extraction Methods**: Pull Extraction, Push Extraction
* **Extraction Techniques**: Database Querying, File Parsing, API Calls, Event-Based Streaming, Change Data Capture (CDC), Web Scraping, Manual Data Extraction

### 2. Data Transformation
* **Cleansing**: Removing duplicates, data filtering, handling missing data, handling invalid values, trimming unwanted spaces, outlier detection, data type casting.
* **Standardization & Logic**: Normalization, derived columns, data integration across systems, business rules application.

### 3. Data Loading & SCD Handling
* **Processing Types**: Batch Processing, Stream Processing
* **Load Methods**:
  * **Full Load**: Truncate & Insert, Upsert, Drop/Create/Insert
  * **Incremental Load**: Append, Merge, Upsert
* **Slowly Changing Dimensions (SCD)**:
  * `SCD 0`: No historization
  * `SCD 1`: Overwrite existing records
  * `SCD 2`: Full historization (tracking changes over time)

---

## 🏷️ Naming Conventions & Standards

All database objects in this data warehouse follow a standardized naming standard to ensure consistency across teams.

### 1. General Principles
* **Style**: Use `snake_case` (lowercase letters with underscores `_`).
* **Language**: All object names must be in English.
* **Reserved Words**: Do not use SQL reserved keywords.

### 2. Table Naming Conventions
* **Bronze Layer**: `<sourcesystem>_<entity>`
  * *Example*: `crm_customer_info` (Raw customer table from CRM)
* **Silver Layer**: `<sourcesystem>_<entity>`
  * *Example*: `crm_customer_info` (Cleaned customer table from CRM)
* **Gold Layer**: `<category>_<entity>`
  * Category prefixes:
    * `dim_`: Dimension tables (*Example*: `dim_customers`)
    * `fact_`: Fact tables (*Example*: `fact_sales`)
    * `agg_`: Aggregated summary tables (*Example*: `agg_sales_monthly`)

### 3. Column Naming Conventions
* **Surrogate Keys**: Primary keys in dimension tables must end with `_key`.
  * *Pattern*: `<table_name>_key` (*Example*: `customer_key` in `dim_customers`)
* **Technical Metadata Columns**: System-generated columns must start with `dwh_`.
  * *Pattern*: `dwh_<column_name>` (*Example*: `dwh_load_date` for batch ingestion date)

### 4. Stored Procedure Naming Conventions
* Stored procedures for loading data layers must follow:
  * *Pattern*: `load_<layer>`
  * *Examples*: `load_bronze`, `load_silver`, `load_gold`

---

## 🛠️ Engineering Lifecycle & Data Quality

1. **Source System Exploration**: Understand data owners, business processes, storage engines, integration capabilities, and volume limits.
2. **Layer Ingestion**:
   * **Bronze**: Schema & completeness checks during ingestion.
   * **Silver**: Data correctness and cleansing validation.
   * **Gold**: Business integration checks.
3. **Documentation & Version Control**: Track all SQL scripts, data models, and catalogs using Git versioning.

---

## 🤝 Contributing
Feel free to open issues or pull requests. Please adhere strictly to the **Naming Conventions** specified above.
