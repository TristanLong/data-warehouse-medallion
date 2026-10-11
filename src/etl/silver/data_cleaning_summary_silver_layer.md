# 🛠️ Silver Layer ETL Pipeline - Data Cleaning Summary

This document summarizes the data cleaning and transformation rules applied when moving data from the **Bronze** layer to the **Silver** layer. 

## 📌 Global Rules

*Applied automatically to **ALL** text (string) columns in every table.*

| Action | Details |
| :--- | :--- |
| **Trim Spaces** | Removes extra spaces at the beginning and end of texts. |
| **Handle Empty Strings** | Converts empty texts (`""`) to `NULL`. |

---

## 📊 1. CRM Data (Customer Relationship Management)

| Source Table | Target Column | Cleaning Logic |
| :--- | :--- | :--- |
| **`crm_cust_info`** | *(Whole table)* | **Remove Duplicates:** Keeps only the newest record for each customer (based on `cst_create_date`). Ignores records with no ID. |
| | `cst_marital_status` | **Standardize:** Maps 'S' to 'Single', 'M' to 'Married'. Other values become 'n/a'. |
| | `cst_gndr` | **Standardize:** Maps 'F' to 'Female', 'M' to 'Male'. Other values become 'n/a'. |
| **`crm_prd_info`** | `prd_key`, `cat_id` | **Split & Format:** Splits the original ID into a category ID (replaces `-` with `_`) and a new product key. |
| | `prd_cost` | **Fill Missing:** Changes `NULL` costs to `0`. |
| | `prd_line` | **Standardize:** Maps 'M' $\rightarrow$ 'Mountain', 'R' $\rightarrow$ 'Road', 'S' $\rightarrow$ 'Other Sales', 'T' $\rightarrow$ 'Touring'. Others become 'n/a'. |
| | `prd_end_dt` | **Calculate Date:** Automatically calculates the end date by subtracting 1 day from the next product's start date. |
| **`crm_sales_details`**| Date columns (order, ship, due) | **Format Dates:** Converts raw numbers/texts (YYYYMMDD) to a standard `DATE` format (YYYY-MM-DD). Invalid dates become `NULL`. |
| | `sls_sales` | **Math Check:** If total sales is wrong, negative, or `NULL`, recalculates it as: `Quantity × Absolute Price`. |
| | `sls_price` | **Math Check:** If price is negative or `NULL`, recalculates it as: `Total Sales ÷ Quantity`. |

---

## 🏢 2. ERP Data (Enterprise Resource Planning)

| Source Table | Target Column | Cleaning Logic |
| :--- | :--- | :--- |
| **`erp_cust_az12`** | `cid` | **Format ID:** Removes the 'NAS' prefix if it exists. |
| | `bdate` | **Logic Check:** Changes future birthdates to `NULL`. |
| | `gen` | **Standardize:** Maps 'F' or 'FEMALE' to 'Female', 'M' or 'MALE' to 'Male'. Others become 'n/a'. |
| **`erp_loc_a101`** | `cid` | **Format ID:** Removes hyphens (`-`). |
| | `cntry` | **Standardize:** Maps 'DE' to 'Germany', 'US' or 'USA' to 'United States'. Empty or `NULL` values become 'n/a'. |
| **`erp_px_cat_g1v2`**| *(All Columns)* | **Basic Clean:** Only applies the Global Rules (Trim Spaces and Handle Empty Strings). No special logic. |