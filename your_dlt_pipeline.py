from variables import *
# Import other required packages

# -----------------------------------------
# All table names
# -----------------------------------------

BRONZE_SALES = f"{BRONZE_SCHEMA}.bronze_sales"
BRONZE_CUSTOMERS = f"{BRONZE_SCHEMA}.bronze_customers"
BRONZE_PRODUCTS = f"{BRONZE_SCHEMA}.bronze_products"
BRONZE_STORES = f"{BRONZE_SCHEMA}.bronze_stores"

CUSTOMERS_CLEANED_STREAM = "customers_cleaned_stream"
PRODUCTS_CLEANED_STREAM = "products_cleaned_stream"
STORES_CLEANED_STREAM = "stores_cleaned_stream"
SALES_CLEANED_STREAM = "sales_cleaned_stream"

SILVER_CUSTOMERS = f"{SILVER_SCHEMA}.silver_customers"
SILVER_PRODUCTS = f"{SILVER_SCHEMA}.silver_products"
SILVER_STORES = f"{SILVER_SCHEMA}.silver_stores"
SILVER_SALES_TRANSACTIONS = f"{SILVER_SCHEMA}.silver_sales_transactions"
SILVER_RETURNS_TRANSACTIONS = f"{SILVER_SCHEMA}.silver_returns_transactions"

SILVER_CUSTOMERS_CURRENT = "silver_customers_current"
SILVER_PRODUCTS_CURRENT = "silver_products_current"
SILVER_STORES_CURRENT = "silver_stores_current"

GOLD_DENORMALIZED_SALES_FACTS = f"{GOLD_SCHEMA}.denormalized_sales_facts"
GOLD_DAILY_SALES_BY_STORE = f"{GOLD_SCHEMA}.gold_daily_sales_by_store"
GOLD_PRODUCT_PERFORMANCE = f"{GOLD_SCHEMA}.gold_product_performance"
GOLD_CUSTOMER_LIFETIME_VALUE = f"{GOLD_SCHEMA}.gold_customer_lifetime_value"

# -----------------------------------------
# 01 BRONZE: Raw Layer
# -----------------------------------------

# -----------------------------------------
# 02A SILVER: Cleaned Streams (Intermediate Views)
# -----------------------------------------


# -----------------------------------------
# 02B SILVER: Materialized SCD2 Business Dimension Tables
# (using create_auto_cdc_flow)
# -----------------------------------------


# -----------------------------------------
# 02C SILVER: Current State Views for Dimension Lookup
# (using __END_AT IS NULL for current, since create_auto_cdc_flow does not use _dlt_current)
# -----------------------------------------

# -----------------------------------------
# 03 GOLD: Denormalized Analytics and Aggregates
# -----------------------------------------
