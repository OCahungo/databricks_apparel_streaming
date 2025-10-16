from variables import *
import dlt
from datetime import datetime, timezone
from pyspark.sql import functions as F
from variables import *

# -----------------------------------------
# 02B SILVER: Materialized SCD2 Business Dimension Tables
# (using create_auto_cdc_flow)
# -----------------------------------------

"""
- **Task 9: Create the `02_silver.silver_customers` Table**

  - Tips

    - _Business logic:_ Track customer attribute changes over time using SCD2, enabling historical analysis and compliance. Only business-relevant columns are tracked for history, and technical columns are excluded. Null updates are ignored to prevent accidental overwrites.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 9".

  - [ ] Use `dlt.create_auto_cdc_flow` with `live.customers_cleaned_stream` as the source.
  - [ ] Set `keys` to `["customer_id"]`.
    - _Business logic:_ Customer ID is the unique identifier for tracking changes.
  - [ ] Set `sequence_by` to `last_update_time`.
    - _Business logic:_ Ensures the most recent changes are applied correctly.
  - [ ] Set `track_history_column_list` to `['name', 'email', 'address', 'phone_number', 'gender']`.
    - _Business logic:_ These columns are important for customer analytics and compliance.
  - [ ] Set `except_column_list` to exclude `["last_update_time", "ingest_timestamp", "source_file_path"]`.
    - _Business logic:_ Exclude technical columns from history tracking.
  - [ ] Ensure `ignore_null_updates` is `True` and `stored_as_scd_type` is `2`.
    - _Business logic:_ SCD2 tracks full history, not just current state.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# dlt.create_streaming_table(SILVER_CUSTOMERS)
# dlt.create_auto_cdc_flow(
#     target=SILVER_CUSTOMERS,
#     source=f"live.{CUSTOMERS_CLEANED_STREAM}",
#     keys=["customer_id"],
#     sequence_by=F.col("last_update_time"),
#     ignore_null_updates=True,
#     track_history_column_list=['name', 'email', 'address', 'phone_number', 'gender'],
#     except_column_list=["last_update_time", "ingest_timestamp", "source_file_path"],
#     stored_as_scd_type=2
# )


##########################################################################################
##########################################################################################
##########################################################################################
"""
- **Task 10: Create the `02_silver.silver_products` Table**

  - Tips

    - _Business logic:_ Track product attribute changes over time using SCD2, enabling analysis of pricing, branding, and inventory trends. Only business-relevant columns are tracked for history, and technical columns are excluded. Null updates are ignored.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 10".

  - [ ] Use `dlt.create_auto_cdc_flow` with `live.products_cleaned_stream` as the source.
  - [ ] Set `keys` to `["product_id"]`.
    - _Business logic:_ Product ID is the unique identifier for tracking changes.
  - [ ] Set `sequence_by` to `last_update_time`.
    - _Business logic:_ Ensures the most recent changes are applied correctly.
  - [ ] Set `track_history_column_list` to `['name', 'category', 'brand', 'price', 'description', 'color', 'size']`.
    - _Business logic:_ These columns are important for product analytics and compliance.
  - [ ] Set `except_column_list` to exclude `["last_update_time", "ingest_timestamp", "source_file_path"]`.
    - _Business logic:_ Exclude technical columns from history tracking.
  - [ ] Ensure `ignore_null_updates` is `True` and `stored_as_scd_type` is `2`.
    - _Business logic:_ SCD2 tracks full history, not just current state.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# dlt.create_streaming_table(SILVER_PRODUCTS)
# dlt.create_auto_cdc_flow(
#     target=SILVER_PRODUCTS,
#     source=f"live.{PRODUCTS_CLEANED_STREAM}",
#     keys=["product_id"],
#     sequence_by=F.col("last_update_time"),
#     ignore_null_updates=True,
#     except_column_list=["last_update_time", "ingest_timestamp", "source_file_path"],
#     track_history_column_list=['name', 'category', 'brand', 'price', 'description', 'color', 'size'],
#     stored_as_scd_type=2
# )

##########################################################################################
##########################################################################################
##########################################################################################

"""
- **Task 11: Create the `02_silver.silver_stores` Table**

  - Tips

    - _Business logic:_ Track store attribute changes over time using SCD2, enabling analysis of operational changes and performance. Only business-relevant columns are tracked for history, and technical columns are excluded. Null updates are ignored.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 11".

  - [ ] Use `dlt.create_auto_cdc_flow` with `live.stores_cleaned_stream` as the source.
  - [ ] Set `keys` to `["store_id"]`.
    - _Business logic:_ Store ID is the unique identifier for tracking changes.
  - [ ] Set `sequence_by` to `last_update_time`.
    - _Business logic:_ Ensures the most recent changes are applied correctly.
  - [ ] Set `track_history_column_list` to `['name', 'address', 'manager', 'status']`.
    - _Business logic:_ These columns are important for store analytics and compliance.
  - [ ] Set `except_column_list` to exclude `["last_update_time", "ingest_timestamp", "source_file_path"]`.
    - _Business logic:_ Exclude technical columns from history tracking.
  - [ ] Ensure `ignore_null_updates` is `True` and `stored_as_scd_type` is `2`.
    - _Business logic:_ SCD2 tracks full history, not just current state.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# dlt.create_streaming_table(SILVER_STORES)
# dlt.create_auto_cdc_flow(
#     target=SILVER_STORES,
#     source=f"live.{STORES_CLEANED_STREAM}",
#     keys=["store_id"],
#     sequence_by=F.col("last_update_time"),
#     ignore_null_updates=True,
#     except_column_list=["last_update_time", "ingest_timestamp", "source_file_path"],
#     track_history_column_list=['name', 'address', 'manager', 'status'],
#     stored_as_scd_type=2
# )