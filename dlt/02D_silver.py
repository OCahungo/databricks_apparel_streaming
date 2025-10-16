from variables import *
import dlt
from datetime import datetime, timezone
from pyspark.sql import functions as F
from variables import *

# -----------------------------------------
# 02D SILVER: Create "Current" Dimension Views
# -----------------------------------------

"""
- **Task 14: Create `silver_customers_current` View**

  - Tips

    - _Business logic:_ Filter SCD2 customer table for current records (no end date), providing the latest snapshot for analytics and lookups.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 14".

  - [ ] Read from `02_silver.silver_customers`.
  - [ ] Filter for records where the `__END_AT` column is `NULL`.
    - _Business logic:_ Only records without an end date are considered current.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dlt.view(
#     name=SILVER_CUSTOMERS_CURRENT,
#     comment="Current customers (SCD2)"
# )
# def silver_customers_current():
#     return dlt.read(SILVER_CUSTOMERS).filter(F.col("__END_AT").isNull())


##########################################################################################
##########################################################################################
##########################################################################################
"""
- **Task 15: Create `silver_products_current` View**

  - Tips

    - _Business logic:_ Filter SCD2 product table for current records, ensuring analytics use the latest product attributes.
    - _Tip:_ Run the pipeline after this step to validate current product view logic.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 15".

  - [ ] Read from `02_silver.silver_products`.
  - [ ] Filter for records where the `__END_AT` column is `NULL`.
    - _Business logic:_ Only records without an end date are considered current.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dlt.view(
#     name=SILVER_PRODUCTS_CURRENT,
#     comment="Current products (SCD2)"
# )
# def silver_products_current():
#     return dlt.read(SILVER_PRODUCTS).filter(F.col("__END_AT").isNull())


##########################################################################################
##########################################################################################
##########################################################################################
"""
- **Task 16: Create `silver_stores_current` View**

  - Tips

    - _Business logic:_ Filter SCD2 store table for current records, ensuring analytics use the latest store attributes.
    - _Tip:_ Run the pipeline after this step to validate current store view logic.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 16".

  - [ ] Read from `02_silver.silver_stores`.
  - [ ] Filter for records where the `__END_AT` column is `NULL`.
    - _Business logic:_ Only records without an end date are considered current.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dlt.view(
#     name=SILVER_STORES_CURRENT,
#     comment="Current stores (SCD2)"
# )
# def silver_stores_current():
#     return dlt.read(SILVER_STORES).filter(F.col("__END_AT").isNull())
