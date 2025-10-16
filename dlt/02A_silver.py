from variables import *
import dlt
from datetime import datetime, timezone
from pyspark.sql import functions as F
from variables import *

"""
### ✅ Silver Layer: Cleansed & Conformed Data

- **Goal:** Apply incremental data quality rules, cleanse and conform records, and prepare for historical tracking and analytics. This layer improves data reliability and prepares dimensions and facts for business use.
- **Table Name Prefix:** `02_silver.`
- **Silver Layer Tips:**
  - Apply expectations to catch and handle dirty data. Use views for intermediate cleansing and tables for historical tracking.
  - Think about how CDC (Change Data Capture) and SCD2 (Slowly Changing Dimension Type 2) work in practice.
  - For a more interactive experience, run the `data_generator.py` script while testing your pipeline. This will continuously generate new data, allowing you to observe how each layer processes incoming records in real time. You can stop and restart the generator as needed to see immediate effects in your tables and views.

"""
# -----------------------------------------
# 02A SILVER: Cleaned Streams (Intermediate Views)
# -----------------------------------------


"""
- **Task 5: Create the `customers_cleaned_stream` View**

  - Tips

    - _Business logic:_ Cleanse customer data by enforcing valid IDs, realistic ages, non-negative loyalty points, and valid gender values. Warn on invalid emails and outlier ages, but only drop records with missing IDs. This ensures only usable customer records are tracked, while allowing for business review of questionable but not fatal data issues.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search 'Task 5'.

  - [ ] Read from the `bronze_customers` stream.
  - [ ] Add `comment`: "QC for customers stream".
    - _Business logic:_ Documenting the purpose of this view helps future users understand its role in the pipeline.
  - [ ] **Apply Expectations:**
    - Drop if `customer_id` is `NULL`.
      - _Business logic:_ No customer ID means the record can't be used for analytics or marketing.
    - Warn if `email` is not `NULL` and does not match the pattern `%@%.%`.
      - _Business logic:_ Invalid emails impact communication and customer engagement.
    - Warn if `age` is not between 18 and 100.
      - _Business logic:_ Age outliers may indicate data entry errors or special cases.
    - Warn if `loyalty_points` are less than 0.
      - _Business logic:_ Negative loyalty points are not possible in a real program.
    - Warn if `gender` is not one of ('Male', 'Female', 'Other').
      - _Business logic:_ Ensures inclusivity and data consistency.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dlt.view(name=CUSTOMERS_CLEANED_STREAM, comment="QC for customers stream")
# @dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
# @dlt.expect("valid_email", "email IS NULL OR email LIKE '%@%.%'")
# @dlt.expect("realistic_age", "age >= 18 AND age <= 100")
# @dlt.expect("realistic_loyalty_points", "loyalty_points >= 0")
# @dlt.expect("valid_gender", "gender IN ('Male', 'Female', 'Other')")
# def customers_cleaned_stream():
#     return dlt.read_stream(BRONZE_CUSTOMERS)

##########################################################################################
##########################################################################################
##########################################################################################
"""
- **Task 6: Create the `products_cleaned_stream` View**

  - Tips

    - _Business logic:_ Cleanse product data by enforcing valid IDs and realistic prices, and warn on negative stock, invalid categories, or missing brands. Only drop records with missing IDs or out-of-range prices, ensuring inventory and sales analytics are based on trustworthy product records.
    - _Tip:_ Run the pipeline after this step to see how product data is cleaned and flagged.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 6".

  - [ ] Read from the `bronze_products` stream.
  - [ ] Add `comment`: "QC for products stream".
    - _Business logic:_ Comments clarify the business role of the cleaned product view.
  - [ ] **Apply Expectations:**
    - Drop if `product_id` is `NULL`.
      - _Business logic:_ Product ID is required for all product-level analytics.
    - Drop if `price` is not greater than 0 AND less than 10000.
      - _Business logic:_ Prices outside this range are likely errors or outliers.
    - Warn if `stock_quantity` is less than 0.
      - _Business logic:_ Negative stock is not possible in a real inventory system.
    - Warn if `category` is not one of ('Casual Wear', 'Formal Wear', 'Sportswear', 'Accessories', 'Footwear').
      - _Business logic:_ Ensures products are categorized for reporting and merchandising.
    - Warn if `brand` is `NULL`.
      - _Business logic:_ Brand is important for marketing and product analysis.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dlt.view(name=PRODUCTS_CLEANED_STREAM, comment="QC for products stream")
# @dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
# @dlt.expect_or_drop("valid_product_price", "price > 0 AND price < 10000")
# @dlt.expect("non_negative_stock", "stock_quantity >= 0")
# @dlt.expect("valid_category", "category IN ('Casual Wear', 'Formal Wear', 'Sportswear', 'Accessories', 'Footwear')")
# @dlt.expect("valid_brand", "brand IS NOT NULL")
# def products_cleaned_stream():
#     return dlt.read_stream(BRONZE_PRODUCTS)

##########################################################################################
##########################################################################################
##########################################################################################
"""
- **Task 7: Create the `stores_cleaned_stream` View**

  - Tips

    - _Business logic:_ Cleanse store data by enforcing valid IDs and status, and replace missing manager names with 'Unknown' for reporting consistency. Warn on missing manager names and invalid status, but only drop records with missing IDs. This ensures store-level analytics are reliable and inclusive.
    - _Tip:_ Run the pipeline after this step to validate store data cleaning.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 7".

  - [ ] Read from the `bronze_stores` stream.
  - [ ] Add `comment`: "QC for stores stream".
    - _Business logic:_ Comments clarify the business context of the cleaned store view.
  - [ ] **Apply Transformation:** Replace `NULL` values in the `manager` column with the string 'Unknown'.
    - _Business logic:_ Missing manager names can be handled gracefully for reporting.
  - [ ] **Apply Expectations (post-transformation):**
    - Drop if `store_id` is `NULL`.
      - _Business logic:_ Store ID is required for all store-level analytics.
    - Warn if `manager` is `NULL`.
      - _Business logic:_ Manager assignment is important for accountability.
    - Warn if `status` is not one of ('Open', 'Under Renovation').
      - _Business logic:_ Store status affects operational reporting and analysis.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dlt.view(name=STORES_CLEANED_STREAM, comment="QC for stores stream")
# @dlt.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
# @dlt.expect("valid_manager_name", "manager IS NOT NULL")
# @dlt.expect("valid_status", "status IN ('Open', 'Under Renovation')")
# def stores_cleaned_stream():
#     return dlt.read_stream(BRONZE_STORES).withColumn("manager", F.coalesce(F.col("manager"), F.lit("Unknown")))


##########################################################################################
##########################################################################################
##########################################################################################
"""
- **Task 8: Create the `sales_cleaned_stream` View**

  - Tips

    - _Business logic:_ Cleanse sales data by enforcing valid payment methods and non-negative discounts, and add a derived date column for time-based analytics. Apply watermarking to handle late-arriving data, ensuring streaming analytics are robust and timely.
    - _Tip:_ Run the pipeline after this step to see how sales data is cleaned and flagged.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 8".

  - [ ] Read from the `bronze_sales` stream.
  - [ ] Add `comment`: "QC for sales stream (fact)".
    - _Business logic:_ Comments clarify the business role of the cleaned sales view.
  - [ ] **Apply Transformation:** Add a `date` column by casting `event_time` to a date type.
    - _Business logic:_ Date columns are essential for time-based reporting and analysis.
  - [ ] **Apply Watermarking:** Set a 10-minute watermark on the `event_time` column.
    - _Business logic:_ Watermarking helps handle late-arriving data, which is common in real-world streaming systems.
  - [ ] **Apply Expectations:**
    - Warn if `payment_method` is not one of ('Cash', 'Credit Card', 'Debit Card', 'Mobile Pay', 'Gift Card').
      - _Business logic:_ Invalid payment methods may indicate data entry errors or fraud.
    - Warn if `discount_applied` is less than 0.
      - _Business logic:_ Negative discounts are not possible in real sales.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# @dlt.view(name=SALES_CLEANED_STREAM, comment="QC for sales stream (fact)")
# @dlt.expect("valid_payment_method", "payment_method IN ('Cash', 'Credit Card', 'Debit Card', 'Mobile Pay', 'Gift Card')")
# @dlt.expect("realistic_discount", "discount_applied >= 0")
# def sales_cleaned_stream():
#     return (
#         dlt.read_stream(BRONZE_SALES)
#         .withColumn("date", F.col("event_time").cast("date"))
#         .withWatermark("event_time", "10 minutes")
#         # Data delay is controlled by LATENCY_MAX_S in data generator
#     )