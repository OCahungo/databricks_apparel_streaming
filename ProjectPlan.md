# Project Plan

## Information

- **Configuration:**  
  Many files in this repository rely on `variables.py` to configure your environment and determine where synthetic data is generated. To change the catalog name or other object names, simply update them in `variables.py`.
- **Resource Limits:**  
  If you see a "Resources exhausted" error:
  - Stop the `data_generator.py` script if it's running.
  - Wait a short time before trying again.
  - _Note: This is a limitation of Databricks Free Edition._
- **Help & Inspiration:**  
  If you encounter challenges or uncertainties:
  - A complete reference implementation is available in the "final dlt pipeline" folder.
  - Reviewing the final code can clarify requirements, demonstrate best practices, and guide your pipeline structure.
  - Use this resource to:
    - Compare your approach
    - Troubleshoot issues
    - Deepen your understanding of Delta Live Tables and Databricks workflows

---

## Databricks Configuration Requirements

> **Tip:** If you are new to Databricks, take time to explore the workspace UI and documentation. Understanding catalogs, schemas, and volumes will help you later.

To run this project end-to-end, complete the following setup steps in your Databricks workspace:

1. **Create a Databricks Account**

   - Sign up for a [Databricks Free Edition account](https://www.databricks.com/learn/free-edition) if you don’t already have one.
   - Familiarize yourself with the workspace, clusters, and notebook interface.

2. **Import this repository to Databricks**

   - In Databricks, go to the Workspace sidebar and click the "Repos" section, click "Add Repo".
     - Alternatively, go to your personal folder, click "create" and select "git folder".
   - Paste the GitHub URL for this repository.
   - Authenticate with GitHub if prompted, and select the main branch.
   - The repo will appear as a folder in your workspace, allowing you to edit, run notebooks, and manage files directly from Databricks.
   - For more details, see the official Databricks documentation: [Repos in Databricks](https://docs.databricks.com/repos/index.html).

3. **Run the "utils/Setup Environment" notebook to set up a catalog, schemas and volumes for the synthetic data generator**. It will use the paths defined in the variables file for creating appropriate objects in Unity Catalog.

4. **Create and Configure a DLT Pipeline**

   - In Databricks, create a new Delta Live Tables (DLT) pipeline.
   - Set the pipeline to use your project folder (containing the DLT code) as the source.
   - Set the default catalog to `apparel_store`.
   - Set the target schema to, for example, `02_silver` (or as appropriate for your workflow).
   - Configure cluster and permissions as needed.
   - Set the source folder to this folder, and the source code as the "your_dlt_pipeline" file.
   - **Tip:** Review the DLT pipeline settings and documentation. Understand the difference between streaming and batch tables.

5. **Run the synthetic data generator (`utils/data_generator.py`) to initialize some data.**
   - It will stream infinitely until stopped, so stop it after a few minutes.
   - **Tip:** Check the output location and schema of the generated data. Use the provided markdown for table schemas and data quirks.

---

## DLT Pipeline Reconstruction Checklist

> **How to approach the checklist:**  
> For each task, think about the business logic, data quality, and transformation required. Use the synthetic data generator's quirks (see [SynteticDataGenerator.md](SynteticDataGenerator.md)) to inform your design. Try to reason about why each expectation or transformation is needed.

### ✅ Bronze Layer: Raw Data Ingestion

- **Goal:** Ingest raw source data as streams, add metadata for lineage, and enforce critical schema rules. This layer is the foundation for all downstream processing and should guarantee that only structurally valid records enter the pipeline.
- **Source Path:** `/Volumes/apparel_store/00_landing/streaming/`
- **Table Name Prefix:** `01_bronze.`
- **Bronze Layer Tips:**

  - Focus on schema enforcement and metadata. This is your first line of defense against bad data.
  - Use the variables from `variables.py` for paths and table names to keep your code maintainable.
  - For a more interactive experience, run the `data_generator.py` script while testing your pipeline. This will continuously generate new data, allowing you to observe how each layer processes incoming records in real time. You can stop and restart the generator as needed to see immediate effects in your tables and views.

- **Task 1: Create `01_bronze.bronze_sales` Table**

  - Tips

    - _Business logic:_ Ingest raw sales transactions as a stream, enforce schema by casting columns to correct types, and add metadata (`ingest_timestamp`, `source_file_path`) for auditability. Fail the pipeline if any record is missing a `transaction_id`, ensuring only valid sales events are processed downstream.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 1".

  - [ ] Read as a stream from the `sales` directory - use the `RAW_SALES_PATH` variable.
  - [ ] Add `comment`: "Raw sales data".
    - _Business logic:_ Comments help future maintainers understand the table's purpose. Use clear, business-focused descriptions.
  - [ ] Add `ingest_timestamp` and `source_file_path` columns.
    - _Business logic:_ These columns provide lineage and traceability, which are critical for audits and debugging data issues.
  - [ ] Cast all columns to their specified types (e.g., `transaction_id` to `int`, `event_time` to `timestamp`, `unit_price` to `double`). Use [SynteticDataGenerator.md](SynteticDataGenerator.md) for information about available columns.
    - _Business logic:_ Enforcing types ensures consistency for analytics and prevents silent errors. Think about what each type means for business logic (e.g., `unit_price` as double for currency calculations).
  - [ ] **Set Failure Condition:** The pipeline must fail if `transaction_id` is `NULL`.
    - _Business logic:_ A missing transaction ID means the sale cannot be tracked or joined to other data. This is a critical business rule.

---

- **Task 2: Create `01_bronze.bronze_customers` Table**

  - Tips

    - _Business logic:_ Ingest raw customer records as a stream, enforce schema and add lineage metadata. Fail the pipeline if any record is missing a `customer_id`, ensuring referential integrity for all downstream customer analytics.
    - _Tip:_ Run the pipeline after this step to validate customer data ingestion and catch schema or data quality issues early.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 2".

  - [ ] Read as a stream from the `customers` directory - use the `RAW_CUSTOMERS_PATH` variable.
  - [ ] Add `comment`: "Raw customers data from landing zone".
    - _Business logic:_ Use comments to clarify the source and business role of the table.
  - [ ] Add `ingest_timestamp` and `source_file_path` columns.
    - _Business logic:_ Track when and where customer data was ingested for compliance and troubleshooting.
  - [ ] Cast all columns to their specified types (use [SynteticDataGenerator.md](SynteticDataGenerator.md) for information about available columns).
    - _Business logic:_ Proper types help with downstream joins and analytics. For example, casting `age` to integer allows for age-based segmentation.
  - [ ] **Set Failure Condition:** The pipeline must fail if `customer_id` is `NULL`.
    - _Business logic:_ Customer ID is the primary key for all customer analytics. Missing IDs break referential integrity.

---

- **Task 3: Create `01_bronze.bronze_products` Table**

  - Tips

    - _Business logic:_ Ingest raw product records as a stream, enforce schema and add lineage metadata. Fail the pipeline if any record is missing a `product_id`, ensuring all products are uniquely identifiable for inventory and sales analytics.
    - _Tip:_ Run the pipeline after this step to check for product data issues and validate schema enforcement.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 3".

  - [ ] Read as a stream from the `items` directory - use the `RAW_PRODUCTS_PATH` variable.
  - [ ] Add `comment`: "Raw products data".
    - _Business logic:_ Comments clarify the business role and source of the product data.
  - [ ] Add `ingest_timestamp` and `source_file_path` columns.
    - _Business logic:_ These columns help track product data lineage and support troubleshooting.
  - [ ] Cast all columns to their specified types (use [SynteticDataGenerator.md](SynteticDataGenerator.md) for information about available columns).
    - _Business logic:_ Type enforcement is key for analytics and reporting. For example, casting `price` to double ensures correct calculations.
  - [ ] **Set Failure Condition:** The pipeline must fail if `product_id` is `NULL`.
    - _Business logic:_ Product ID is required for all product-level analytics and joins.

---

- **Task 4: Create `01_bronze.bronze_stores` Table**

  - Tips

    - _Business logic:_ Ingest raw store records as a stream, enforce schema and add lineage metadata. Fail the pipeline if any record is missing a `store_id`, ensuring all stores are uniquely tracked for location-based analytics.
    - _Tip:_ Run the pipeline after this step to confirm store data is ingested correctly and matches business expectations.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 4".

  - [ ] Read as a stream from the `stores` directory - use the `RAW_STORES_PATH` variable.
  - [ ] Add `comment`: "Raw stores data".
    - _Business logic:_ Comments help clarify the business context and source of the store data.
  - [ ] Add `ingest_timestamp` and `source_file_path` columns.
    - _Business logic:_ Track store data lineage for audits and troubleshooting.
  - [ ] Cast all columns to their specified types (use [SynteticDataGenerator.md](SynteticDataGenerator.md) for information about available columns).
    - _Business logic:_ Type enforcement ensures store data can be joined and analyzed reliably.
  - [ ] **Set Failure Condition:** The pipeline must fail if `store_id` is `NULL`.
    - _Business logic:_ Store ID is the key for all store-level analytics and reporting.

---

### ✅ Silver Layer: Cleansed & Conformed Data

- **Goal:** Apply incremental data quality rules, cleanse and conform records, and prepare for historical tracking and analytics. This layer improves data reliability and prepares dimensions and facts for business use.
- **Table Name Prefix:** `02_silver.`
- **Silver Layer Tips:**
  - Apply expectations to catch and handle dirty data. Use views for intermediate cleansing and tables for historical tracking.
  - Think about how CDC (Change Data Capture) and SCD2 (Slowly Changing Dimension Type 2) work in practice.
  - For a more interactive experience, run the `data_generator.py` script while testing your pipeline. This will continuously generate new data, allowing you to observe how each layer processes incoming records in real time. You can stop and restart the generator as needed to see immediate effects in your tables and views.

#### Part A: Create Intermediate Cleaned Views

- **Task 5: Create the `customers_cleaned_stream` View**

  - Tips

    - _Business logic:_ Cleanse customer data by enforcing valid IDs, realistic ages, non-negative loyalty points, and valid gender values. Warn on invalid emails and outlier ages, but only drop records with missing IDs. This ensures only usable customer records are tracked, while allowing for business review of questionable but not fatal data issues.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 5".

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

---

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

---

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

---

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

---

#### Part B: Create Materialized Dimension Tables with History (SCD Type 2)

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

---

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

---

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

---

#### Part C: Create Materialized Fact Tables

- **Task 12: Create the `02_silver.silver_sales_transactions` Table**

  - Tips

    - _Business logic:_ Filter sales records to include only positive quantities (actual sales), and drop records with invalid discounts or missing foreign keys. This ensures only valid sales transactions are available for analytics.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 12".

  - [ ] Read from the `live.sales_cleaned_stream`.
  - [ ] Filter to include only records where `quantity > 0`.
    - _Business logic:_ Negative or zero quantities are not valid sales.
  - [ ] **Apply Expectations:**
    - Drop if `discount_applied` is less than 0.
      - _Business logic:_ Negative discounts are not possible in real sales.
    - Drop if `store_id` is `NULL`.
      - _Business logic:_ Store ID is required for all store-level analytics.
    - Drop if `customer_id` is `NULL`.
      - _Business logic:_ Customer ID is required for all customer-level analytics.
    - Drop if `product_id` is `NULL`.
      - _Business logic:_ Product ID is required for all product-level analytics.

---

- **Task 13: Create the `02_silver.silver_returns_transactions` Table**

  - Tips

    - _Business logic:_ Filter sales records to include only negative quantities (returns), transform to absolute values for reporting, and drop records with invalid discounts or missing foreign keys. This ensures returns are tracked separately and accurately.
    - _Tip:_ Run the pipeline after this step to validate returns processing and expectations.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 13".

  - [ ] Read from the `live.sales_cleaned_stream`.
  - [ ] Filter to include only records where `quantity < 0`.
    - _Business logic:_ Only negative quantities should be considered returns.
  - [ ] **Apply Transformations:**
    - Create `returned_quantity` column using the absolute value of `quantity`.
      - _Business logic:_ Absolute value makes reporting and analysis easier.
    - Create `returned_amount` column using the absolute value of `total_amount`.
      - _Business logic:_ Absolute value makes reporting and analysis easier.
    - Drop the original `quantity` and `total_amount` columns.
      - _Business logic:_ Removes ambiguity in reporting.
  - [ ] Apply the same four "expect or drop" quality rules as the sales transactions table.
    - _Business logic:_ Ensures returns data is as clean as sales data.

---

#### Part D: Create "Current" Dimension Views

- **Task 14: Create `silver_customers_current` View**

  - Tips

    - _Business logic:_ Filter SCD2 customer table for current records (no end date), providing the latest snapshot for analytics and lookups.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 14".

  - [ ] Read from `02_silver.silver_customers`.
  - [ ] Filter for records where the `__END_AT` column is `NULL`.
    - _Business logic:_ Only records without an end date are considered current.

---

- **Task 15: Create `silver_products_current` View**

  - Tips

    - _Business logic:_ Filter SCD2 product table for current records, ensuring analytics use the latest product attributes.
    - _Tip:_ Run the pipeline after this step to validate current product view logic.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 15".

  - [ ] Read from `02_silver.silver_products`.
  - [ ] Filter for records where the `__END_AT` column is `NULL`.
    - _Business logic:_ Only records without an end date are considered current.

---

- **Task 16: Create `silver_stores_current` View**

  - Tips

    - _Business logic:_ Filter SCD2 store table for current records, ensuring analytics use the latest store attributes.
    - _Tip:_ Run the pipeline after this step to validate current store view logic.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 16".

  - [ ] Read from `02_silver.silver_stores`.
  - [ ] Filter for records where the `__END_AT` column is `NULL`.
    - _Business logic:_ Only records without an end date are considered current.

---

### **✅ Gold Layer: Business-Ready Analytics**

- **Goal:** Create denormalized and aggregated tables for direct use by analysts and BI tools. This layer delivers business value by joining, enriching, and summarizing data for reporting and decision-making.
- **Table Name Prefix:** `03_gold.`
- **Gold Layer Tips:**

  - Focus on joins and aggregations. This is where you create business value from your data.
  - Think about how analysts will use these tables for reporting and BI.
  - For a more interactive experience, run the `data_generator.py` script while testing your pipeline. This will continuously generate new data, allowing you to observe how each layer processes incoming records in real time. You can stop and restart the generator as needed to see immediate effects in your tables and views.

- **Task 17: Create `03_gold.denormalized_sales_facts` Streaming Table**

  - Tips

    - _Business logic:_ Join sales transactions with current dimension tables to enrich each sale with customer, product, and store attributes. Use left joins to ensure all sales are included, even if some dimension data is missing. Alias columns for clarity and usability in BI tools.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 17".

  - [ ] Read as a stream from `LIVE.02_silver.silver_sales_transactions`.
  - [ ] Read `LIVE.silver_customers_current`, `LIVE.silver_products_current`, and `LIVE.silver_stores_current` as static/lookup tables.
    - _Business logic:_ Lookup tables provide the latest dimension attributes for each sale.
  - [ ] **Join Logic:** `left` join the sales stream with stores, then customers, then products on their respective IDs.
    - _Business logic:_ Left joins ensure all sales are included, even if some dimension data is missing.
  - [ ] **Select & Alias Columns:** Construct the final schema with aliased columns like `customer_name`, `product_name`, and `store_name`.
    - _Business logic:_ Aliased columns make reporting and analysis easier for business users.

---

- **Task 18: Create `03_gold.gold_daily_sales_by_store` Aggregate Table**

  - Tips

    - _Business logic:_ Aggregate denormalized sales facts by store and day, calculating total revenue, transaction count, items sold, and unique customers. This enables daily performance tracking for each store.
    - _Tip:_ Run the pipeline after this step to validate aggregations and reporting logic.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 18".

  - [ ] Read from `LIVE.03_gold.denormalized_sales_facts`.
  - [ ] **Group by:**
    - A `1 day` window on `event_time`.
      - _Business logic:_ Daily windows align with business reporting cycles.
    - `store_id`
      - _Business logic:_ Store-level grouping enables location-based analysis.
    - `store_name`
      - _Business logic:_ Store names make reports more readable.
  - [ ] **Calculate Aggregations:**
    - `total_revenue`: `round(sum(total_amount), 2)`
      - _Business logic:_ Total revenue is a primary business KPI.
    - `total_transactions`: `count(transaction_id)`
      - _Business logic:_ Transaction count helps track store activity.
    - `total_items_sold`: `sum(quantity)`
      - _Business logic:_ Items sold is important for inventory and sales analysis.
    - `unique_customers`: `countDistinct(customer_id)`
      - _Business logic:_ Unique customers help measure store reach and loyalty.
  - [ ] **Final Select:** Choose the grouping columns and aggregated metrics, casting the window start time to a `sale_date`.
    - _Business logic:_ Sale date is essential for time-based reporting.

---

- **Task 19: Create `03_gold.gold_product_performance` Aggregate Table**

  - Tips

    - _Business logic:_ Product performance metrics drive merchandising, inventory, and marketing decisions.
    - _Tip:_ Run the pipeline after this step to validate product performance metrics and aggregations.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 19".

  - [ ] Read from `LIVE.03_gold.denormalized_sales_facts`.
  - [ ] **Group by:** `product_id`, `product_name`, `product_category`.
    - _Business logic:_ Grouping by product attributes enables detailed analysis.
  - [ ] **Calculate Aggregations:**
    - `total_revenue`: `round(sum(total_amount), 2)`
      - _Business logic:_ Revenue by product is key for profitability analysis.
    - `total_quantity_sold`: `round(sum(quantity), 2)`
      - _Business logic:_ Quantity sold helps with inventory planning.
    - `total_orders`: `count(transaction_id)`
      - _Business logic:_ Order count is useful for demand forecasting.

---

- **Task 20: Create `03_gold.gold_customer_lifetime_value` Aggregate Table**

  - Tips

    - _Business logic:_ Customer lifetime value is a key metric for marketing and retention strategies.
    - _Tip:_ Run the pipeline after this task to validate CLV calculations and aggregations.
    - _Tip (code):_ If stuck, open `final_dlt.py` (under the `final dlt pipeline` folder) and search "Task 20".

  - [ ] Read from `LIVE.03_gold.denormalized_sales_facts`.
  - [ ] **Group by:** `customer_id`, `customer_name`.
    - _Business logic:_ Grouping by customer enables personalized analytics.
  - [ ] **Calculate Aggregations:**
    - `total_spend`: `round(sum(total_amount), 2)`
      - _Business logic:_ Total spend is the foundation of CLV.
    - `total_orders`: `countDistinct(transaction_id)`
      - _Business logic:_ Order count shows engagement.
    - `first_purchase_date`: `min(event_time)` cast to date.
      - _Business logic:_ First purchase date helps track customer lifecycle.
    - `last_purchase_date`: `max(event_time)` cast to date.
      - _Business logic:_ Last purchase date helps track retention.
    - `avg_order_value`: `round(avg(total_amount), 2)`
      - _Business logic:_ Average order value is a key marketing metric.
      - _Business logic:_ Last purchase date helps track retention.
    - `avg_order_value`: `round(avg(total_amount), 2)`
      - _Business logic:_ Average order value is a key marketing metric.
      - _Business logic:_ Store names make reports more readable.
  - [ ] **Calculate Aggregations:**
    - `total_revenue`: `round(sum(total_amount), 2)`
      - _Business logic:_ Total revenue is a primary business KPI.
    - `total_transactions`: `count(transaction_id)`
      - _Business logic:_ Transaction count helps track store activity.
    - `total_items_sold`: `sum(quantity)`
      - _Business logic:_ Items sold is important for inventory and sales analysis.
    - `unique_customers`: `countDistinct(customer_id)`
      - _Business logic:_ Unique customers help measure store reach and loyalty.
  - [ ] **Final Select:** Choose the grouping columns and aggregated metrics, casting the window start time to a `sale_date`.
    - _Business logic:_ Sale date is essential for time-based reporting.
