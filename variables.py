# --- Unified Path and Table Definitions ---

CATALOG_NAME = "apparel_store"

LANDING_SCHEMA = "00_landing"
BRONZE_SCHEMA = "01_bronze"
SILVER_SCHEMA = "02_silver"
GOLD_SCHEMA = "03_gold"

# Volume base paths
RAW_STREAMING_VOLUME = f"{CATALOG_NAME}.{LANDING_SCHEMA}.streaming"
RAW_STREAMING_PATH = f"/Volumes/{CATALOG_NAME}/{LANDING_SCHEMA}/streaming"

# Raw data directories
RAW_SALES_PATH = f"{RAW_STREAMING_PATH}/sales"
RAW_CUSTOMERS_PATH = f"{RAW_STREAMING_PATH}/customers"
RAW_PRODUCTS_PATH = f"{RAW_STREAMING_PATH}/items"
RAW_STORES_PATH = f"{RAW_STREAMING_PATH}/stores"