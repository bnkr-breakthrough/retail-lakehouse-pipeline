import os
import yaml
import pandas as pd
import snowflake.connector

from snowflake.connector.pandas_tools import write_pandas

DATA_PATH = os.getenv("DATA_PATH", "../data")
CONFIG_PATH = os.getenv("CONFIG_PATH", "../config")


# ==================================
# Load Snowflake Config
# ==================================

with open(f"{CONFIG_PATH}/snowflake_config.yaml", "r") as file:
    config = yaml.safe_load(file)


# ==================================
# Connect to Snowflake
# ==================================

conn = snowflake.connector.connect(
    user=config["user"],
    password=config["password"],
    account=config["account"],
    warehouse=config["warehouse"],
    database=config["database"],
    role=config["role"]
)

print("Connected to Snowflake")


# ==================================
# Reusable Function
# ==================================

def load_parquet_to_snowflake(
    source_path,
    schema_name,
    table_name
):

    print(f"\nLoading {table_name}")

    df = pd.read_parquet(source_path)

    print(f"Row Count: {len(df)}")

    cursor = conn.cursor()

    cursor.execute(
        f"USE SCHEMA {schema_name}"
    )

    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name,
        auto_create_table=True,
        overwrite=True
    )

    print("Load Successful")
    print(f"Rows Loaded: {nrows}")

    cursor.close()


# ==================================
# Load Gold Store Performance
# ==================================

tables_to_load = [

    # Bronze
    (f"{DATA_PATH}/bronze/bronze_stores", "BRONZE", "BRONZE_STORES"),
    (f"{DATA_PATH}/bronze/bronze_features", "BRONZE", "BRONZE_FEATURES"),
    (f"{DATA_PATH}/bronze/bronze_sales", "BRONZE", "BRONZE_SALES"),

    # Silver
    (f"{DATA_PATH}/silver/silver_stores", "SILVER", "SILVER_STORES"),
    (f"{DATA_PATH}/silver/silver_features", "SILVER", "SILVER_FEATURES"),
    (f"{DATA_PATH}/silver/silver_sales", "SILVER", "SILVER_SALES"),

    # Gold
    (f"{DATA_PATH}/gold/gold_store_performance",
     "GOLD", "GOLD_STORE_PERFORMANCE"),
    (f"{DATA_PATH}/gold/gold_department_performance",
     "GOLD", "GOLD_DEPARTMENT_PERFORMANCE"),
    (f"{DATA_PATH}/gold/gold_monthly_sales_trend",
     "GOLD", "GOLD_MONTHLY_SALES_TREND"),
    (f"{DATA_PATH}/gold/gold_holiday_impact_analysis",
     "GOLD", "GOLD_HOLIDAY_IMPACT_ANALYSIS")
]

for source_path, schema_name, table_name in tables_to_load:

    load_parquet_to_snowflake(
        source_path,
        schema_name,
        table_name
    )
conn.close()
