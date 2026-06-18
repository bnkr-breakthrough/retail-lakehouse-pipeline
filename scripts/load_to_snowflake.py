import yaml
import pandas as pd
import snowflake.connector

from snowflake.connector.pandas_tools import write_pandas


# ==================================
# Load Snowflake Config
# ==================================

with open("../config/snowflake_config.yaml", "r") as file:
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
    ("../data/bronze/bronze_stores", "BRONZE", "BRONZE_STORES"),
    ("../data/bronze/bronze_features", "BRONZE", "BRONZE_FEATURES"),
    ("../data/bronze/bronze_sales", "BRONZE", "BRONZE_SALES"),

    # Silver
    ("../data/silver/silver_stores", "SILVER", "SILVER_STORES"),
    ("../data/silver/silver_features", "SILVER", "SILVER_FEATURES"),
    ("../data/silver/silver_sales", "SILVER", "SILVER_SALES"),

    # Gold
    ("../data/gold/gold_store_performance", "GOLD", "GOLD_STORE_PERFORMANCE"),
    ("../data/gold/gold_department_performance",
     "GOLD", "GOLD_DEPARTMENT_PERFORMANCE"),
    ("../data/gold/gold_monthly_sales_trend", "GOLD", "GOLD_MONTHLY_SALES_TREND"),
    ("../data/gold/gold_holiday_impact_analysis",
     "GOLD", "GOLD_HOLIDAY_IMPACT_ANALYSIS")
]

for source_path, schema_name, table_name in tables_to_load:

    load_parquet_to_snowflake(
        source_path,
        schema_name,
        table_name
    )
conn.close()
