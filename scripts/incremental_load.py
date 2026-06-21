import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd

BASE_PATH = os.getenv("DATA_PATH", "../data")

spark = (
    SparkSession.builder
    .appName("Retail Incremental Processing")
    .getOrCreate()
)

# ==============================
# Read Watermark
# ==============================

watermark_file = (
    f"{BASE_PATH}/audit/watermark.csv"
)

watermark_df = pd.read_csv(
    watermark_file
)

last_processed_date = (
    watermark_df.loc[
        0,
        "last_processed_date"
    ]
)

print(
    f"\nLast Processed Date: "
    f"{last_processed_date}"
)

# ==============================
# Read Source Data
# ==============================

sales_df = spark.read.csv(
    f"{BASE_PATH}/landing/train.csv",
    header=True,
    inferSchema=True
)

# ==============================
# Filter New Records
# ==============================

incremental_df = sales_df.filter(
    col("Date") > last_processed_date
)

new_record_count = (
    incremental_df.count()
)

print(
    f"New Records Found: "
    f"{new_record_count}"
)

# ==============================
# Update Watermark
# ==============================

if new_record_count > 0:

    max_date = (
        incremental_df
        .agg(
            {"Date": "max"}
        )
        .collect()[0][0]
    )

    pd.DataFrame(
        {
            "last_processed_date": [
                str(max_date)
            ]
        }
    ).to_csv(
        watermark_file,
        index=False
    )

    print(
        f"Watermark Updated To: "
        f"{max_date}"
    )

else:

    print(
        "No New Records Found"
    )
