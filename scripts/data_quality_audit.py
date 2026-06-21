import os
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

BASE_PATH = os.getenv("DATA_PATH", "../data")

spark = (
    SparkSession.builder
    .appName("Retail Lakehouse Audit Layer")
    .getOrCreate()
)

audit_records = []

# ==================================
# Bronze Layer Checks
# ==================================

bronze_tables = [
    ("bronze_stores", "bronze/bronze_stores"),
    ("bronze_features", "bronze/bronze_features"),
    ("bronze_sales", "bronze/bronze_sales")
]

for table_name, path in bronze_tables:

    df = spark.read.parquet(
        f"{BASE_PATH}/{path}"
    )

    row_count = df.count()

    audit_records.append(
        Row(
            pipeline_name=table_name,
            check_name="row_count_check",
            actual_value=str(row_count),
            status="PASS" if row_count > 0 else "FAIL"
        )
    )

# ==================================
# Silver Layer Checks
# ==================================

silver_stores = spark.read.parquet(
    f"{BASE_PATH}/silver/silver_stores"
)

silver_features = spark.read.parquet(
    f"{BASE_PATH}/silver/silver_features"
)

silver_sales = spark.read.parquet(
    f"{BASE_PATH}/silver/silver_sales"
)

audit_records.append(
    Row(
        pipeline_name="silver_stores",
        check_name="row_count_check",
        actual_value=str(silver_stores.count()),
        status="PASS"
    )
)

audit_records.append(
    Row(
        pipeline_name="silver_features",
        check_name="row_count_check",
        actual_value=str(silver_features.count()),
        status="PASS"
    )
)

negative_sales_count = (
    silver_sales
    .filter("weekly_sales < 0")
    .count()
)

audit_records.append(
    Row(
        pipeline_name="silver_sales",
        check_name="negative_sales_check",
        actual_value=str(negative_sales_count),
        status="PASS"
        if negative_sales_count <= 5000
        else "FAIL"
    )
)

# ==================================
# Gold Layer Checks
# ==================================

gold_tables = [
    (
        "gold_store_performance",
        "gold/gold_store_performance"
    ),
    (
        "gold_department_performance",
        "gold/gold_department_performance"
    ),
    (
        "gold_monthly_sales_trend",
        "gold/gold_monthly_sales_trend"
    ),
    (
        "gold_holiday_impact_analysis",
        "gold/gold_holiday_impact_analysis"
    )
]

for table_name, path in gold_tables:

    df = spark.read.parquet(
        f"{BASE_PATH}/{path}"
    )

    row_count = df.count()

    audit_records.append(
        Row(
            pipeline_name=table_name,
            check_name="row_count_check",
            actual_value=str(row_count),
            status="PASS"
            if row_count > 0
            else "FAIL"
        )
    )

# ==================================
# Create Audit DataFrame
# ==================================

audit_df = spark.createDataFrame(
    audit_records
).withColumn(
    "audit_timestamp",
    current_timestamp()
)

print("\n===== DATA QUALITY AUDIT =====")

audit_df.show(
    truncate=False
)

audit_df.write.mode(
    "overwrite"
).parquet(
    f"{BASE_PATH}/audit/data_quality_audit"
)

print(
    "\nAudit Table Created Successfully"
)
