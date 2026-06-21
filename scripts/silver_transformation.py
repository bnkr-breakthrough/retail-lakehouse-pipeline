import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

BASE_PATH = os.getenv("DATA_PATH", "../data")


spark = (
    SparkSession.builder
    .appName("Retail Lakehouse Silver Layer")
    .getOrCreate()
)

bronze_stores = spark.read.parquet(
    f"{BASE_PATH}/bronze/bronze_stores"
)

bronze_features = spark.read.parquet(
    f"{BASE_PATH}/bronze/bronze_features"
)

bronze_sales = spark.read.parquet(
    f"{BASE_PATH}/bronze/bronze_sales"
)

# ==================================
# Silver Stores Transformation
# ==================================

silver_stores = bronze_stores.select(
    col("Store").alias("store"),
    col("Type").alias("type"),
    col("Size").alias("size"),
    col("load_timestamp"),
    col("source_file_name")
)
before_count = silver_stores.count()
if before_count == 0:
    raise Exception(
        "Silver Stores table is empty"
    )

print(f"\nStores Before Deduplication: {before_count}")

silver_stores = silver_stores.dropDuplicates(
    ["store", "type", "size"]
)

after_count = silver_stores.count()

duplicates_removed = before_count - after_count

if duplicates_removed > 100:
    raise Exception(
        f"Duplicate threshold exceeded: {duplicates_removed}"
    )

print(f"Stores After Deduplication: {after_count}")
print(f"Duplicates Removed: {duplicates_removed}")

silver_stores.write.mode("overwrite").parquet(
    f"{BASE_PATH}/silver/silver_stores"
)

print("\nSilver Stores Created Successfully")

silver_stores.printSchema()

silver_stores.show(5, truncate=False)


# ==================================
# Silver Features Transformation
# ==================================


silver_features = bronze_features.select(
    col("Store").alias("store"),
    col("Date").alias("date"),
    col("Temperature").alias("temperature"),
    col("Fuel_Price").alias("fuel_price"),
    col("MarkDown1").alias("markdown1"),
    col("MarkDown2").alias("markdown2"),
    col("MarkDown3").alias("markdown3"),
    col("MarkDown4").alias("markdown4"),
    col("MarkDown5").alias("markdown5"),
    col("CPI").alias("cpi"),
    col("Unemployment").alias("unemployment"),
    col("IsHoliday").alias("is_holiday"),
    col("load_timestamp"),
    col("source_file_name")
)

markdown_columns = [
    "markdown1",
    "markdown2",
    "markdown3",
    "markdown4",
    "markdown5",
    "cpi",
    "unemployment"
]

for column_name in markdown_columns:

    silver_features = silver_features.withColumn(
        column_name,
        when(
            col(column_name) == "NA",
            None
        ).otherwise(
            col(column_name)
        )
    )

silver_features = (
    silver_features
    .withColumn(
        "markdown1",
        col("markdown1").cast("double")
    )
    .withColumn(
        "markdown2",
        col("markdown2").cast("double")
    )
    .withColumn(
        "markdown3",
        col("markdown3").cast("double")
    )
    .withColumn(
        "markdown4",
        col("markdown4").cast("double")
    )
    .withColumn(
        "markdown5",
        col("markdown5").cast("double")
    )
    .withColumn(
        "cpi",
        col("cpi").cast("double")
    )
    .withColumn(
        "unemployment",
        col("unemployment").cast("double")
    )
)
before_count = silver_features.count()
silver_features = silver_features.dropDuplicates(
    ["store", "date"]
)
after_count = silver_features.count()

duplicates_removed = before_count - after_count

if duplicates_removed > 100:
    raise Exception(
        f"Features duplicate threshold exceeded: {duplicates_removed}"
    )
if after_count == 0:
    raise Exception(
        "Silver Features table is empty"
    )
print("\n===== FEATURES DUPLICATE ANALYSIS =====")

print(f"Before Deduplication: {before_count}")
print(f"After Deduplication: {after_count}")
print(f"Duplicates Removed: {duplicates_removed}")

print("\n===== SILVER FEATURES SCHEMA =====")

silver_features.printSchema()

silver_features.show(5, truncate=False)


silver_features.write.mode("overwrite").parquet(
    f"{BASE_PATH}/silver/silver_features"
)

print("\nSilver Features Created Successfully")


# ==================================
# Silver Sales Transformation
# ==================================

silver_sales = bronze_sales.select(
    col("Store").alias("store"),
    col("Dept").alias("dept"),
    col("Date").alias("date"),
    col("Weekly_Sales").alias("weekly_sales"),
    col("IsHoliday").alias("is_holiday"),
    col("load_timestamp"),
    col("source_file_name")
)

before_count = silver_sales.count()
if before_count == 0:
    raise Exception(
        "Silver Sales table is empty"
    )

silver_sales = silver_sales.dropDuplicates(
    ["store", "dept", "date"]
)

after_count = silver_sales.count()

duplicates_removed = before_count - after_count

if duplicates_removed > 100:
    raise Exception(
        f"Sales duplicate threshold exceeded: {duplicates_removed}"
    )

print("\n===== SALES DUPLICATE ANALYSIS =====")

print(f"Before Deduplication: {before_count}")
print(f"After Deduplication: {after_count}")
print(f"Duplicates Removed: {duplicates_removed}")

negative_sales_count = silver_sales.filter(
    col("weekly_sales") < 0
).count()

if negative_sales_count > 5000:
    raise Exception(
        f"Negative sales threshold exceeded: {negative_sales_count}"
    )

print(
    f"Negative Weekly Sales Records: {negative_sales_count}"
)

silver_sales.write.mode("overwrite").parquet(
    f"{BASE_PATH}/silver/silver_sales"
)

print("\nSilver Sales Created Successfully")

print("\n===== SILVER SALES SCHEMA =====")

silver_sales.printSchema()

silver_sales.show(5, truncate=False)
