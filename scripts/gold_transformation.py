from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum,
    avg,
    count,
    col,
    year,
    month
)

spark = (
    SparkSession.builder
    .appName("Retail Lakehouse Gold Layer")
    .getOrCreate()
)

silver_sales = spark.read.parquet(
    "../data/silver/silver_sales"
)
"""
gold_store_performance = (
    silver_sales
    .groupBy("store")
    .agg(
        sum("weekly_sales").alias("total_sales"),
        avg("weekly_sales").alias("average_sales"),
        count("*").alias("transaction_count")
    )
)
print("\n===== GOLD STORE PERFORMANCE =====")

gold_store_performance.printSchema()

gold_store_performance.show(10, truncate=False)
gold_store_performance.write.mode("overwrite").parquet(
    "../data/gold/gold_store_performance"
)

print("\nGold Store Performance Created Successfully")

# ==================================
# Gold Department Performance
# ==================================

gold_department_performance = (
    silver_sales
    .groupBy("dept")
    .agg(
        sum("weekly_sales").alias("total_sales"),
        avg("weekly_sales").alias("average_sales"),
        count("*").alias("transaction_count")
    )
)
print("\n===== GOLD DEPARTMENT PERFORMANCE =====")

gold_department_performance.printSchema()

gold_department_performance.show(10, truncate=False)
gold_department_performance.write.mode("overwrite").parquet(
    "../data/gold/gold_department_performance"
)

print("\nGold Department Performance Created Successfully")

# ==================================
# Gold Monthly Sales Trend
# ==================================

gold_monthly_sales_trend = (
    silver_sales
    .withColumn(
        "year",
        year(col("date"))
    )
    .withColumn(
        "month",
        month(col("date"))
    )
    .groupBy(
        "year",
        "month"
    )
    .agg(
        sum("weekly_sales").alias("total_sales")
    )
    .orderBy(
        "year",
        "month"
    )
)
print("\n===== GOLD MONTHLY SALES TREND =====")

gold_monthly_sales_trend.printSchema()

gold_monthly_sales_trend.show(20, truncate=False)
gold_monthly_sales_trend.write.mode("overwrite").parquet(
    "../data/gold/gold_monthly_sales_trend"
)

print("\nGold Monthly Sales Trend Created Successfully")
"""

# ==================================
# Gold Holiday Impact Analysis
# ==================================

gold_holiday_impact_analysis = (
    silver_sales
    .groupBy("is_holiday")
    .agg(
        sum("weekly_sales").alias("total_sales"),
        avg("weekly_sales").alias("average_sales"),
        count("*").alias("transaction_count")
    )
)
print("\n===== GOLD HOLIDAY IMPACT ANALYSIS =====")

gold_holiday_impact_analysis.printSchema()

gold_holiday_impact_analysis.show(truncate=False)
gold_holiday_impact_analysis.write.mode("overwrite").parquet(
    "../data/gold/gold_holiday_impact_analysis"
)

print("\nGold Holiday Impact Analysis Created Successfully")
