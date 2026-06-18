from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# Create Spark Session

spark = (
    SparkSession.builder
    .appName("Retail Lakehouse Bronze Layer")
    .getOrCreate()
)


def ingest_to_bronze(source_file, target_folder):

    landing_path = f"../data/landing/{source_file}"

    bronze_path = f"../data/bronze/{target_folder}"

    print(f"\nProcessing {source_file}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(landing_path)
    )

    df = (
        df
        .withColumn(
            "load_timestamp",
            current_timestamp()
        )
        .withColumn(
            "source_file_name",
            lit(source_file)
        )
    )

    df.write.mode("overwrite").parquet(bronze_path)

    print(f"Successfully created {target_folder}")

    print(f"Row Count: {df.count()}")


ingest_to_bronze(
    "stores.csv",
    "bronze_stores"
)

ingest_to_bronze(
    "features.csv",
    "bronze_features"
)

ingest_to_bronze(
    "train.csv",
    "bronze_sales"
)
