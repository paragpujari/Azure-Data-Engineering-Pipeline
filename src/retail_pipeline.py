"""Local PySpark batch pipeline for the synthetic retail sales project."""
import argparse
import os
from pyspark.sql import SparkSession, functions as F, types as T

SCHEMA = T.StructType([
    T.StructField("order_id", T.StringType(), True),
    T.StructField("order_date", T.StringType(), True),
    T.StructField("customer_id", T.StringType(), True),
    T.StructField("product_id", T.StringType(), True),
    T.StructField("product_name", T.StringType(), True),
    T.StructField("category", T.StringType(), True),
    T.StructField("region", T.StringType(), True),
    T.StructField("city", T.StringType(), True),
    T.StructField("sales_channel", T.StringType(), True),
    T.StructField("quantity", T.IntegerType(), True),
    T.StructField("unit_price", T.DoubleType(), True),
    T.StructField("unit_cost", T.DoubleType(), True),
    T.StructField("discount_pct", T.DoubleType(), True),
    T.StructField("order_status", T.StringType(), True),
])

def transform(input_path: str, output_dir: str):
    spark = (SparkSession.builder
             .appName("RetailSalesLakehousePortfolio")
             .getOrCreate())
    raw = (spark.read.option("header", True)
           .schema(SCHEMA)
           .csv(input_path)
           .withColumn("_source_file", F.input_file_name())
           .withColumn("_ingested_at", F.current_timestamp()))

    standardized = (raw
        .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
        .withColumn("region", F.initcap(F.trim("region")))
        .withColumn("category", F.initcap(F.trim("category")))
        .withColumn("order_status", F.initcap(F.trim("order_status")))
        .withColumn("discount_pct", F.coalesce(F.col("discount_pct"), F.lit(0.0)))
        .dropDuplicates(["order_id"]))

    valid_condition = (
        F.col("order_id").isNotNull() &
        F.col("order_date").isNotNull() &
        F.col("product_id").isNotNull() &
        F.col("quantity").isNotNull() & (F.col("quantity") > 0) &
        F.col("unit_price").isNotNull() & (F.col("unit_price") >= 0) &
        F.col("unit_cost").isNotNull() & (F.col("unit_cost") >= 0) &
        (F.col("discount_pct") >= 0) & (F.col("discount_pct") <= 1)
    )
    valid = standardized.filter(valid_condition)
    rejected = standardized.filter(~valid_condition)

    silver = (valid
        .withColumn("gross_sales", F.col("quantity") * F.col("unit_price"))
        .withColumn("net_sales", F.round(F.col("quantity") * F.col("unit_price") *
                                         (1 - F.col("discount_pct")), 2))
        .withColumn("total_cost", F.round(F.col("quantity") * F.col("unit_cost"), 2))
        .withColumn("profit", F.round(F.col("net_sales") - F.col("total_cost"), 2))
        .withColumn("profit_margin_pct",
                    F.when(F.col("net_sales") != 0,
                           F.round(F.col("profit") / F.col("net_sales") * 100, 2))))

    kpi_base = silver.filter(F.col("order_status") != "Cancelled")
    daily = (kpi_base.groupBy("order_date", "region")
        .agg(F.countDistinct("order_id").alias("order_count"),
             F.sum("quantity").alias("units_sold"),
             F.round(F.sum("net_sales"), 2).alias("net_sales"),
             F.round(F.sum("total_cost"), 2).alias("total_cost"),
             F.round(F.sum("profit"), 2).alias("profit"))
        .withColumn("profit_margin_pct",
                    F.when(F.col("net_sales") != 0,
                           F.round(F.col("profit") / F.col("net_sales") * 100, 2))))

    product = (kpi_base.groupBy("product_id", "product_name", "category")
        .agg(F.countDistinct("order_id").alias("order_count"),
             F.sum("quantity").alias("units_sold"),
             F.round(F.sum("net_sales"), 2).alias("net_sales"),
             F.round(F.sum("profit"), 2).alias("profit"))
        .withColumn("profit_margin_pct",
                    F.when(F.col("net_sales") != 0,
                           F.round(F.col("profit") / F.col("net_sales") * 100, 2))))

    regional = (kpi_base.groupBy("region")
        .agg(F.countDistinct("order_id").alias("order_count"),
             F.round(F.sum("net_sales"), 2).alias("net_sales"),
             F.round(F.sum("profit"), 2).alias("profit"))
        .withColumn("profit_margin_pct",
                    F.when(F.col("net_sales") != 0,
                           F.round(F.col("profit") / F.col("net_sales") * 100, 2))))

    os.makedirs(output_dir, exist_ok=True)
    (raw.write.mode("overwrite").parquet(os.path.join(output_dir, "bronze")))
    (silver.write.mode("overwrite").parquet(os.path.join(output_dir, "silver")))
    (rejected.write.mode("overwrite").parquet(os.path.join(output_dir, "rejects")))
    (daily.write.mode("overwrite").parquet(os.path.join(output_dir, "gold_daily_sales")))
    (product.write.mode("overwrite").parquet(os.path.join(output_dir, "gold_product_performance")))
    (regional.write.mode("overwrite").parquet(os.path.join(output_dir, "gold_regional_performance")))

    print("=== Pipeline run summary ===")
    print(f"Raw rows: {raw.count()}")
    print(f"Valid rows: {silver.count()}")
    print(f"Rejected rows: {rejected.count()}")
    print(f"Gold daily rows: {daily.count()}")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to retail_sales.csv")
    parser.add_argument("--output", required=True, help="Output directory")
    args = parser.parse_args()
    transform(args.input, args.output)
