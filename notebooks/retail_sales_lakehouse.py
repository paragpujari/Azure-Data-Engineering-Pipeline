# Databricks notebook source
# Configure these paths for your Azure environment before running.
from pyspark.sql import functions as F
from delta.tables import DeltaTable

dbutils.widgets.text("bronze_path", "abfss://bronze@<storage-account>.dfs.core.windows.net/retail_sales/")
dbutils.widgets.text("silver_path", "abfss://silver@<storage-account>.dfs.core.windows.net/retail_sales/")
dbutils.widgets.text("gold_path", "abfss://gold@<storage-account>.dfs.core.windows.net/retail_sales/")
bronze_path = dbutils.widgets.get("bronze_path")
silver_path = dbutils.widgets.get("silver_path")
gold_path = dbutils.widgets.get("gold_path")

# Bronze: preserve source data and attach ingestion metadata.
raw = (spark.read.option("header", True).csv(bronze_path)
       .withColumn("_ingested_at", F.current_timestamp())
       .withColumn("_source_file", F.input_file_name()))
(raw.write.format("delta").mode("append").save(bronze_path.rstrip("/") + "_delta"))

# Silver: enforce types, standardize text, separate rejected records.
typed = (raw
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("unit_cost", F.col("unit_cost").cast("double"))
    .withColumn("discount_pct", F.col("discount_pct").cast("double"))
    .withColumn("region", F.initcap(F.trim("region")))
    .dropDuplicates(["order_id"]))

valid_condition = (
    F.col("order_id").isNotNull() & F.col("order_date").isNotNull() &
    F.col("product_id").isNotNull() & (F.col("quantity") > 0) &
    (F.col("unit_price") >= 0) & (F.col("unit_cost") >= 0) &
    F.col("discount_pct").between(0, 1)
)
silver = (typed.filter(valid_condition)
    .withColumn("gross_sales", F.col("quantity") * F.col("unit_price"))
    .withColumn("net_sales", F.round(F.col("quantity") * F.col("unit_price") *
                                     (1 - F.col("discount_pct")), 2))
    .withColumn("total_cost", F.round(F.col("quantity") * F.col("unit_cost"), 2))
    .withColumn("profit", F.round(F.col("net_sales") - F.col("total_cost"), 2)))
rejects = typed.filter(~valid_condition)
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)
rejects.write.format("delta").mode("append").save(gold_path.rstrip("/") + "_rejects")

# Gold: KPI-ready tables.
business = silver.filter(F.col("order_status") != "Cancelled")
daily = (business.groupBy("order_date", "region")
    .agg(F.countDistinct("order_id").alias("order_count"),
         F.sum("quantity").alias("units_sold"),
         F.round(F.sum("net_sales"), 2).alias("net_sales"),
         F.round(F.sum("profit"), 2).alias("profit")))
products = (business.groupBy("product_id", "product_name", "category")
    .agg(F.sum("quantity").alias("units_sold"),
         F.round(F.sum("net_sales"), 2).alias("net_sales"),
         F.round(F.sum("profit"), 2).alias("profit")))
regions = (business.groupBy("region")
    .agg(F.countDistinct("order_id").alias("order_count"),
         F.round(F.sum("net_sales"), 2).alias("net_sales"),
         F.round(F.sum("profit"), 2).alias("profit")))

daily.write.format("delta").mode("overwrite").save(gold_path.rstrip("/") + "/daily_sales")
products.write.format("delta").mode("overwrite").save(gold_path.rstrip("/") + "/product_performance")
regions.write.format("delta").mode("overwrite").save(gold_path.rstrip("/") + "/regional_performance")

# Example Delta MERGE pattern for an idempotent silver upsert.
# Replace source_df with the incremental batch and ensure one row per order_id.
# if DeltaTable.isDeltaTable(spark, silver_path):
#     target = DeltaTable.forPath(spark, silver_path)
#     (target.alias("t").merge(source_df.alias("s"), "t.order_id = s.order_id")
#       .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
# else:
#     source_df.write.format("delta").mode("overwrite").save(silver_path)
