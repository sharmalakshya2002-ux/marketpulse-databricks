# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_TABLE = (
    "databricks_cert.marketpulse.bronze_stock_prices"
)

bronze_df = spark.table(BRONZE_TABLE)

print("Bronze record count:", bronze_df.count())

bronze_df.printSchema()

display(bronze_df.limit(10))

# COMMAND ----------

required_columns = [
    "ticker",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "adj_close"
]

missing_columns = [
    column for column in required_columns
    if column not in bronze_df.columns
]

if missing_columns:
    raise ValueError(
        f"Missing required columns: {missing_columns}"
    )

silver_input = bronze_df.select(
    *required_columns,
    "ingestion_timestamp"
)

print("Required columns are present.")

# COMMAND ----------

typed_df = (
    silver_input
    .withColumn("ticker", F.upper(F.trim("ticker")))
    .withColumn("trade_date", F.to_date("trade_date"))
    .withColumn("open", F.col("open").cast("double"))
    .withColumn("high", F.col("high").cast("double"))
    .withColumn("low", F.col("low").cast("double"))
    .withColumn("close", F.col("close").cast("double"))
    .withColumn("adj_close", F.col("adj_close").cast("double"))
    .withColumn("volume", F.col("volume").cast("long"))
    .withColumn(
        "ingestion_timestamp",
        F.to_timestamp("ingestion_timestamp")
    )
)

valid_condition = (
    F.col("ticker").isNotNull()
    & (F.col("ticker") != "")
    & F.col("trade_date").isNotNull()
    & F.col("open").isNotNull()
    & F.col("high").isNotNull()
    & F.col("low").isNotNull()
    & F.col("close").isNotNull()
    & F.col("volume").isNotNull()
    & (F.col("open") > 0)
    & (F.col("high") > 0)
    & (F.col("low") > 0)
    & (F.col("close") > 0)
    & (F.col("volume") >= 0)
    & (F.col("high") >= F.col("low"))
    & (F.col("high") >= F.col("open"))
    & (F.col("high") >= F.col("close"))
    & (F.col("low") <= F.col("open"))
    & (F.col("low") <= F.col("close"))
)

valid_df = typed_df.filter(valid_condition)
invalid_df = typed_df.filter(~valid_condition)

print("Valid records:", valid_df.count())
print("Invalid records:", invalid_df.count())

# COMMAND ----------

dedup_window = (
    Window
    .partitionBy("ticker", "trade_date")
    .orderBy(F.col("ingestion_timestamp").desc_nulls_last())
)

deduplicated_df = (
    valid_df
    .withColumn(
        "row_num",
        F.row_number().over(dedup_window)
    )
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

print("Records after deduplication:", deduplicated_df.count())

# COMMAND ----------

SILVER_TABLE = (
    "databricks_cert.marketpulse.silver_stock_prices"
)

(
    deduplicated_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print("Silver table saved:", SILVER_TABLE)

# COMMAND ----------

silver_df = spark.table(SILVER_TABLE)

print("Bronze count:", bronze_df.count())
print("Silver count:", silver_df.count())

display(
    silver_df.groupBy("ticker")
    .agg(
        F.count("*").alias("records"),
        F.min("trade_date").alias("first_date"),
        F.max("trade_date").alias("last_date"),
        F.min("close").alias("minimum_close"),
        F.max("close").alias("maximum_close")
    )
    .orderBy("ticker")
)

# COMMAND ----------

