# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

BRONZE_TABLE = (
    "databricks_cert.marketpulse.bronze_stock_prices"
)

SILVER_TABLE = (
    "databricks_cert.marketpulse.silver_stock_prices"
)

bronze_df = spark.table(BRONZE_TABLE)

print("Bronze records:", bronze_df.count())

if not spark.catalog.tableExists(SILVER_TABLE):
    raise ValueError(
        f"Silver table does not exist: {SILVER_TABLE}. "
        "Run the original Silver notebook first."
    )

print("Existing Silver records:",
      spark.table(SILVER_TABLE).count())

# COMMAND ----------

max_date = bronze_df.agg(
    F.max("trade_date").alias("max_date")
).first()["max_date"]

if max_date is None:
    raise ValueError("Bronze table has no trading dates.")

recent_df = bronze_df.filter(
    F.col("trade_date") >= F.date_sub(
        F.lit(max_date), 35
    )
)

required_columns = [
    "ticker",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "adj_close",
    "ingestion_timestamp"
]

missing_columns = [
    c for c in required_columns
    if c not in recent_df.columns
]

if missing_columns:
    raise ValueError(
        f"Missing required columns: {missing_columns}"
    )

silver_input = recent_df.select(*required_columns)

silver_input = (
    silver_input
    .withColumn("ticker", F.upper(F.trim("ticker")))
    .withColumn("trade_date", F.to_date("trade_date"))
    .withColumn("open", F.col("open").cast("double"))
    .withColumn("high", F.col("high").cast("double"))
    .withColumn("low", F.col("low").cast("double"))
    .withColumn("close", F.col("close").cast("double"))
    .withColumn("volume", F.col("volume").cast("long"))
    .withColumn("adj_close", F.col("adj_close").cast("double"))
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
    & (F.col("high") >= F.col("low"))
    & (F.col("high") >= F.col("open"))
    & (F.col("high") >= F.col("close"))
    & (F.col("low") <= F.col("open"))
    & (F.col("low") <= F.col("close"))
    & (F.col("volume") >= 0)
)

invalid_count = silver_input.filter(
    ~valid_condition
).count()

if invalid_count > 0:
    raise ValueError(
        f"Found {invalid_count} invalid records. "
        "Fix or quarantine them before merging."
    )

valid_df = silver_input.filter(valid_condition)

print("Latest Bronze date:", max_date)
print("Recent source records:", recent_df.count())
print("Invalid records:", invalid_count)

# COMMAND ----------

dedup_window = (
    Window
    .partitionBy("ticker", "trade_date")
    .orderBy(F.col("ingestion_timestamp").desc_nulls_last())
)

silver_updates = (
    valid_df
    .withColumn(
        "_row_num",
        F.row_number().over(dedup_window)
    )
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

incoming_count = silver_updates.count()

print("Unique records to merge:", incoming_count)

# Capture the current Silver count before the merge
silver_before = spark.table(SILVER_TABLE).count()

silver_delta = DeltaTable.forName(
    spark,
    SILVER_TABLE
)

(
    silver_delta.alias("target")
    .merge(
        silver_updates.alias("source"),
        """
        target.ticker = source.ticker
        AND target.trade_date = source.trade_date
        """
    )
    .whenMatchedUpdate(
        set={
            "open": "source.open",
            "high": "source.high",
            "low": "source.low",
            "close": "source.close",
            "volume": "source.volume",
            "adj_close": "source.adj_close",
            "ingestion_timestamp": "source.ingestion_timestamp"
        }
    )
    .whenNotMatchedInsert(
        values={
            "ticker": "source.ticker",
            "trade_date": "source.trade_date",
            "open": "source.open",
            "high": "source.high",
            "low": "source.low",
            "close": "source.close",
            "volume": "source.volume",
            "adj_close": "source.adj_close",
            "ingestion_timestamp": "source.ingestion_timestamp"
        }
    )
    .execute()
)

print("Silver MERGE completed successfully.")
print("Rows before:", silver_before)

# COMMAND ----------

silver_after = spark.table(SILVER_TABLE)

after_count = silver_after.count()

print("Silver MERGE completed.")
print("Rows before:", silver_before)
print("Rows after:", after_count)
print("Net row change:", after_count - silver_before)

display(
    silver_after
    .groupBy("ticker")
    .agg(
        F.count("*").alias("record_count"),
        F.min("trade_date").alias("first_date"),
        F.max("trade_date").alias("last_date")
    )
    .orderBy("ticker")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     trade_date,
# MAGIC     COUNT(*) AS record_count
# MAGIC FROM databricks_cert.marketpulse.silver_stock_prices
# MAGIC GROUP BY ticker, trade_date
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

