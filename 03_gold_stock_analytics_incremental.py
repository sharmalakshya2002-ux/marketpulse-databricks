# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

SILVER_TABLE = (
    "databricks_cert.marketpulse.silver_stock_prices"
)

GOLD_TABLE = (
    "databricks_cert.marketpulse.gold_stock_analytics"
)

silver_df = spark.table(SILVER_TABLE)

print("Silver records:", silver_df.count())

if not spark.catalog.tableExists(GOLD_TABLE):
    raise ValueError(
        f"Gold table does not exist: {GOLD_TABLE}. "
        "Run the original Gold notebook first."
    )

print("Existing Gold records:",
      spark.table(GOLD_TABLE).count())

# COMMAND ----------

ticker_window = (
    Window
    .partitionBy("ticker")
    .orderBy("trade_date")
)

last_7_days = ticker_window.rowsBetween(-6, 0)
last_30_days = ticker_window.rowsBetween(-29, 0)

gold_updates = (
    silver_df
    .withColumn(
        "previous_close",
        F.lag("close").over(ticker_window)
    )
    .withColumn(
        "daily_return_pct",
        F.when(
            F.col("previous_close") > 0,
            (F.col("close") / F.col("previous_close") - 1) * 100
        )
    )
    .withColumn(
        "moving_avg_7d",
        F.avg("close").over(last_7_days)
    )
    .withColumn(
        "moving_avg_30d",
        F.avg("close").over(last_30_days)
    )
    .withColumn(
        "avg_volume_30d",
        F.avg("volume").over(last_30_days)
    )
    .withColumn(
        "volatility_30d_pct",
        F.stddev_samp("daily_return_pct").over(last_30_days)
    )
    .withColumn(
        "processed_at",
        F.current_timestamp()
    )
)

print("Gold records calculated:", gold_updates.count())

display(
    gold_updates.select(
        "ticker",
        "trade_date",
        "close",
        "daily_return_pct",
        "moving_avg_7d",
        "moving_avg_30d",
        "volatility_30d_pct"
    ).orderBy("ticker", "trade_date")
)

# COMMAND ----------

gold_delta = DeltaTable.forName(
    spark,
    GOLD_TABLE
)

gold_before = spark.table(GOLD_TABLE).count()

(
    gold_delta.alias("target")
    .merge(
        gold_updates.alias("source"),
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
            "ingestion_timestamp": "source.ingestion_timestamp",
            "previous_close": "source.previous_close",
            "daily_return_pct": "source.daily_return_pct",
            "moving_avg_7d": "source.moving_avg_7d",
            "moving_avg_30d": "source.moving_avg_30d",
            "avg_volume_30d": "source.avg_volume_30d",
            "volatility_30d_pct": "source.volatility_30d_pct",
            "processed_at": "source.processed_at"
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
            "ingestion_timestamp": "source.ingestion_timestamp",
            "previous_close": "source.previous_close",
            "daily_return_pct": "source.daily_return_pct",
            "moving_avg_7d": "source.moving_avg_7d",
            "moving_avg_30d": "source.moving_avg_30d",
            "avg_volume_30d": "source.avg_volume_30d",
            "volatility_30d_pct": "source.volatility_30d_pct",
            "processed_at": "source.processed_at"
        }
    )
    .execute()
)

gold_after = spark.table(GOLD_TABLE).count()

print("Gold MERGE completed successfully.")
print("Rows before:", gold_before)
print("Rows after:", gold_after)
print("Net row change:", gold_after - gold_before)

# COMMAND ----------

gold_saved = spark.table(GOLD_TABLE)

print("Gold record count:", gold_saved.count())

display(
    gold_saved
    .groupBy("ticker")
    .agg(
        F.count("*").alias("record_count"),
        F.max("trade_date").alias("latest_date"),
        F.round(
            F.avg("daily_return_pct"), 4
        ).alias("avg_daily_return_pct"),
        F.round(
            F.max("volatility_30d_pct"), 4
        ).alias("max_rolling_volatility_pct")
    )
    .orderBy("ticker")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ticker, trade_date, COUNT(*) AS record_count
# MAGIC FROM databricks_cert.marketpulse.gold_stock_analytics
# MAGIC GROUP BY ticker, trade_date
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

