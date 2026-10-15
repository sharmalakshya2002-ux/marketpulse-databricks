# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TABLE = (
    "databricks_cert.marketpulse.silver_stock_prices"
)

silver_df = spark.table(SILVER_TABLE)

print("Silver records:", silver_df.count())

# COMMAND ----------

ticker_window = (
    Window
    .partitionBy("ticker")
    .orderBy("trade_date")
)

last_7_days = ticker_window.rowsBetween(-6, 0)
last_30_days = ticker_window.rowsBetween(-29, 0)

gold_df = (
    silver_df
    .withColumn(
        "previous_close",
        F.lag("close").over(ticker_window)
    )
    .withColumn(
        "daily_return_pct",
        F.when(
            F.col("previous_close") > 0,
            (
                F.col("close") / F.col("previous_close") - 1
            ) * 100
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

display(
    gold_df.select(
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

GOLD_TABLE = (
    "databricks_cert.marketpulse.gold_stock_analytics"
)

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE)
)

print("Gold table created:", GOLD_TABLE)

# COMMAND ----------

gold_saved = spark.table(
    "databricks_cert.marketpulse.gold_stock_analytics"
)

print("Gold records:", gold_saved.count())

display(
    gold_saved
    .groupBy("ticker")
    .agg(
        F.round(
            F.avg("daily_return_pct"), 4
        ).alias("avg_daily_return_pct"),
        F.round(
            F.max("volatility_30d_pct"), 4
        ).alias("max_rolling_volatility_pct"),
        F.max("trade_date").alias("latest_date")
    )
    .orderBy("ticker")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     ROUND(
# MAGIC         (MAX_BY(close, trade_date) /
# MAGIC          MIN_BY(close, trade_date) - 1) * 100,
# MAGIC         2
# MAGIC     ) AS period_return_pct
# MAGIC FROM databricks_cert.marketpulse.gold_stock_analytics
# MAGIC GROUP BY ticker
# MAGIC ORDER BY period_return_pct DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     trade_date,
# MAGIC     close,
# MAGIC     ROUND(daily_return_pct, 2) AS daily_return_pct
# MAGIC FROM databricks_cert.marketpulse.gold_stock_analytics
# MAGIC WHERE daily_return_pct IS NOT NULL
# MAGIC ORDER BY ABS(daily_return_pct) DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ranked AS (
# MAGIC     SELECT
# MAGIC         ticker,
# MAGIC         trade_date,
# MAGIC         close,
# MAGIC         moving_avg_7d,
# MAGIC         moving_avg_30d,
# MAGIC         volatility_30d_pct,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY ticker
# MAGIC             ORDER BY trade_date DESC
# MAGIC         ) AS rn
# MAGIC     FROM databricks_cert.marketpulse.gold_stock_analytics
# MAGIC )
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     trade_date,
# MAGIC     ROUND(close, 2) AS latest_close,
# MAGIC     ROUND(moving_avg_7d, 2) AS moving_avg_7d,
# MAGIC     ROUND(moving_avg_30d, 2) AS moving_avg_30d,
# MAGIC     ROUND(volatility_30d_pct, 4) AS volatility_30d_pct
# MAGIC FROM ranked
# MAGIC WHERE rn = 1
# MAGIC ORDER BY ticker;

# COMMAND ----------

