# Databricks notebook source
from datetime import datetime, timezone
import requests
import json

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Stock symbols
TICKERS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"]

# Existing Bronze Delta table
BRONZE_TABLE = (
    "databricks_cert.marketpulse.bronze_stock_prices"
)

# Re-fetch recent history to capture new dates and revisions
LOOKBACK = "1mo"
INTERVAL = "1d"

print("MarketPulse incremental ingestion started")
print("Tickers:", TICKERS)
print("Target table:", BRONZE_TABLE)

# COMMAND ----------

rows = []

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0"
})

for ticker in TICKERS:

    url = (
        "https://query1.finance.yahoo.com/"
        f"v8/finance/chart/{ticker}"
    )

    response = session.get(
        url,
        params={
            "range": LOOKBACK,
            "interval": INTERVAL
        },
        timeout=30
    )

    response.raise_for_status()

    payload = response.json()["chart"]

    if payload.get("error"):
        raise RuntimeError(
            f"Yahoo Finance error for {ticker}: "
            f"{payload['error']}"
        )

    result = payload["result"][0]

    timestamps = result.get("timestamp", [])
    quotes = result["indicators"]["quote"][0]

    adjusted = result["indicators"].get(
        "adjclose", [{}]
    )[0].get("adjclose", [])

    ingestion_time = datetime.now(timezone.utc)

    for i, timestamp in enumerate(timestamps):

        record = {
            "ticker": ticker,
            "trade_date": datetime.fromtimestamp(
                timestamp, timezone.utc
            ).date().isoformat(),
            "open": quotes["open"][i],
            "high": quotes["high"][i],
            "low": quotes["low"][i],
            "close": quotes["close"][i],
            "volume": quotes["volume"][i],
            "adj_close": (
                adjusted[i]
                if i < len(adjusted)
                else None
            ),
            "ingestion_timestamp": ingestion_time
        }

        record["raw_json"] = json.dumps({
            key: value
            for key, value in record.items()
            if key != "ingestion_timestamp"
        })

        rows.append(record)

    print(
        f"{ticker}: retrieved {len(timestamps)} records"
    )

if not rows:
    raise ValueError("No stock records were retrieved.")

print("Total incoming records:", len(rows))

# COMMAND ----------

incoming_df = spark.createDataFrame(rows)

incoming_df = (
    incoming_df
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

# Basic data quality validation
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

invalid_count = incoming_df.filter(
    ~valid_condition
).count()

if invalid_count > 0:
    raise ValueError(
        f"Validation failed: {invalid_count} invalid records found."
    )

# Keep the latest incoming record per ticker and date
dedup_window = (
    Window
    .partitionBy("ticker", "trade_date")
    .orderBy(F.col("ingestion_timestamp").desc())
)

incoming_df = (
    incoming_df
    .withColumn(
        "_row_num",
        F.row_number().over(dedup_window)
    )
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

incoming_count = incoming_df.count()

print("Invalid records:", invalid_count)
print("Unique incoming records:", incoming_count)

display(
    incoming_df.orderBy("ticker", "trade_date")
)

# COMMAND ----------

# Ensure the existing target table is available
if not spark.catalog.tableExists(BRONZE_TABLE):
    raise ValueError(
        f"Target Bronze table not found: {BRONZE_TABLE}"
    )

bronze_delta = DeltaTable.forName(
    spark,
    BRONZE_TABLE
)

before_count = spark.table(BRONZE_TABLE).count()

# Insert new dates and update existing dates
(
    bronze_delta.alias("target")
    .merge(
        incoming_df.alias("source"),
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
            "raw_json": "source.raw_json"
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
            "raw_json": "source.raw_json"
        }
    )
    .execute()
)

after_count = spark.table(BRONZE_TABLE).count()

print("Bronze MERGE completed successfully")
print("Rows before:", before_count)
print("Rows after:", after_count)
print("Net row change:", after_count - before_count)

display(
    spark.table(BRONZE_TABLE)
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
# MAGIC SELECT ticker, trade_date, COUNT(*) AS record_count
# MAGIC FROM databricks_cert.marketpulse.bronze_stock_prices
# MAGIC GROUP BY ticker, trade_date
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

