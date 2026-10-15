# Databricks notebook source
print("MarketPulse setup started!")

print("Spark version:", spark.version)

display(spark.range(5))

# COMMAND ----------

import requests

url = "https://query1.finance.yahoo.com/v8/finance/chart/AAPL"

try:
    response = requests.get(
        url,
        params={"range": "5d", "interval": "1d"},
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=20
    )

    print("HTTP status:", response.status_code)
    response.raise_for_status()

    payload = response.json()
    result = payload["chart"]["result"][0]

    print("Ticker:", result["meta"]["symbol"])
    print("Number of records:", len(result["timestamp"]))
    print("Connectivity test passed!")

except Exception as e:
    print("Connectivity test failed:", str(e))

# COMMAND ----------

from datetime import datetime, timezone
import requests
import json

TICKERS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"]

LOOKBACK = "1y"
INTERVAL = "1d"

print("Tickers configured:", TICKERS)

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
    result = response.json()["chart"]["result"][0]

    timestamps = result.get("timestamp", [])
    quotes = result["indicators"]["quote"][0]
    adjclose = result["indicators"].get(
        "adjclose", [{}]
    )[0].get("adjclose", [])

    ingestion_time = datetime.now(timezone.utc)

    for i, timestamp in enumerate(timestamps):
        rows.append({
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
                adjclose[i] if i < len(adjclose) else None
            ),
            "ingestion_timestamp": ingestion_time,
            "raw_json": json.dumps({
                "ticker": ticker,
                "timestamp": timestamp,
                "open": quotes["open"][i],
                "high": quotes["high"][i],
                "low": quotes["low"][i],
                "close": quotes["close"][i],
                "volume": quotes["volume"][i]
            })
        })

    print(f"{ticker}: fetched {len(timestamps)} records")

print("Total records fetched:", len(rows))

# COMMAND ----------

from pyspark.sql import functions as F

if not rows:
    raise ValueError("No records were retrieved.")

bronze_df = (
    spark.createDataFrame(rows)
    .withColumn(
        "trade_date",
        F.to_date("trade_date")
    )
)

display(bronze_df.limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN databricks_cert;

# COMMAND ----------

CATALOG = "databricks_cert"
SCHEMA = "marketpulse"
TABLE = "bronze_stock_prices"

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"`{CATALOG}`.`{SCHEMA}`"
)

table_name = f"`{CATALOG}`.`{SCHEMA}`.`{TABLE}`"

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table_name)
)

print(f"Saved Bronze table: {CATALOG}.{SCHEMA}.{TABLE}")

# COMMAND ----------

saved_df = spark.table(
    "databricks_cert.marketpulse.bronze_stock_prices"
)

print("Total rows:", saved_df.count())

display(
    saved_df.groupBy("ticker")
    .agg(
        F.count("*").alias("record_count"),
        F.min("trade_date").alias("first_date"),
        F.max("trade_date").alias("last_date")
    )
    .orderBy("ticker")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databricks_cert.marketpulse.bronze_stock_prices
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     trade_date,
# MAGIC     COUNT(*) AS record_count
# MAGIC FROM databricks_cert.marketpulse.bronze_stock_prices
# MAGIC GROUP BY ticker, trade_date
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY ticker, trade_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END)
# MAGIC         AS missing_close,
# MAGIC     SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END)
# MAGIC         AS missing_volume
# MAGIC FROM databricks_cert.marketpulse.bronze_stock_prices
# MAGIC GROUP BY ticker
# MAGIC ORDER BY ticker;

# COMMAND ----------

