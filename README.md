# MarketPulse — End-to-End Stock Market Data Engineering Pipeline

MarketPulse is an end-to-end data engineering project built using **Databricks Free Edition, PySpark, Delta Lake and SQL**. It ingests historical stock market data, validates and transforms records through a medallion architecture, and produces analytics-ready data for stock performance analysis.

The project demonstrates API ingestion, incremental processing, Delta Lake MERGE operations, data quality validation and multi-task workflow orchestration.

## Project Objectives

- Build a complete stock market data pipeline using Databricks.
- Implement Bronze, Silver and Gold layers.
- Process new and recently updated stock records using Delta MERGE.
- Apply data validation and duplicate detection.
- Calculate daily returns, moving averages and rolling volatility.
- Orchestrate notebook execution using Databricks Workflows.

## Architecture

```text
Yahoo Finance Chart Endpoint
            |
            v
   Bronze — Raw Data
   API ingestion
   Delta MERGE
            |
            v
   Silver — Clean Data
   Type validation
   Price validation
   Deduplication
   Delta MERGE
            |
            v
   Gold — Analytics
   Daily returns
   7-session moving average
   30-session moving average
   Rolling volatility
            |
            v
   SQL Queries and Analytics
```

The pipeline is orchestrated through Databricks Workflows with dependencies between Bronze, Silver and Gold tasks.

## Technology Stack

| Technology | Purpose |
|---|---|
| Databricks Free Edition | Cloud-based data engineering environment |
| Python | API ingestion and pipeline logic |
| PySpark | Distributed data processing and transformations |
| Delta Lake | Reliable tables and MERGE operations |
| SQL | Data validation and analytical queries |
| Databricks Workflows | Task orchestration and dependency management |
| GitHub | Source control and project documentation |

## Data Source

The project uses the Yahoo Finance chart endpoint to retrieve daily historical data for five US-listed companies:

- Apple (AAPL)
- Microsoft (MSFT)
- NVIDIA (NVDA)
- Amazon (AMZN)
- Alphabet (GOOGL)

The initial ingestion covers approximately one year of daily prices. Incremental ingestion re-fetches a recent rolling window to capture new trading dates and potential revisions to recent records.

**Data-source note:** This uses an unofficial Yahoo Finance endpoint for learning and portfolio purposes. Availability, rate limits and permitted usage may change.

## Medallion Architecture

### Bronze — Raw Ingestion

- Retrieves daily stock price data.
- Captures ticker, trading date, OHLC prices, volume and ingestion metadata.
- Stores records in a Delta table.
- Uses Delta MERGE to insert new ticker-date records and update existing records.

### Silver — Data Cleaning and Validation

- Standardises ticker symbols and data types.
- Validates price relationships and non-negative volume.
- Deduplicates records using ticker and trading date.
- Merges cleaned records into the Silver Delta table.
- Preserves historical records outside the processing window.

### Gold — Business Analytics

- Calculates daily percentage returns.
- Computes 7-session and 30-session moving averages.
- Calculates 30-session rolling return volatility.
- Calculates rolling average trading volume.
- Stores analytics in a Gold Delta table.

The current Gold implementation recalculates analytics across the available Silver history before merging the results. This prioritises correctness when historical prices change; selective recalculation of affected dates is a potential future optimisation.

## Incremental Processing

Bronze and Silver use a rolling-window strategy and Delta MERGE operations.

The merge key is:

`(ticker, trade_date)`

Existing records are updated, new records are inserted, and records outside the processing window are preserved.

Data quality checks verify required values, price consistency and uniqueness of ticker-date keys.

## Workflow Orchestration

The incremental workflow contains three dependent notebook tasks:

1. `bronze_incremental`
2. `silver_incremental`
3. `gold_incremental`

The workflow was successfully executed in Databricks Free Edition, with all three tasks reporting success.

## Example Business Questions

The Gold table supports questions such as:

- Which stock had the highest price return over the observed period?
- Which stocks experienced the highest rolling volatility?
- How do closing prices compare with 7-session and 30-session moving averages?
- What were the largest daily price movements?
- How does trading volume vary across companies?

## Repository Structure

```text
marketpulse-databricks/
├── README.md
├── .gitignore
├── notebooks/
│   ├── 01_bronze_stock_ingestion.py
│   ├── 01_bronze_stock_ingestion_incremental.py
│   ├── 02_silver_stock_transformation.py
│   ├── 02_silver_stock_transformation_incremental.py
│   ├── 03_gold_stock_analytics.py
│   └── 03_gold_stock_analytics_incremental.py
├── sql/
│   ├── data_quality_checks.sql
│   └── stock_analytics_queries.sql
└── docs/
    ├── architecture.md
    └── workflow-screenshot.png
```

## Running the Project

1. Create or access a Databricks workspace.
2. Import the notebook source files.
3. Configure the target catalog and schema.
4. Ensure the stock data source is accessible from the compute environment.
5. Run the Bronze ingestion notebook.
6. Run the Silver transformation notebook.
7. Run the Gold analytics notebook.
8. Configure a Databricks Workflow with task dependencies.

The example table names use the `databricks_cert.marketpulse` catalog and schema from the development workspace. Update these names if your environment differs.

## Current Limitations

- The stock data source is an unofficial endpoint and may change.
- Incremental ingestion re-fetches a recent rolling window rather than using a source change-data feed.
- Gold recalculates historical analytics before merging.
- The project currently focuses on daily US stock data and does not include transaction costs, dividends or total-return calculations.
- Dashboard development and production-grade alerting are not included in the current version.

## Future Improvements

- Add automated data quality metrics and quarantine handling.
- Recalculate only the affected Gold records.
- Add pipeline run logging and failure notifications.
- Add automated tests and CI checks.
- Add a Databricks SQL dashboard.
- Introduce parameterised configurations and deployment automation.

## Disclaimer

This project is for educational and portfolio purposes only. It is not investment advice. Stock prices and derived metrics are historical and may not reflect future performance.

## Author

**Lakshya Sharma**

Data Engineering | Data Analytics | PySpark | SQL | Databricks
