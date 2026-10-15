
-- MarketPulse: Data Quality Checks

-- 1. Bronze: Check duplicate ticker-date keys
SELECT ticker, trade_date, COUNT(*) AS record_count
FROM databricks_cert.marketpulse.bronze_stock_prices
GROUP BY ticker, trade_date
HAVING COUNT(*) > 1;

-- 2. Silver: Check duplicate ticker-date keys
SELECT ticker, trade_date, COUNT(*) AS record_count
FROM databricks_cert.marketpulse.silver_stock_prices
GROUP BY ticker, trade_date
HAVING COUNT(*) > 1;

-- 3. Gold: Check duplicate ticker-date keys
SELECT ticker, trade_date, COUNT(*) AS record_count
FROM databricks_cert.marketpulse.gold_stock_analytics
GROUP BY ticker, trade_date
HAVING COUNT(*) > 1;

-- 4. Record counts by layer
SELECT 'Bronze' AS layer, COUNT(*) AS total_records
FROM databricks_cert.marketpulse.bronze_stock_prices
UNION ALL
SELECT 'Silver', COUNT(*)
FROM databricks_cert.marketpulse.silver_stock_prices
UNION ALL
SELECT 'Gold', COUNT(*)
FROM databricks_cert.marketpulse.gold_stock_analytics;
