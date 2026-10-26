-- Run against the Silver/Gold tables after registering them in Databricks SQL.
-- Adapt catalog.schema.table names to your workspace.

-- 1. Monthly sales and profitability trend
SELECT date_trunc('MONTH', order_date) AS sales_month,
       SUM(net_sales) AS net_sales,
       SUM(profit) AS profit,
       ROUND(100.0 * SUM(profit) / NULLIF(SUM(net_sales), 0), 2) AS profit_margin_pct
FROM main.retail.silver_retail_sales
WHERE order_status <> 'Cancelled'
GROUP BY date_trunc('MONTH', order_date)
ORDER BY sales_month;

-- 2. Product/category performance
SELECT category, product_name,
       SUM(quantity) AS units_sold,
       SUM(net_sales) AS net_sales,
       SUM(profit) AS profit,
       ROUND(100.0 * SUM(profit) / NULLIF(SUM(net_sales), 0), 2) AS profit_margin_pct
FROM main.retail.silver_retail_sales
WHERE order_status <> 'Cancelled'
GROUP BY category, product_name
ORDER BY profit DESC;

-- 3. Regional performance
SELECT region, COUNT(DISTINCT order_id) AS order_count,
       SUM(net_sales) AS net_sales, SUM(profit) AS profit
FROM main.retail.silver_retail_sales
WHERE order_status <> 'Cancelled'
GROUP BY region
ORDER BY net_sales DESC;

-- 4. Data-quality checks: duplicate order IDs
SELECT order_id, COUNT(*) AS row_count
FROM main.retail.silver_retail_sales
GROUP BY order_id
HAVING COUNT(*) > 1;

-- 5. Data-quality checks: invalid discounts
SELECT COUNT(*) AS invalid_discount_rows
FROM main.retail.silver_retail_sales
WHERE discount_pct < 0 OR discount_pct > 1;
