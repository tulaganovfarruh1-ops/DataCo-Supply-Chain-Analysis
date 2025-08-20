-- =================================================================================================
--
--                                  DataCo Supply Chain - Full Analysis Project
--
-- =================================================================================================
-- Author: Farrux Tulyaganov
-- Date: 2025-08-20
-- Description: This script contains the complete SQL analysis for the DataCo Supply Chain dataset.
--              It is structured into five main blocks, covering everything from sales trend
--              analysis to predictive modeling data preparation.
-- =================================================================================================


-- =================================================================================================
-- Block 1: Sales & Profitability Deep Dive
-- Description: This section covers the complete analysis of sales trends, from initial KPI
--              calculation to the investigation of a critical sales anomaly in late 2017.
-- =================================================================================================

-- Query 1.1: Monthly Key Performance Indicators (KPIs)
-- Objective: Calculate core business metrics for each month to get a high-level overview.
WITH orders_clean_kpi AS (
    SELECT
        SUBSTR(
            "Order Date (DateOrders)",
            INSTR("Order Date (DateOrders)", '/') + INSTR(SUBSTR("Order Date (DateOrders)", INSTR("Order Date (DateOrders)", '/') + 1), '/') + 1, 4
        ) || '-' || printf('%02d', SUBSTR("Order Date (DateOrders)", 1, INSTR("Order Date (DateOrders)", '/') - 1)) AS year_month,
        Sales, "Benefit per order", "Order Id"
    FROM supply_chain
)
SELECT
    year_month,
    SUM(Sales) AS total_sales,
    SUM("Benefit per order") AS total_profit,
    COUNT(DISTINCT "Order Id") AS orders_count,
    SUM("Benefit per order") * 100.0 / SUM(Sales) AS profit_margin_pct,
    SUM(Sales) / COUNT(DISTINCT "Order Id") AS average_order_value_aov
FROM orders_clean_kpi
GROUP BY year_month
ORDER BY year_month;


-- Query 1.2: Time Series Analysis (MoM & YoY Growth)
-- Objective: Analyze short-term and long-term growth trends using window functions.
WITH orders_clean_ts AS (
    SELECT
        SUBSTR(
            "Order Date (DateOrders)",
            INSTR("Order Date (DateOrders)", '/') + INSTR(SUBSTR("Order Date (DateOrders)", INSTR("Order Date (DateOrders)", '/') + 1), '/') + 1, 4
        ) || '-' || printf('%02d', SUBSTR("Order Date (DateOrders)", 1, INSTR("Order Date (DateOrders)", '/') - 1)) AS year_month,
        Sales
    FROM supply_chain
),
monthly_sales AS (
    SELECT year_month, SUM(Sales) AS total_sales
    FROM orders_clean_ts
    GROUP BY year_month
)
SELECT
    year_month,
    total_sales,
    (total_sales - LAG(total_sales, 1) OVER (ORDER BY year_month)) * 100.0 / LAG(total_sales, 1) OVER (ORDER BY year_month) AS mom_growth_pct,
    (total_sales - LAG(total_sales, 12) OVER (ORDER BY year_month)) * 100.0 / LAG(total_sales, 12) OVER (ORDER BY year_month) AS yoy_growth_pct
FROM monthly_sales
ORDER BY year_month;


-- Query 1.3: Anomaly Investigation - Normalized Analysis by Category & Market
-- Objective: Identify the root cause of the sales collapse by comparing the anomaly period
--            to the historical monthly average for each category and market.
WITH orders_clean_anomaly AS (
    SELECT
        SUBSTR(
            "Order Date (DateOrders)",
            INSTR("Order Date (DateOrders)", '/') + INSTR(SUBSTR("Order Date (DateOrders)", INSTR("Order Date (DateOrders)", '/') + 1), '/') + 1, 4
        ) || '-' || printf('%02d', SUBSTR("Order Date (DateOrders)", 1, INSTR("Order Date (DateOrders)", '/') - 1)) AS year_month,
        Sales, "Category Name" AS category_name, Market, "Order Id"
    FROM supply_chain
),
category_normal_metrics AS (
    SELECT
        category_name,
        SUM(Sales) / COUNT(DISTINCT year_month) AS avg_monthly_sales_normal
    FROM orders_clean_anomaly
    WHERE year_month < '2017-11'
    GROUP BY category_name
),
category_anomaly_metrics AS (
    SELECT
        category_name,
        SUM(Sales) / 3.0 AS avg_monthly_sales_anomaly
    FROM orders_clean_anomaly
    WHERE year_month IN ('2017-11', '2017-12', '2018-01')
    GROUP BY category_name
)
SELECT
    norm.category_name,
    norm.avg_monthly_sales_normal,
    COALESCE(anom.avg_monthly_sales_anomaly, 0) AS avg_monthly_sales_anomaly,
    (COALESCE(anom.avg_monthly_sales_anomaly, 0) - norm.avg_monthly_sales_normal) * 100.0 / norm.avg_monthly_sales_normal AS performance_change_pct
FROM category_normal_metrics AS norm
LEFT JOIN category_anomaly_metrics AS anom ON norm.category_name = anom.category_name
ORDER BY performance_change_pct ASC;


-- =================================================================================================
-- Block 2: Delivery Performance & Logistics Deep Dive
-- Description: This section analyzes the efficiency of the delivery process and pinpoints
--              the root causes of late deliveries by market and shipping method.
-- =================================================================================================

-- Query 2.1: Overall Delivery Status Distribution
-- Objective: To get a high-level understanding of delivery performance across the company.
SELECT
    "Delivery Status",
    COUNT(*) AS total_orders,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS percentage_of_total
FROM supply_chain
GROUP BY "Delivery Status"
ORDER BY total_orders DESC;


-- Query 2.2: Detailed Breakdown by Market and Shipping Mode
-- Objective: To identify the precise sources of inefficiency by analyzing performance for each combination.
SELECT
    Market,
    "Shipping Mode",
    SUM(CASE WHEN "Delivery Status" = 'Late delivery' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS late_orders_pct,
    AVG(CASE WHEN "Delivery Status" = 'Late delivery' THEN "Days for shipping (real)" - "Days for shipment (scheduled)" ELSE NULL END) AS avg_delay_days
FROM supply_chain
GROUP BY Market, "Shipping Mode"
ORDER BY late_orders_pct DESC;


-- =================================================================================================
-- Block 3: RFM Customer Segmentation
-- Description: This section performs a complete RFM (Recency, Frequency, Monetary) analysis
--              to segment the customer base and evaluate the financial contribution of each segment.
-- =================================================================================================

-- Query 3.1: RFM Segmentation and Summary
-- Objective: To segment all customers and get a high-level overview of the customer base.
WITH orders_clean_rfm AS (
    SELECT
        "Customer Id" AS customer_id, "Order Id" AS order_id,
        SUBSTR( "Order Date (DateOrders)", INSTR("Order Date (DateOrders)", '/') + INSTR(SUBSTR("Order Date (DateOrders)", INSTR("Order Date (DateOrders)", '/') + 1), '/') + 1, 4) || '-' ||
        printf('%02d', SUBSTR("Order Date (DateOrders)", 1, INSTR("Order Date (DateOrders)", '/') - 1)) || '-' ||
        printf('%02d', SUBSTR(SUBSTR("Order Date (DateOrders)", INSTR("Order Date (DateOrders)", '/') + 1), 1, INSTR(SUBSTR("Order Date (DateOrders)", INSTR("Order Date (DateOrders)", '/') + 1), '/') - 1)) AS order_date,
        Sales
    FROM supply_chain
),
customer_rfm AS (
    SELECT
        customer_id,
        ROUND(julianday((SELECT MAX(order_date) FROM orders_clean_rfm)) - julianday(MAX(order_date))) AS Recency,
        COUNT(DISTINCT order_id) AS Frequency,
        SUM(Sales) AS Monetary
    FROM orders_clean_rfm
    GROUP BY customer_id
),
rfm_scores AS (
    SELECT
        customer_id, Monetary,
        NTILE(4) OVER (ORDER BY Recency DESC) AS R_Score,
        NTILE(4) OVER (ORDER BY Frequency ASC) AS F_Score,
        NTILE(4) OVER (ORDER BY Monetary ASC) AS M_Score
    FROM customer_rfm
),
customer_segments AS (
    SELECT
        customer_id, Monetary,
        CASE
            WHEN R_Score = 4 AND F_Score = 4 AND M_Score = 4 THEN 'Champions'
            WHEN R_Score >= 3 AND F_Score >= 3 THEN 'Loyal Customers'
            WHEN R_Score <= 2 AND F_Score >= 3 AND M_Score >= 3 THEN 'At Risk (High Value)'
            WHEN R_Score <= 2 AND F_Score <= 2 THEN 'Hibernating'
            WHEN R_Score = 4 AND F_Score <= 2 THEN 'New Customers'
            ELSE 'Regular'
        END AS customer_segment
    FROM rfm_scores
)
SELECT
    customer_segment,
    COUNT(customer_id) AS customer_count,
    SUM(Monetary) AS total_monetary,
    SUM(Monetary) * 100.0 / (SELECT SUM(Monetary) FROM customer_segments) AS monetary_percentage
FROM customer_segments
GROUP BY customer_segment
ORDER BY total_monetary DESC;


-- =================================================================================================
-- Block 4: Statistical Hypothesis Testing
-- Description: This script prepares data for a two-sample t-test to statistically prove the
--              relationship between delivery delays and customer value.
-- =================================================================================================

-- Query 4.1: Prepare Data for t-test
-- Objective: To create a table where each customer is assigned to one of two groups for comparison.
SELECT
    customer_id,
    Monetary,
    CASE
        WHEN had_late_delivery_flag = 1 THEN 'Experienced Delays'
        ELSE 'No Delays'
    END AS delivery_experience
FROM (
    SELECT
        "Customer Id" AS customer_id,
        MAX(CASE WHEN "Delivery Status" = 'Late delivery' THEN 1 ELSE 0 END) AS had_late_delivery_flag,
        SUM(Sales) AS Monetary
    FROM supply_chain
    GROUP BY "Customer Id"
);


-- =================================================================================================
-- Block 5: Predictive Modeling - Data Preparation
-- Description: This script prepares a clean, feature-rich dataset for building an advanced
--              regression model to identify the drivers of profitability.
-- =================================================================================================

-- Query 5.1: Prepare Data for Advanced Regression
-- Objective: To create a dataset with engineered features and a random sample for modeling.
SELECT
    "Order Item Profit Ratio" AS profit_ratio,
    LOG("Product Price") AS log_product_price,
    "Order Item Discount" * 1.0 / "Product Price" AS discount_rate,
    "Order Item Quantity" AS quantity,
    CASE WHEN "Category Name" = 'Fishing' THEN 1 ELSE 0 END AS is_fishing,
    CASE WHEN "Category Name" = 'Cleats' THEN 1 ELSE 0 END AS is_cleats,
    CASE WHEN "Category Name" = 'Camping & Hiking' THEN 1 ELSE 0 END AS is_camping,
    CASE WHEN "Category Name" = 'Cardio Equipment' THEN 1 ELSE 0 END AS is_cardio,
    CASE WHEN Market = 'LATAM' THEN 1 ELSE 0 END AS is_latam,
    CASE WHEN Market = 'Europe' THEN 1 ELSE 0 END AS is_europe,
    CASE WHEN Market = 'USCA' THEN 1 ELSE 0 END AS is_usca,
    CASE WHEN Market = 'Africa' THEN 1 ELSE 0 END AS is_africa,
    ("Order Item Discount" * 1.0 / "Product Price") * (CASE WHEN "Category Name" = 'Fishing' THEN 1 ELSE 0 END) AS discount_interaction_fishing
FROM
    supply_chain
WHERE
    "Product Price" > 0 AND "Order Item Discount" < "Product Price"
ORDER BY
    RANDOM()
LIMIT
    20000;
