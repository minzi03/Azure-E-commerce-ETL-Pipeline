/* =========================================================
   05. BUSINESS / BI VIEWS
   ========================================================= */

-- 1. Sales summary by month
CREATE OR ALTER VIEW gold.vw_sales_summary AS
SELECT
    purchase_year,
    purchase_month,
    SUM(sales_amount) AS total_sales,
    SUM(shipping_cost) AS total_shipping_cost,
    SUM(gross_item_amount) AS total_gross_amount,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(*) AS total_order_items
FROM gold.fact_sales
GROUP BY
    purchase_year,
    purchase_month;
GO

-- 2. Top product categories
CREATE OR ALTER VIEW gold.vw_top_product_categories AS
SELECT
    p.category,
    p.category_english,
    SUM(f.sales_amount) AS total_sales,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(*) AS total_order_items
FROM gold.fact_sales f
LEFT JOIN gold.dim_product p
    ON f.product_sk = p.product_sk
GROUP BY
    p.category,
    p.category_english;
GO

-- 3. Sales by seller
CREATE OR ALTER VIEW gold.vw_sales_by_seller AS
SELECT
    s.natural_seller_key,
    s.seller_city,
    s.seller_state,
    SUM(f.sales_amount) AS total_sales,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM gold.fact_sales f
LEFT JOIN gold.dim_seller s
    ON f.seller_sk = s.seller_sk
GROUP BY
    s.natural_seller_key,
    s.seller_city,
    s.seller_state;
GO

-- 4. Customer value
CREATE OR ALTER VIEW gold.vw_customer_value AS
SELECT
    c.natural_customer_key,
    c.customer_city,
    c.customer_state,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.sales_amount) AS total_spent,
    AVG(f.review_score) AS avg_review_score
FROM gold.fact_sales f
LEFT JOIN gold.dim_customer c
    ON f.customer_sk = c.customer_sk
GROUP BY
    c.natural_customer_key,
    c.customer_city,
    c.customer_state;
GO

-- 5. Delivery performance
CREATE OR ALTER VIEW gold.vw_delivery_performance AS
SELECT
    purchase_year,
    purchase_month,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) AS delayed_orders,
    AVG(CAST(delivery_days AS FLOAT)) AS avg_delivery_days,
    AVG(CAST(shipping_days AS FLOAT)) AS avg_shipping_days,
    AVG(CAST(approval_days AS FLOAT)) AS avg_approval_days
FROM gold.fact_sales
GROUP BY
    purchase_year,
    purchase_month;
GO

-- 6. Payment insights
CREATE OR ALTER VIEW gold.vw_payment_analysis AS
SELECT
    payment_type,
    payment_group,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(payment_amount) AS total_payment_amount,
    AVG(CAST(payment_installments AS FLOAT)) AS avg_installments
FROM gold.fact_order_payments_partitioned
GROUP BY
    payment_type,
    payment_group;
GO