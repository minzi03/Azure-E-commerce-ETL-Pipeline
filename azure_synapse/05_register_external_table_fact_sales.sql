-- 05. REGISTER EXTERNAL TABLE FOR FACT_SALES

CREATE EXTERNAL TABLE gold.fact_sales_table
(
    fact_sales_sk BIGINT,
    order_id NVARCHAR(100),
    customer_sk NVARCHAR(100),
    product_sk NVARCHAR(100),
    seller_sk NVARCHAR(100),
    order_status NVARCHAR(50),
    purchase_date DATE,
    delivery_date DATE,
    sales_amount FLOAT,
    shipping_cost FLOAT,
    payment_amount FLOAT,
    payment_type NVARCHAR(50),
    review_score INT,
    review_sentiment NVARCHAR(50)
)
WITH (
    LOCATION = 'fact_sales/',
    DATA_SOURCE = goldlayer,
    FILE_FORMAT = extfileformat
);

SELECT COUNT(*) AS total_rows FROM gold.fact_sales_table;

SELECT TOP 10 * FROM gold.fact_sales_table;
