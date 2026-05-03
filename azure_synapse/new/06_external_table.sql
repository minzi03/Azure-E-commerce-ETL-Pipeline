/* =========================================================
   06. OPTIONAL PARQUET SNAPSHOT EXTERNAL TABLE
   IMPORTANT:
   - Native external tables are better for Parquet/Csv snapshots
   - Do NOT point external table directly to Delta folders
   - Use this only if you export a dedicated Parquet snapshot
   ========================================================= */

-- Example only:
-- If later you export a pure Parquet snapshot from Databricks to:
-- gold/fact_sales_parquet_snapshot/

IF NOT EXISTS (
    SELECT 1
    FROM sys.external_file_formats
    WHERE name = 'ext_parquet_format'
)
BEGIN
    CREATE EXTERNAL FILE FORMAT ext_parquet_format
    WITH (
        FORMAT_TYPE = PARQUET
    );
END;
GO

-- DROP EXTERNAL TABLE IF EXISTS gold.fact_sales_table;
-- GO

-- CREATE EXTERNAL TABLE gold.fact_sales_table
-- (
--     fact_sales_sk BIGINT,
--     order_sk BIGINT,
--     customer_sk BIGINT,
--     product_sk BIGINT,
--     seller_sk BIGINT,
--     order_id NVARCHAR(100),
--     order_item_id INT,
--     natural_product_id NVARCHAR(100),
--     natural_seller_id NVARCHAR(100),
--     natural_customer_id NVARCHAR(100),
--     order_status NVARCHAR(50),
--     order_status_group NVARCHAR(50),
--     purchase_date DATE,
--     delivery_date DATE,
--     estimated_delivery_date DATE,
--     purchase_year INT,
--     purchase_month INT,
--     purchase_day INT,
--     delivery_days INT,
--     approval_days INT,
--     shipping_days INT,
--     is_delayed BIT,
--     sales_amount FLOAT,
--     shipping_cost FLOAT,
--     gross_item_amount FLOAT,
--     payment_amount FLOAT,
--     payment_type NVARCHAR(50),
--     payment_group NVARCHAR(50),
--     payment_installments INT,
--     payment_transaction_count INT,
--     has_installment_payment BIT,
--     has_high_value_payment BIT,
--     review_score FLOAT,
--     review_sentiment NVARCHAR(50),
--     has_comment BIT,
--     avg_response_time_days FLOAT,
--     review_count INT
-- )
-- WITH (
--     LOCATION = 'fact_sales_parquet_snapshot/',
--     DATA_SOURCE = goldlayer,
--     FILE_FORMAT = ext_parquet_format
-- );
-- GO