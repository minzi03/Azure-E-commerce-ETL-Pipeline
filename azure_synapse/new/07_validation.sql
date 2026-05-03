/* =========================================================
   07. VALIDATION
   ========================================================= */

-- Sample checks
SELECT TOP 10 * FROM gold.dim_customer;
SELECT TOP 10 * FROM gold.dim_geolocation;
SELECT TOP 10 * FROM gold.dim_orders;
SELECT TOP 10 * FROM gold.dim_product;
SELECT TOP 10 * FROM gold.dim_seller;

SELECT TOP 10 * FROM gold.fact_sales;
SELECT TOP 10 * FROM gold.fact_sales_agg;
SELECT TOP 10 * FROM gold.fact_sales_partitioned;
SELECT TOP 10 * FROM gold.fact_order_payments_partitioned;
SELECT TOP 10 * FROM gold.bridge_order_items;

SELECT TOP 10 * FROM gold.vw_sales_summary;
SELECT TOP 10 * FROM gold.vw_top_product_categories;
SELECT TOP 10 * FROM gold.vw_sales_by_seller;
SELECT TOP 10 * FROM gold.vw_customer_value;
SELECT TOP 10 * FROM gold.vw_delivery_performance;
SELECT TOP 10 * FROM gold.vw_payment_analysis;
GO

-- Row counts
SELECT COUNT(*) AS total_rows FROM gold.dim_customer;
SELECT COUNT(*) AS total_rows FROM gold.dim_product;
SELECT COUNT(*) AS total_rows FROM gold.dim_seller;
SELECT COUNT(*) AS total_rows FROM gold.dim_geolocation;
SELECT COUNT(*) AS total_rows FROM gold.dim_orders;

SELECT COUNT(*) AS total_rows FROM gold.fact_sales;
SELECT COUNT(*) AS total_rows FROM gold.fact_sales_agg;
SELECT COUNT(*) AS total_rows FROM gold.fact_sales_partitioned;
SELECT COUNT(*) AS total_rows FROM gold.fact_order_payments_partitioned;
SELECT COUNT(*) AS total_rows FROM gold.bridge_order_items;
GO

-- Metadata checks
SELECT * FROM sys.schemas WHERE name = 'gold';
SELECT * FROM sys.database_scoped_credentials WHERE name = 'WorkspaceIdentity';
SELECT * FROM sys.external_data_sources WHERE name = 'goldlayer';
SELECT * FROM sys.external_file_formats WHERE name = 'ext_parquet_format';
GO

-- List all gold views
SELECT
    name,
    type_desc
FROM sys.objects
WHERE schema_id = SCHEMA_ID('gold')
  AND type = 'V';
GO

-- List all external tables
SELECT
    name,
    type_desc
FROM sys.external_tables
WHERE schema_id = SCHEMA_ID('gold');
GO