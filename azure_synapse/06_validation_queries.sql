-- 06. VALIDATION & METADATA CHECKS

-- Sample View Queries
SELECT TOP 10 * FROM gold.dim_customer;
SELECT TOP 10 * FROM gold.dim_order_items;
SELECT TOP 10 * FROM gold.dim_order_payments;
SELECT TOP 10 * FROM gold.dim_order_reviews;
SELECT TOP 10 * FROM gold.dim_orders;
SELECT TOP 10 * FROM gold.dim_product;
SELECT TOP 10 * FROM gold.dim_seller;

SELECT TOP 10 * FROM gold.fact_sales;
SELECT TOP 10 * FROM gold.fact_sales_agg;
SELECT TOP 10 * FROM gold.fact_sales_partitioned;
SELECT TOP 10 * FROM gold.fact_order_payments_partitioned;
SELECT TOP 10 * FROM gold.bridge_order_items;

-- External Table Query
SELECT TOP 10 * FROM gold.fact_sales_table;

-- Row Count (snapshot-like table)
SELECT COUNT(*) AS total_rows FROM gold.fact_sales_table;

-- Column Count
SELECT COUNT(*) AS total_columns
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'gold' AND TABLE_NAME = 'fact_sales_table';

-- Metadata Checks
SELECT * FROM sys.schemas WHERE name = 'gold';
SELECT * FROM sys.external_file_formats WHERE name = 'extfileformat';
SELECT * FROM sys.database_scoped_credentials WHERE name = 'WorkspaceIdentity';
SELECT * FROM sys.external_data_sources WHERE name = 'goldlayer';

-- List all gold views
SELECT name, type_desc
FROM sys.objects
WHERE schema_id = SCHEMA_ID('gold') AND type = 'V';

-- List all external tables
SELECT name, type_desc
FROM sys.external_tables
WHERE schema_id = SCHEMA_ID('gold');
