-- ========================================
-- SETUP: SCHEMA, CREDENTIAL, FILE FORMAT
-- ========================================
-- 1. Create 'gold' schema if not exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END;

-- 2. Create external file format (Parquet + Snappy)
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'extfileformat')
CREATE EXTERNAL FILE FORMAT extfileformat
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

-- 3. Create master key (if not exists)
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Duy@123';

-- 4. Create database scoped credential (Managed Identity)
IF NOT EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'WorkspaceIdentity')
CREATE DATABASE SCOPED CREDENTIAL WorkspaceIdentity
WITH IDENTITY = 'Managed Identity';

-- 5. Create external data source
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'goldlayer')
CREATE EXTERNAL DATA SOURCE goldlayer
WITH (
    LOCATION = 'https://olistetlstga.dfs.core.windows.net/olistdata/gold/',
    CREDENTIAL = WorkspaceIdentity
);

-- ===============================
-- DIMENSION TABLES (as VIEWS)
-- ===============================

CREATE OR ALTER VIEW gold.dim_customer AS
SELECT * FROM OPENROWSET(BULK 'dim_customer/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET') AS rows;

CREATE OR ALTER VIEW gold.dim_geolocation AS
SELECT * FROM OPENROWSET(BULK 'dim_geolocation/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET') AS rows;

CREATE OR ALTER VIEW gold.dim_order_items AS
SELECT * FROM OPENROWSET(BULK 'dim_order_items/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET') AS rows;

CREATE OR ALTER VIEW gold.dim_order_payments AS
SELECT * FROM OPENROWSET(BULK 'dim_order_payments/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET') AS rows;

CREATE OR ALTER VIEW gold.dim_order_reviews AS
SELECT * FROM OPENROWSET(BULK 'dim_order_reviews/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET') AS rows;

CREATE OR ALTER VIEW gold.dim_orders AS
SELECT * FROM OPENROWSET(BULK 'dim_orders/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET') AS rows;

CREATE OR ALTER VIEW gold.dim_product AS
SELECT * FROM OPENROWSET(BULK 'dim_product/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET') AS rows;

CREATE OR ALTER VIEW gold.dim_seller AS
SELECT * FROM OPENROWSET(BULK 'dim_seller/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET') AS rows;

-- ===============================
-- FACT & BRIDGE TABLES (as VIEWS)
-- ===============================

CREATE OR ALTER VIEW gold.fact_sales AS
SELECT * FROM OPENROWSET(BULK 'fact_sales/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET') AS rows;

CREATE OR ALTER VIEW gold.fact_sales_agg AS
SELECT * FROM OPENROWSET(BULK 'fact_sales_agg/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET') AS rows;

CREATE OR ALTER VIEW gold.fact_sales_partitioned AS
SELECT * FROM OPENROWSET(BULK 'fact_sales_partitioned/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET') AS rows;

CREATE OR ALTER VIEW gold.fact_order_payments_partitioned AS
SELECT * FROM OPENROWSET(BULK 'fact_order_payments_partitioned/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET') AS rows;

CREATE OR ALTER VIEW gold.bridge_order_items AS
SELECT * FROM OPENROWSET(BULK 'bridge_order_items/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET') AS rows;

-- ===============================
-- SNAPSHOT TABLE (External Table)
-- ===============================

-- Optional: Create snapshot table for fact_sales
CREATE EXTERNAL TABLE gold.fact_sales_table
WITH (
    LOCATION = 'fact_sales/',
    DATA_SOURCE = goldlayer,
    FILE_FORMAT = extfileformat
)
AS SELECT * FROM gold.fact_sales;

-- ===============================
-- VALIDATION QUERIES
-- ===============================

-- Sample View Queries
SELECT TOP 100 * FROM gold.dim_customer;
SELECT TOP 100 * FROM gold.dim_geolocation;
SELECT TOP 100 * FROM gold.dim_order_items;
SELECT TOP 100 * FROM gold.dim_order_payments;
SELECT TOP 100 * FROM gold.dim_order_reviews;
SELECT TOP 100 * FROM gold.dim_orders;
SELECT TOP 100 * FROM gold.dim_product;
SELECT TOP 100 * FROM gold.dim_seller;
SELECT TOP 100 * FROM gold.fact_sales;
SELECT TOP 100 * FROM gold.fact_sales_agg;
SELECT TOP 100 * FROM gold.bridge_order_items;
SELECT TOP 100 * FROM gold.fact_order_payments_partitioned;

-- Snapshot Table Query
SELECT TOP 100 * FROM gold.fact_sales_table;

-- Row Count (snapshot table)
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
SELECT * FROM sys.external_tables WHERE name = 'fact_sales_table';

-- View column definitions
SELECT * 
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'gold' AND TABLE_NAME = 'fact_sales_table';
