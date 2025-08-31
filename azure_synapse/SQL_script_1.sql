-- Create 'gold' schema if it does not exist
IF NOT EXISTS (
    SELECT * FROM sys.schemas WHERE name = 'gold'
)
BEGIN
    EXEC('CREATE SCHEMA gold');
END;

-- Define external file format for reading Parquet files with Snappy compression
CREATE EXTERNAL FILE FORMAT extfileformat
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

-- Create master key (only needed once per database)
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Duy@123';

-- Create a database-scoped credential using Managed Identity
CREATE DATABASE SCOPED CREDENTIAL WorkspaceIdentity
WITH IDENTITY = 'Managed Identity';

-- Create an external data source that links to the Gold layer folder in ADLS Gen2
CREATE EXTERNAL DATA SOURCE goldlayer
WITH (
    LOCATION = 'https://olistetlstg.dfs.core.windows.net/olistdata/gold/',
    CREDENTIAL = WorkspaceIdentity
);

-- Create views for dimension and fact tables by reading Parquet folders
CREATE OR ALTER VIEW gold.dim_customers AS
SELECT * FROM OPENROWSET(
    BULK 'customers/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET'
) AS rows;

CREATE OR ALTER VIEW gold.dim_products AS
SELECT * FROM OPENROWSET(
    BULK 'products/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET'
) AS rows;

CREATE OR ALTER VIEW gold.dim_sellers AS
SELECT * FROM OPENROWSET(
    BULK 'sellers/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET'
) AS rows;

CREATE OR ALTER VIEW gold.dim_geolocation AS
SELECT * FROM OPENROWSET(
    BULK 'geolocation/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET'
) AS rows;

CREATE OR ALTER VIEW gold.dim_reviews AS
SELECT * FROM OPENROWSET(
    BULK 'reviews/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET'
) AS rows;

CREATE OR ALTER VIEW gold.dim_payments AS
SELECT * FROM OPENROWSET(
    BULK 'payments/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET'
) AS rows;

CREATE OR ALTER VIEW gold.dim_items AS
SELECT * FROM OPENROWSET(
    BULK 'items/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET'
) AS rows;

CREATE OR ALTER VIEW gold.fact_orders AS
SELECT * FROM OPENROWSET(
    BULK 'fact_orders/', DATA_SOURCE = 'goldlayer', FORMAT = 'PARQUET'
) AS rows;

-- Create snapshot external table for fact_orders
CREATE EXTERNAL TABLE gold.fact_orders_table
WITH (
    LOCATION = 'gold/fact_orders_table/',
    DATA_SOURCE = goldlayer,
    FILE_FORMAT = extfileformat
)
AS SELECT * FROM gold.fact_orders;

SELECT TOP 100 * FROM gold.dim_customers;
SELECT TOP 100 * FROM gold.dim_geolocation;
SELECT TOP 100 * FROM gold.dim_items;
SELECT TOP 100 * FROM gold.dim_payments;
SELECT TOP 100 * FROM gold.dim_products;
SELECT TOP 100 * FROM gold.dim_reviews;
SELECT TOP 100 * FROM gold.dim_sellers;
SELECT TOP 100 * FROM gold.fact_orders;

SELECT TOP 100 * FROM gold.fact_orders_table;

-- Row count (Snapshot)
SELECT COUNT(*) AS total_rows FROM gold.fact_orders_table;

-- Column count (Snapshot)
SELECT COUNT(*) AS total_columns
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'gold' AND TABLE_NAME = 'fact_orders_table';

-- Row count (Direct from Parquet)
SELECT COUNT(*) AS total_rows FROM gold.fact_orders;

-- Column count (Direct from Parquet)
SELECT COUNT(*) AS total_columns
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'gold' AND TABLE_NAME = 'fact_orders';

-- Check if schema 'gold' exists
SELECT * FROM sys.schemas WHERE name = 'gold';

-- Verify file format
SELECT * FROM sys.external_file_formats WHERE name = 'extfileformat';

-- Check managed identity credential
SELECT * FROM sys.database_scoped_credentials WHERE name = 'WorkspaceIdentity';

-- Check data source configuration
SELECT * FROM sys.external_data_sources WHERE name = 'goldlayer';

-- List all views in schema 'gold'
SELECT name, type_desc
FROM sys.objects
WHERE schema_id = SCHEMA_ID('gold') AND type = 'V';

-- List all external tables in schema 'gold'
SELECT * FROM sys.external_tables WHERE name = 'fact_orders_table';

-- View column metadata
SELECT * 
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'gold' AND TABLE_NAME = 'fact_orders_table';
