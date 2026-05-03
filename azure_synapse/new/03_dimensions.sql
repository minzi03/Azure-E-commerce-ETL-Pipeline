/* =========================================================
   03. DIMENSION VIEWS
   Note:
   - Gold outputs from Databricks are Delta
   - Read them with OPENROWSET ... FORMAT = 'DELTA'
   ========================================================= */

CREATE OR ALTER VIEW gold.dim_customer AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_customer/',
    DATA_SOURCE = 'goldlayer',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE OR ALTER VIEW gold.dim_geolocation AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_geolocation/',
    DATA_SOURCE = 'goldlayer',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE OR ALTER VIEW gold.dim_orders AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_orders/',
    DATA_SOURCE = 'goldlayer',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE OR ALTER VIEW gold.dim_product AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_product/',
    DATA_SOURCE = 'goldlayer',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE OR ALTER VIEW gold.dim_seller AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_seller/',
    DATA_SOURCE = 'goldlayer',
    FORMAT = 'DELTA'
) AS rows;
GO

SELECT TOP 10 * FROM gold.dim_customer;
GO