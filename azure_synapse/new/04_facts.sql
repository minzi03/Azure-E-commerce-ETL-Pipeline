/* =========================================================
   04. FACT & BRIDGE VIEWS
   ========================================================= */

CREATE OR ALTER VIEW gold.fact_sales AS
SELECT *
FROM OPENROWSET(
    BULK 'fact_sales/',
    DATA_SOURCE = 'goldlayer',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE OR ALTER VIEW gold.fact_sales_agg AS
SELECT *
FROM OPENROWSET(
    BULK 'fact_sales_agg/',
    DATA_SOURCE = 'goldlayer',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE OR ALTER VIEW gold.fact_sales_partitioned AS
SELECT *
FROM OPENROWSET(
    BULK 'fact_sales_partitioned/',
    DATA_SOURCE = 'goldlayer',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE OR ALTER VIEW gold.fact_order_payments_partitioned AS
SELECT *
FROM OPENROWSET(
    BULK 'fact_order_payments_partitioned/',
    DATA_SOURCE = 'goldlayer',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE OR ALTER VIEW gold.bridge_order_items AS
SELECT *
FROM OPENROWSET(
    BULK 'bridge_order_items/',
    DATA_SOURCE = 'goldlayer',
    FORMAT = 'DELTA'
) AS rows;
GO

SELECT TOP 10 * FROM gold.fact_sales;
GO

SELECT TOP 10 * FROM gold.bridge_order_items;
GO