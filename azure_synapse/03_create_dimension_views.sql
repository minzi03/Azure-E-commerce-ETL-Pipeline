-- 03. DIMENSION TABLES (as VIEWS)

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

SELECT TOP 10 * FROM gold.dim_customer;