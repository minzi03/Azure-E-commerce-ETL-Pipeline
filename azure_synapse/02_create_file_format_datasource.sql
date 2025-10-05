-- 02. CREATE FILE FORMAT & DATA SOURCE

-- External file format (Parquet + Snappy)
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'extfileformat')
CREATE EXTERNAL FILE FORMAT extfileformat
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

-- External data source (Gold Layer)
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'goldlayer')
CREATE EXTERNAL DATA SOURCE goldlayer
WITH (
    LOCATION = 'https://olistetlstga.dfs.core.windows.net/olistdata/gold/',
    CREDENTIAL = WorkspaceIdentity
);

SELECT * FROM sys.external_data_sources WHERE name = 'goldlayer';