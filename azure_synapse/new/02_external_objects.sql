/* =========================================================
   02. EXTERNAL DATA SOURCE
   ========================================================= */

IF NOT EXISTS (
    SELECT 1
    FROM sys.external_data_sources
    WHERE name = 'goldlayer'
)
BEGIN
    CREATE EXTERNAL DATA SOURCE goldlayer
    WITH (
        LOCATION = 'https://olistetlstga.dfs.core.windows.net/olistdata/gold/',
        CREDENTIAL = WorkspaceIdentity
    );
END;
GO

SELECT *
FROM sys.external_data_sources
WHERE name = 'goldlayer';
GO