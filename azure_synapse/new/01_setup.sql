/* =========================================================
   01. SETUP: SCHEMA, MASTER KEY, CREDENTIAL
   ========================================================= */

-- 1. Create schema gold if not exists
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END;
GO

-- 2. Create master key if not exists
IF NOT EXISTS (
    SELECT 1
    FROM sys.symmetric_keys
    WHERE name = '##MS_DatabaseMasterKey##'
)
BEGIN
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Duy@123';
END;
GO

-- 3. Create database scoped credential for Managed Identity
IF NOT EXISTS (
    SELECT 1
    FROM sys.database_scoped_credentials
    WHERE name = 'WorkspaceIdentity'
)
BEGIN
    CREATE DATABASE SCOPED CREDENTIAL WorkspaceIdentity
    WITH IDENTITY = 'Managed Identity';
END;
GO

SELECT *
FROM sys.database_scoped_credentials
WHERE name = 'WorkspaceIdentity';
GO