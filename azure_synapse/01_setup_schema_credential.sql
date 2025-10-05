-- 01. SETUP: SCHEMA & MASTER KEY

-- 1. Create 'gold' schema if not exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END;

-- 2. Create master key (if not exists)
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Duy@123';

-- 3. Create database scoped credential (Managed Identity)
IF NOT EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'WorkspaceIdentity')
CREATE DATABASE SCOPED CREDENTIAL WorkspaceIdentity
WITH IDENTITY = 'Managed Identity';

SELECT * FROM sys.database_scoped_credentials WHERE name = 'WorkspaceIdentity';
