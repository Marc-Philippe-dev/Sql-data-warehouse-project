/*
================================================================================
DATABASE INITIALIZATION SCRIPT - DATA WAREHOUSE PROJECT
================================================================================

Script Name:    database_init.sql
Purpose:        Initialize the DataWarehouse database with Medallion Architecture schemas
Author:         [GNANCADJA Marc Philippe]
Created:        [03-12-2025]
Modified:       [Date]
Version:        1.0

--------------------------------------------------------------------------------
DESCRIPTION
--------------------------------------------------------------------------------
This script performs a complete initialization of the DataWarehouse database:

1. Database Setup:
   - Checks for existing 'DataWarehouse' database
   - Drops existing database if present (with forced disconnect of active users)
   - Creates a fresh 'DataWarehouse' database

2. Schema Creation (Medallion Architecture):
   - bronze  : Raw data layer (source data as-is)
   - silver  : Cleaned and transformed data layer
   - gold    : Business-ready analytics layer (star schema)

--------------------------------------------------------------------------------
⚠️  CRITICAL WARNING
--------------------------------------------------------------------------------
*** DESTRUCTIVE OPERATION ***

This script will:
✗ DROP the entire 'DataWarehouse' database if it exists
✗ PERMANENTLY DELETE all tables, data, stored procedures, and objects
✗ FORCE DISCONNECT all active database connections

PREREQUISITES:
✓ Ensure you have proper database backups
✓ Verify you have sysadmin or dbcreator permissions
✓ Confirm this is the intended environment (DEV/TEST/PROD)
✓ Notify team members if running in shared environments

DO NOT RUN IN PRODUCTION without proper authorization and backup verification!

--------------------------------------------------------------------------------
USAGE INSTRUCTIONS
--------------------------------------------------------------------------------
1. Review the WARNING section above
2. Verify backups are current (if applicable)
3. Execute this script in SQL Server Management Studio (SSMS)
4. Confirm successful execution by checking for the three schemas

Expected execution time: < 5 seconds

--------------------------------------------------------------------------------
EXECUTION ORDER
--------------------------------------------------------------------------------
This script should be executed FIRST in the following sequence:
  1. ✓ 00_database_init.sql    (THIS SCRIPT)
  2. ✓ 01_ddl_bronze.sql
  3. ✓ 02_proc_load_bronze_.sql
  4. ✓ 01_ddl_silver.sql
  5. ✓ 02_proc_load_silver_data.sql
  6. ✓ ddl_gold.sql 

--------------------------------------------------------------------------------
ROLLBACK / CLEANUP
--------------------------------------------------------------------------------
To remove the database created by this script:

    USE master;
    GO
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
    GO

--------------------------------------------------------------------------------
VALIDATION QUERIES
--------------------------------------------------------------------------------
After running this script, verify successful execution:

-- Check if database exists
SELECT name, database_id, create_date 
FROM sys.databases 
WHERE name = 'DataWarehouse';

-- Check if schemas were created
SELECT s.name AS schema_name, 
       s.schema_id,
       u.name AS schema_owner
FROM DataWarehouse.sys.schemas s
INNER JOIN DataWarehouse.sys.database_principals u 
    ON s.principal_id = u.principal_id
WHERE s.name IN ('bronze', 'silver', 'gold')
ORDER BY s.name;

--------------------------------------------------------------------------------
TROUBLESHOOTING
--------------------------------------------------------------------------------
Issue: "Cannot drop database 'DataWarehouse' because it is currently in use"
Solution: The script uses SINGLE_USER WITH ROLLBACK IMMEDIATE to force disconnect

Issue: "CREATE DATABASE permission denied"
Solution: Ensure you have sysadmin or dbcreator server role

Issue: "'CREATE SCHEMA' must be the first statement in a query batch"
Solution: This error occurs if PRINT statements precede CREATE SCHEMA. Use GO to separate batches.

--------------------------------------------------------------------------------
CHANGE LOG
--------------------------------------------------------------------------------
Version | Date       | Author      | Description
--------|------------|-------------|--------------------------------------------
1.0     | [Date]     | [Name]      | Initial script creation

================================================================================
*/

-- ============================================================================
-- SECTION 1: Switch to Master Database
-- ============================================================================
-- Purpose: Ensure we're in the master database to perform database-level operations
-- Note: Database creation/deletion must be done from master or another database

USE master;
GO

-- ============================================================================
-- SECTION 2: Drop Existing Database (if exists)
-- ============================================================================
-- Purpose: Clean slate initialization by removing any existing DataWarehouse
-- Method: 
--   1. Check if database exists in sys.databases catalog view
--   2. Force all connections to close immediately (ROLLBACK IMMEDIATE)
--   3. Drop the database completely

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    PRINT 'Database "DataWarehouse" detected. Initiating drop sequence...';
    
    -- Force single user mode and rollback any active transactions
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    
    PRINT 'Active connections terminated. Dropping database...';
    DROP DATABASE DataWarehouse;
    
    PRINT '✓ Database "DataWarehouse" successfully dropped.';
END
ELSE
BEGIN
    PRINT 'Database "DataWarehouse" does not exist. Proceeding with creation...';
END;
GO

-- ============================================================================
-- SECTION 3: Create New Database
-- ============================================================================
-- Purpose: Create a fresh DataWarehouse database with default settings
-- Note: This uses SQL Server default file locations and settings

PRINT 'Creating new "DataWarehouse" database...';
CREATE DATABASE DataWarehouse;
PRINT '✓ Database "DataWarehouse" successfully created.';
GO

-- ============================================================================
-- SECTION 4: Switch to New Database
-- ============================================================================
-- Purpose: Set context to the newly created database for schema creation

USE DataWarehouse;
GO

-- ============================================================================
-- SECTION 5: Create Medallion Architecture Schemas
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Bronze Schema: Raw Data Layer
-- ----------------------------------------------------------------------------
-- Purpose: Store unprocessed data exactly as received from source systems
-- Characteristics: 
--   - No transformations applied
--   - Historical archive of source data
--   - May contain data quality issues

CREATE SCHEMA bronze;
GO

PRINT '✓ Schema "bronze" created (Raw Data Layer).';
GO

-- ----------------------------------------------------------------------------
-- Silver Schema: Cleaned Data Layer
-- ----------------------------------------------------------------------------
-- Purpose: Store cleansed, validated, and conformed data
-- Characteristics:
--   - Data quality rules applied
--   - Standardized formats
--   - Deduplication performed
--   - Ready for business logic

CREATE SCHEMA silver;
GO

PRINT '✓ Schema "silver" created (Cleaned Data Layer).';
GO

-- ----------------------------------------------------------------------------
-- Gold Schema: Analytics Layer
-- ----------------------------------------------------------------------------
-- Purpose: Store business-ready data models (star schema)
-- Characteristics:
--   - Dimensional models (facts and dimensions)
--   - Aggregated metrics
--   - Optimized for query performance
--   - Ready for BI tools and reporting

CREATE SCHEMA gold;
GO

PRINT '✓ Schema "gold" created (Analytics Layer).';
GO

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================
PRINT '';
PRINT '================================================================================';
PRINT '✓ DATABASE INITIALIZATION COMPLETE';
PRINT '================================================================================';
PRINT 'Database Name:    DataWarehouse';
PRINT 'Schemas Created:  bronze, silver, gold';
PRINT 'Status:           Ready for table creation';
PRINT '';
PRINT 'Next Steps:';
PRINT '  1. Execute bronze layer scripts to create raw data tables';
PRINT '  2. Load data into bronze tables';
PRINT '  3. Continue with silver and gold layer transformations';
PRINT '================================================================================';
GO