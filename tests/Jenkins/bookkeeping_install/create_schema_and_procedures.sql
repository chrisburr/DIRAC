WHENEVER SQLERROR EXIT SQL.SQLCODE;
SET ECHO ON;

ALTER SESSION SET container = BKDBPDB;

-- @src/LHCbDIRAC/BookkeepingSystem/DB/database_schema_cleaner.sql
@src/LHCbDIRAC/BookkeepingSystem/DB/database_schema.sql
@src/LHCbDIRAC/BookkeepingSystem/DB/oracle_schema_storedprocedures.sql
@src/LHCbDIRAC/BookkeepingSystem/DB/oracle_utilities_stored_procedures.sql
