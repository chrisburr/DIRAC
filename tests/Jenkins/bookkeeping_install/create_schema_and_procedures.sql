WHENEVER SQLERROR EXIT SQL.SQLCODE;
SET ECHO ON;

ALTER SESSION SET container = BKDBPDB;

-- @LHCbDIRAC/BookkeepingSystem/DB/database_schema_cleaner.sql
@LHCbDIRAC/BookkeepingSystem/DB/database_schema.sql
@LHCbDIRAC/BookkeepingSystem/DB/oracle_schema_storedprocedures.sql
@LHCbDIRAC/BookkeepingSystem/DB/oracle_utilities_stored_procedures.sql
