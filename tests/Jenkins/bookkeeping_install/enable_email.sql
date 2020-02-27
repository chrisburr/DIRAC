WHENEVER SQLERROR EXIT SQL.SQLCODE;
SET ECHO ON;

ALTER SESSION SET container = BKDBPDB;

@$ORACLE_HOME/rdbms/admin/utlmail.sql
@$ORACLE_HOME/rdbms/admin/prvtmail.plb
ALTER SYSTEM SET smtp_out_server='smtp.domain.com' SCOPE=SPFILE;
GRANT EXECUTE ON utl_mail TO SYSTEM;
