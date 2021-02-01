.. _databaseschema:

============================
Bookkeeping database schema
============================

.. image:: images/bkRelationaldiagram.png
   :height: 500pt

The schema can be recreated using the following files::

  src/LHCbDIRAC/BookkeepingSystem/DB/database_schema.sql

  src/LHCbDIRAC/BookkeepingSystem/DB/oracle_utilities_stored_procedures.sql

  src/LHCbDIRAC/BookkeepingSystem/DB/oracle_schema_storedprocedures.sql

  src/LHCbDIRAC/BookkeepingSystem/DB/admin_tools.sql

Note: The fist and last file does not need to be executed. If you want to start with an empty database, you have to use them.
