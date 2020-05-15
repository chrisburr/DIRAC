====================
DBs in certification
====================

Bookkeeping DB (on Oracle)
==========================

For the **Certification** system, which is on ``int12r`` server, we are using 3 database accounts:

#. ``LHCB_DIRACBOOKKEEPING_INT_R`` (reader account, in CS as /Systems/Bookkeeping/Databases/BookkeepingDB/LHCbDIRACBookkeepingUser)
#. ``LHCB_DIRACBOOKKEEPING_INT_W`` (writer account, in CS as /Systems/Bookkeeping/Databases/BookkeepingDB/LHCbDIRACBookkeepingServer)
#. ``LHCB_DIRACBOOKKEEPING_INT`` (main account, which is not used by LHCbDIRAC, but is used for profiling)

You have a few ways to login:

# The simplest is by being inside the CERN network, and using ``sqlplus`` (e.g. from AFS):
::

    source /afs/cern.ch/project/oracle/script/setoraenv.sh
    setoraenv -s 12101
    sqlplus LHCB_DIRACBOOKKEEPING_INT_R@int12r

(``sqlplus`` will look for ``tnsnames.ora`` file for discovering the connection details, including the service name)

# If you are outside the CERN network, you should first set up port forwarding:
::

    ssh -Nf lxplus.cern.ch -L 10120:itrac1609-v.cern.ch:10121

and then you can connect via e.g. `sqldeveloper <https://www.oracle.com/database/technologies/appdev/sql-developer.html>`_
or using `sqlcl <https://www.oracle.com/database/technologies/appdev/sqlcl.html>`_ client:
::

    sql LHCB_DIRACBOOKKEEPING_INT@localhost:10121/int12r_lb.cern.ch


MySQL DBs
=========

First advice: use the python package ``mycli``. If you are outside the CERN network::

    ssh -Nf lxplus.cern.ch -L 5506:dbod-lbcertif.cern.ch:5506
    mycli -u Dirac -h localhost -P 5506

and if you are inside simply::

    mycli -u Dirac -h dbod-lbcertif.cern.ch -P 5506

There's no ``root`` account: the administrator account is ``admin``


ElasticSearch
=============

All ES data for certification is stored in **es-lhcb-dev.cern.ch** at port 9203, and visualized via kibana in `<https://es-lhcb-dev.cern.ch/kibana/app/kibana#/home?_g=()>`_

<TODO: expand on connection details>