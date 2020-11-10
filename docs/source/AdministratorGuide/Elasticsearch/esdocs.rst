======================================
CERN centralized Elasticsearch service
======================================

This document contains all information needed to manage the ES. The ES is provided by CERN IT.
General info can be found in this `documentation <https://esdocs.web.cern.ch/>`_.

-----------------------
Communication channels:
-----------------------

CERN IT ES `homepage <https://information-technology.web.cern.ch/services/Elasticsearch-Service>`_

1. Tickets: open a `snow ticket <https://cern.service-now.com/service-portal?id=service_element&name=Elasticsearch-Service>`_.
2. Mattermost channel: LHCb `specific channel <https://mattermost.web.cern.ch/it-dep/channels/es-for-lhcb>`_ or `IT general channel <https://mattermost.web.cern.ch/it-dep/channels/it-es-project>`_.

-----------------------
Elasticsearch instances
-----------------------

We are using three production instances:

1. es-lhcb-monitoring for monitoring WMS and ComponentMonitoring
2. es-lhcb-dirac-logs for centralized Monitoring
3. es-lhcb-mcstats for MC statistics

and one for certification:

1. es-lhcb-dev for everything

------------------------------------
Elasticsearch performance monitoring
------------------------------------

IT/ES provides monitoring tool for monitoring ES instances. You can access `in the following link <https://es-perfmon-lhcb.cern.ch>`_ 
(accessible from inside the CERN network).

------
Kibana
------

Kibana is used for visualize the data. IT/ES provides a Kibana end point for each ES instance.
You can access using https://instance/kibana for example: https://es-lhcb-monitoring.cern.ch/kibana

All kibana interfaces are CERN internal web pages. If you are outside of the CERN network you need to follow 
`these instructions <https://security.web.cern.ch/recommendations/en/ssh_browsing.shtml>`_.

Note: You can access to kibana, if you are in one of the group: lhcb-dirac, lhcb-geoc, lhcb-gridshifters

---------------------
Managing ES templates
---------------------

Each ES instance has a dedicated template, what you can found in the `repository <https://gitlab.cern.ch/it-elasticsearch-project>`_ by
searching lhcb. For example: `https://gitlab.cern.ch/it-elasticsearch-project/endpoint-lhcb-dirac-logs-settings`.

-------
Curator
-------

Curator can be used for easily manage ES data. It can be used in different purpose. 
We are using it for deleting indexes that are older a certain date. To setup Curator you need to
use the ES template repository (see Managing ES templates section.) and create `curator4.actions` file.
For example: `deleting indexes older a certain period <https://gitlab.cern.ch/it-elasticsearch-project/endpoint-lhcb-dirac-logs-settings/raw/master/curator4.actions>`_.

--------------------------
Re-indexing existing index
--------------------------

You may need to re-index indexes from one cluster to another. You can
use the `following script to reindex <https://gitlab.cern.ch/lhcb-dirac/LHCbDIRACMgmt/-/blob/master/ElasticTools/scripts/reindexWMSMonitoring.py>`_.
