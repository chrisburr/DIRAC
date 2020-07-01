###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
"""Module holding MCStatsClient class."""

from DIRAC.Core.Base.Client import Client, createClient


@createClient('ProductionManagement/MCStatsElasticDB')
class MCStatsClient(Client):
  """Client for MCStatsElasticDB.

  Can be specialized client by setting MCStatsClient().indexName
  """

  def __init__(self, **kwargs):
    """simple constructor."""

    super(MCStatsClient, self).__init__(**kwargs)
    self.setServer('ProductionManagement/MCStatsElasticDB')

    self.indexName = 'lhcb-mclogerrors'

  def set(self, typeName, data):
    """set some data in a certain type.

    :params str typeName: type name (e.g. 'LogErr')
    :params dict data: dictionary inserted

    :returns: S_OK/S_ERROR
    """
    return self._getRPC().set(self.indexName, typeName, data)

  def get(self, jobID, mcType):
    """get per Job ID.

    :params int jobID: WMS Job ID
    """
    return self._getRPC().get(self.indexName, jobID, mcType)

  def remove(self, jobID, mcType):
    """remove data for JobID.

    :params int jobID: WMS Job ID
    """
    return self._getRPC().remove(self.indexName, jobID, mcType)
