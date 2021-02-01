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
"""
This tests the chain
MCStatsElasticDBClient > MCStatsElasticDBHandler > MCStatsElasticDBs (several of them)

It assumes the server is running and that ES is present and running
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


import time

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from .MCStatsSampleData import gauss_errors_1, boole_errors_1

# sut
from LHCbDIRAC.ProductionManagementSystem.Client.MCStatsClient import MCStatsClient


mcStatsClient = MCStatsClient()

# db = {
#     'XMLSummary': elasticMCGaussLogErrorsDB,
#     'booleErrors': elasticMCGaussLogErrorsDB,
#     'gaussErrors': elasticMCGaussLogErrorsDB,
# }


def test_setAndGetandRemove():

  # Set gauss errors
  result = mcStatsClient.set('gaussErrors', gauss_errors_1)
  assert result['OK'] is True, result['Message']

  # Set boole errors
  result = mcStatsClient.set('booleErrors', boole_errors_1)
  assert result['OK'] is True

  time.sleep(1)

  # Get gauss errors
  result = mcStatsClient.get('gaussErrors', 4)
  assert result['OK'] is True
  assert result['Value'] == [gauss_errors_1]

  # Get boole errors
  result = mcStatsClient.get('booleErrors', 4)
  assert result['OK'] is True
  assert result['Value'] == [boole_errors_1]

  # Get false
  result = mcStatsClient.get('false', 4)
  assert result['OK'] is False

  # Get non-existant
  result = mcStatsClient.get('booleErrors', 5)
  assert result['OK'] is True
  assert result['Value'] == []

  # Remove
  mcStatsClient.remove('gaussErrors', 4)
  time.sleep(1)
  result = mcStatsClient.get('gaussErrors', 4)
  assert result['OK'] is True
  assert result['Value'] == []
  mcStatsClient.remove('booleErrors', 4)
  time.sleep(1)
  result = mcStatsClient.get('booleErrors', 4)
  assert result['OK'] is True
  assert result['Value'] == []
