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
Tests set(), get() and remove() from ElasticMCGaussLogErrorsDB
"""

from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import time

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from tests.Integration.ProductionManagementSystem.MCStatsSampleData import gauss_errors_1

# sut
from LHCbDIRAC.ProductionManagementSystem.DB.ElasticMCGaussLogErrorsDB import ElasticMCGaussLogErrorsDB


db = ElasticMCGaussLogErrorsDB()


def test_setandGetandRemove():

  # Remove the index
  result = db.deleteIndex(db.indexName)
  assert result['OK'] is True

  # Set

  result = db.set(gauss_errors_1)
  time.sleep(1)
  assert result['OK'] is True
  # Set data2
  # result = db.set(data2)
  # time.sleep(1)
  # assert result['OK'] is True

  # # Data insertion is not instantaneous, so sleep is needed
  # time.sleep(1)

  # Get

  result = db.get(4)
  assert result['OK'] is True
  assert result['Value'] == [gauss_errors_1]

  # result = db.get(id2)
  # assert result['OK'] is True
  # assert result['Value'] == data2

  # Get empty
  result = db.get(10)  # non-existing
  assert result['OK'] is True
  assert result['Value'] == []

  # Remove
  result = db.remove(4)
  assert result['OK'] is True
  # Get again
  time.sleep(1)
  result = db.get(4)  # removed now
  assert result['OK'] is True
  assert result['Value'] == []

  # Remove the index
  result = db.deleteIndex(db.indexName)
  assert result['OK'] is True
