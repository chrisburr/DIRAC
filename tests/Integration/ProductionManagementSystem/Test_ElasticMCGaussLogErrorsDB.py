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

import time

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from LHCbDIRAC.ProductionManagementSystem.DB.ElasticMCGaussLogErrorsDB import ElasticMCGaussLogErrorsDB


db = ElasticMCGaussLogErrorsDB()

data1 = {
    "wmsID": "5",
    "ProductionID": "4",
    "JobID": "3",
    "G4Exception": 10,
    "The signal decay mode is not defined in the main DECAY.DEC table": 2,
    "G4Exception : StuckTrack": 3,
    "G4Exception : 001": 3
}


def test_setandGetandRemove():

  # Remove the index
  result = db.deleteIndex(db.indexName)
  assert result['OK'] is True

  # Set

  # Set data1
  result = db.set(data1)
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
  assert result['Value'] == [data1]

  # result = db.get(id2)
  # assert result['OK'] is True
  # assert result['Value'] == data2

  # Get empty
  result = db.get(10)  # non-existing
  assert result['OK'] is True
  assert result['Value'] == []

  # Remove the index
  result = db.deleteIndex(db.indexName)
  assert result['OK'] is True
