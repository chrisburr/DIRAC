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
MCStatsElasticDBClient > MCStatsElasticDBHandler > MCStatsElasticDB

It assumes the server is running and that ES is present and running
"""

import time

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from LHCbDIRAC.ProductionManagementSystem.Client.MCStatsClient import MCStatsClient


id1 = 1
id2 = 2
falseID = 3
id3 = 4

data1 = {
    "Errors": {
        "ID": {
            "wmsID": "6",
            "ProductionID": "5",
            "JobID": id1
        },
        "Error1": 10,
        "Error2": 5,
        "Error3": 3
    }
}

data2 = {
    "Errors": {
        "ID": {
            "wmsID": "6",
            "ProductionID": "5",
            "JobID": id2
        },
        "Error1": 7,
        "Error2": 9
    }
}

data3 = {
    "Counters": {
        "ID": {
            "prod_job_id": "000000001",
            "ProductionID": "8",
            "JobID": id3
        },
        "ITHitMonitor": {
            "betaGamma": 224730238, 
            "DeltaRay": 4208, 
            "numberHits": 86436
        }, 
        "MCITHitPacker": {
            "PackedData": 86436
        }, 
        "CheckITHits/Diff.": {
            "Energy": 0, 
            "Parent |P|": 9, 
            "TOF": 0, 
            "Displacement": {
                "y": 0, 
                "x": 0, 
                "z": 0
            }, 
            "Entry Point": {
                "y": 0, 
                "x": 0, 
                "z": 0
            }
        }
    }
}

typeName = 'test'
mcType1 = 'errors'
mcType2 = 'summary'

mcStatsClient = MCStatsClient()
mcStatsClient.indexName = 'lhcb-mcstats'


def test_setAndGetandRemove():

  # Set

  # Set data1
  result = mcStatsClient.set(typeName, data1)
  assert result['OK'] is True

  # Set data2
  result = mcStatsClient.set(typeName, data2)
  assert result['OK'] is True

  # Set data3
  result = mcStatsClient.set(typeName, data3)
  assert result['OK'] is True

  time.sleep(5)

  # Get data1
  result = mcStatsClient.get(id1, mcType1)
  assert result['OK'] is True
  assert result['Value'] == data1

  # Get data2
  result = mcStatsClient.get(id2, mcType1)
  assert result['OK'] is True
  assert result['Value'] == data2

  # Get data3
  result = mcStatsClient.get(id3, mcType2)
  assert result['OK'] is True
  assert result['Value'] == data3

  # Get empty
  result = mcStatsClient.get(falseID, mcType1)
  assert result['OK'] is True
  assert result['Value'] == {}

  # Remove

  # Remove data1
  mcStatsClient.remove(id1, mcType1)
  time.sleep(5)
  result = mcStatsClient.get(id1, mcType1)
  assert result['OK'] is True
  assert result['Value'] == {}

  # Remove data2
  mcStatsClient.remove(id2, mcType1)
  time.sleep(5)
  result = mcStatsClient.get(id2, mcType1)
  assert result['OK'] is True
  assert result['Value'] == {}


  # Remove data3
  mcStatsClient.remove(id3, mcType2)
  time.sleep(5)
  result = mcStatsClient.get(id3, mcType2)
  assert result['OK'] is True
  assert result['Value'] == {}

  # # Remove empty
  mcStatsClient.remove(falseID, mcType1)
  time.sleep(5)
  result = mcStatsClient.get(falseID, mcType1)
  assert result['OK'] is True
  assert result['Value'] == {}
