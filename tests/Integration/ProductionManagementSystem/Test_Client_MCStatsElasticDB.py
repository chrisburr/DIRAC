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
import time

from DIRAC.Core.Base.Script import parseCommandLine
from DIRAC import gLogger

gLogger.setLevel("DEBUG")
parseCommandLine()

from .MCStatsSampleData import gauss_errors_2, boole_errors_1

# sut
from LHCbDIRAC.ProductionManagementSystem.Client.MCStatsClient import MCStatsClient

mcStatsClient = MCStatsClient()

# db = {
#     'XMLSummary': elasticMCGaussLogErrorsDB,
#     'booleErrors': elasticMCGaussLogErrorsDB,
#     'gaussErrors': elasticMCGaussLogErrorsDB,
# }

expectedRes = [
    {
        "JobID": "001",
        "ProductionID": "004",
        "wmsID": "001000",
        "timestamp": 1657276525579,
        "Errors": 1,
        "ErrorType": "G4Exception : PART102      issued by : G4ParticleDefintion::G4ParticleDefintionStrange PDGEncoding",
    },
    {
        "JobID": "001",
        "ProductionID": "004",
        "wmsID": "001000",
        "timestamp": 1657276525579,
        "Errors": 1,
        "ErrorType": "G4Exception : PART102      issued by : G4ParticleDefintion::G4ParticleDefintionStrange PDGEncoding",
    },
    {
        "JobID": "001",
        "ProductionID": "004",
        "wmsID": "001000",
        "timestamp": 1657276525579,
        "Errors": 1,
        "ErrorType": "G4Exception : PART102      issued by : G4ParticleDefintion::G4ParticleDefintionStrange PDGEncoding",
    },
]


def test_setAndGetandRemove():

    # Set gauss errors
    result = mcStatsClient.set("gaussErrors", gauss_errors_2)
    assert result["OK"]

    # Set boole errors
    result = mcStatsClient.set("booleErrors", boole_errors_1)
    assert result["OK"]
    time.sleep(1)

    # Get gauss errors
    result = mcStatsClient.get("gaussErrors", "004")
    print(result)
    assert result["OK"]
    assert result["Value"] == expectedRes

    # Get boole errors
    result = mcStatsClient.get("booleErrors", 4)
    assert result["OK"]
    assert result["Value"] == [boole_errors_1]

    # Get false
    result = mcStatsClient.get("false", 4)
    assert not result["OK"]

    # Get non-existant
    result = mcStatsClient.get("booleErrors", 5)
    assert result["OK"]
    assert result["Value"] == []

    # Remove
    mcStatsClient.remove("gaussErrors", "004")
    time.sleep(1)
    result = mcStatsClient.get("gaussErrors", "004")
    assert result["OK"]
    assert result["Value"] == []
    mcStatsClient.remove("booleErrors", 4)
    time.sleep(1)
    result = mcStatsClient.get("booleErrors", 4)
    assert result["OK"]
    assert result["Value"] == []
