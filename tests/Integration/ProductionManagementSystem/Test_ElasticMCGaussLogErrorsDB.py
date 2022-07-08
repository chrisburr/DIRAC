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
from DIRAC import gLogger

parseCommandLine()
gLogger.setLevel("DEBUG")

from .MCStatsSampleData import gauss_errors_1, gauss_errors_2
from LHCbDIRAC.ProductionManagementSystem.DB.ElasticMCGaussLogErrorsDB import ElasticMCGaussLogErrorsDB

firstExpectedRes = [
    {
	"JobID": "001",
	"ProductionID": "002",
	"wmsID": "001000",
	"timestamp": 1657276525579,
	"Errors": 1,
	"ErrorType": "ERROR Gap not found! ",
    },
    {
	"JobID": "001",
	"ProductionID": "002",
	"wmsID": "001000",
	"timestamp": 1657276525579,
	"Errors": 1,
	"ErrorType": "ERROR EvtGenDecay:: EvtGen particle not decayed [Generation] StatusCode=FAILURE",
    },
    {
	"JobID": "001",
	"ProductionID": "002",
	"wmsID": "001000",
	"timestamp": 1657276525579,
	"Errors": 1,
	"ErrorType": "ERROR No particle with barcode equal to 1!",
    },
    {
	"JobID": "001",
	"ProductionID": "002",
	"wmsID": "001000",
	"timestamp": 1657276525579,
	"Errors": 1,
	"ErrorType": "ERROR No particle with barcode equal to 1!",
    },
]

secondExpectedRes = [
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

db = ElasticMCGaussLogErrorsDB()


def test_Delete():
    # Remove the index
    result = db.deleteIndex(db.indexName)
    assert result["OK"]


def test_Set():
    # First set
    result = db.set(gauss_errors_1)
    time.sleep(1)
    assert result["OK"]

    # Second set
    result = db.set(gauss_errors_2)
    time.sleep(1)
    assert result["OK"]


def test_Get():
    # Get
    result = db.get(productionID="002")
    assert result["OK"]
    assert result["Value"] == firstExpectedRes

    result = db.get(productionID="004")
    assert result["OK"]
    assert result["Value"] == secondExpectedRes

    # Get empty
    result = db.get(productionID="10")  # non-existing
    assert result["OK"]
    assert result["Value"] == []


def test_Remove():
    # Remove
    result = db.remove(productionID="002")
    assert result["OK"]
    # Get again
    time.sleep(1)
    result = db.get(productionID="002")  # removed now
    assert result["OK"]
    assert result["Value"] == []

    # Remove the index
    result = db.deleteIndex(db.indexName)
    assert result["OK"]
