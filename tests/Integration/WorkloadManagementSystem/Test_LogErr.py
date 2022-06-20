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
# Test for LogErr.py
# sut
from LHCbDIRAC.Core.Utilities import LogErr
from DIRAC.tests.Utilities.utils import find_all


logFile = find_all("testLogFile.log", "../", "src/LHCbDIRAC/Core/Utilities/test")[0]
with open(logFile, "r") as f:
    logString = f.read()


def test_ReadFirstLogFile():
    jobID = "001"
    prodID = "100"
    wmsID = "123"
    res = LogErr.readLogFile(logString, jobID, prodID, wmsID, "logErrorTestOutput.json")
    assert res["OK"]
    assert res["Value"] == {
        "JobID": "001",
        "ProductionID": "100",
        "wmsID": "123",
        "timestamp": res["Value"]["timestamp"],
        "ERROR Gap not found!": 11,
        "ERROR EvtGenDecay:: EvtGen particle not decayed [Generation] StatusCode=FAILURE": 1,
        "ERROR No particle with barcode equal to 1!": 4,
        "G4Exception : PART102      issued by : G4ParticleDefintion::G4ParticleDefintionStrange PDGEncoding": 7,
    }
