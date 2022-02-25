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
Tests basic methods from the BookkeepingDB
It requires an Oracle database
"""

import sys

import pytest

from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise


@pytest.fixture
def bkkDB():
    from DIRAC.Core.Base.Script import parseCommandLine

    argv, sys.argv = sys.argv, sys.argv[:1]
    parseCommandLine()
    sys.argv = argv + ["-ddd"]
    from LHCbDIRAC.BookkeepingSystem.DB.OracleBookkeepingDB import OracleBookkeepingDB

    yield OracleBookkeepingDB()


def test_dataTakingConditions(bkkDB):

    assert returnValueOrRaise(bkkDB._getDataTakingConditionId("NonExistingDescription")) == -1

    # Insert a condition with only the description, and check the id
    dtcId = returnValueOrRaise(bkkDB.insertDataTakingCondDesc("JustADescription"))
    try:
        assert returnValueOrRaise(bkkDB._getDataTakingConditionId("JustADescription")) == dtcId
    finally:
        returnValueOrRaise(bkkDB.deleteDataTakingCondition(dtcId))

    # Make sure the deletion has worked
    assert returnValueOrRaise(bkkDB._getDataTakingConditionId("JustADescription")) == -1

    dataTakingDict = {
        "Description": "Description",
        "BeamCond": "BeamCond",
        "BeamEnergy": "BeamEnergy",
        "MagneticField": "MagneticField",
        "VELO": "VELO",
        "IT": "IT",
        "TT": "TT",
        "OT": "OT",
        "RICH1": "RICH1",
        "RICH2": "RICH2",
        "SPD_PRS": "SPD_PRS",
        "ECAL": "ECAL",
        "HCAL": "HCAL",
        "MUON": "MUON",
        "L0": "L0",
        "HLT": "HLT",
        "VeloPosition": "VeloPosition",
    }

    # Insert a condition with the full detector details (this is what we want to suppress)
    dtcId = returnValueOrRaise(bkkDB.insertDataTakingCond(dataTakingDict))
    # Get it back
    try:
        assert returnValueOrRaise(bkkDB._getDataTakingConditionId("Description")) == dtcId
    finally:
        returnValueOrRaise(bkkDB.deleteDataTakingCondition(dtcId))
