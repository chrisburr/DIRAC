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
This test connects to the BookkeepingManager
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=invalid-name,wrong-import-position

import pytest

from DIRAC.Core.Base.Script import parseCommandLine

parseCommandLine()

from DIRAC import gLogger

gLogger.setLevel("DEBUG")

from .Utilities import wipeOutDB, addBasicData, insertRAWFiles

# sut
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.BookkeepingSystem.DB.OracleBookkeepingDB import OracleBookkeepingDB

# What's used for the tests
bk = BookkeepingClient()
bkDB = OracleBookkeepingDB()


@pytest.fixture
def wipeout():
    # first delete from DB
    wipeOutDB(bkDB)
    # then add some needed data
    addBasicData(bkDB)

    # inserting the files
    insertRAWFiles(bk)

    yield wipeout

    # delete from DB (again)
    wipeOutDB(bkDB)


def test_getAvailableConfigurations(wipeout):
    """
    Must have one configuration name
    """
    res = bk.getAvailableConfigurations()
    assert res["OK"], res["Message"]
    assert len(res["Value"]) > 0


def test_getAvailableConfigNames(wipeout):
    """
    Must have one configuration name
    """
    res = bk.getAvailableConfigNames()
    assert res["OK"], res["Message"]
    assert len(res["Value"]) > 0


def test_getConfigVersions(wipeout):
    res = bk.getConfigVersions({"ConfigName": "Real Data"})
    assert res["OK"], res["Message"]
    assert len(res["Value"]) > 0
    assert res["Value"]["TotalRecords"] == 0


def test_getConditions(wipeout):
    """
    Get the available configurations for a given bk dict
    """
    simParams = [
	"SimId",
	"Description",
	"BeamCondition",
	"BeamEnergy",
	"Generator",
	"MagneticField",
	"DetectorCondition",
	"Luminosity",
	"G4settings",
    ]
    dataParams = [
	"DaqperiodId",
	"Description",
	"BeamCondition",
	"BeanEnergy",
	"MagneticField",
	"VELO",
	"IT",
	"TT",
	"OT",
	"RICH1",
	"RICH2",
	"SPD_PRS",
	"ECAL",
	"HCAL",
	"MUON",
	"L0",
	"HLT",
	"VeloPosition",
    ]

    res = bk.getConditions({"ConfigName": "MC", "ConfigVersion": "2012"})
    assert res["OK"], res["Message"]
    assert len(res["Value"]) > 0
    assert len(res["Value"]) == 2
    assert res["Value"][1]["TotalRecords"] == 0
    assert res["Value"][1]["ParameterNames"] == dataParams
    assert res["Value"][0]["TotalRecords"] > 0
    assert res["Value"][0]["ParameterNames"] == simParams


# def test_getProcessingPass(self):
#     """

#     Check the available processing passes for a given bk path. Again bkk view...

#     """

#     res = bk.getProcessingPass({"ConfigName": "MC", "ConfigVersion": "2012", })
#     assert res["OK"], res["Message"]
#     assert len(res["Value"]) > 0
#     assertEqual(len(res['Value']), 2)
#     assertTrue(res['Value'][0]['TotalRecords'] > 0)


# def test_bookkeepingtree(self):
#     """
#     Browse the bookkeeping database
#     """
#     bkQuery = {"ConfigName": "MC"}
#     res = bk.getAvailableConfigNames()
#     assert res["OK"], res["Message"]
#     assert len(res["Value"]) > 0
#     assertTrue(bkQuery['ConfigName'] in [cName[0] for cName in res['Value']['Records']])

#     res = bk.getConfigVersions({"ConfigName": "MC"})
#     assert res["OK"], res["Message"]
#     assert len(res["Value"]) > 0
#     assertEqual(res['Value']['TotalRecords'], 14)

#     res = bk.getConditions({"ConfigName": "MC",
#                                     "ConfigVersion": "2012"})
#     assert res["OK"], res["Message"]
#     assert len(res["Value"]) > 0
#     assertEqual(res['Value'][0]['TotalRecords'], 2)

#     res = bk.getProcessingPass({"ConfigName": "MC",
#                                         "ConfigVersion": "2012",
#                                         "ConditionDescription": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"})
#     assert res["OK"], res["Message"]
#     assert len(res["Value"]) > 0
#     assertEqual(res['Value'][0]['Records'][0][0], "Sim08a")

#     res = bk.getProcessingPass({"ConfigName": "MC",
#                                         "ConfigVersion": "2012",
#                                         "ConditionDescription": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"},
#                                        "/Sim08a")
#     assert res["OK"], res["Message"]
#     assert len(res["Value"]) > 0
#     assertEqual(res['Value'][0]['Records'][0][0], "Digi13")

#     res = bk.getProcessingPass({"ConfigName": "MC",
#                                         "ConfigVersion": "2012",
#                                         "ConditionDescription": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"},
#                                        "/Sim08a/Digi13")
#     assert res["OK"], res["Message"]
#     assert len(res["Value"]) > 0
#     assertEqual(res['Value'][0]['Records'][0][0], "Trig0x409f0045")

#     res = bk.getProcessingPass({"ConfigName": "MC",
#                                         "ConfigVersion": "2012",
#                                         "ConditionDescription": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"},
#                                        "/Sim08a/Digi13/Trig0x409f0045")
#     assert res["OK"], res["Message"]
#     assert len(res["Value"]) > 0
#     assertEqual(res['Value'][0]['Records'][0][0], "Reco14a")

#     res = bk.getProcessingPass({"ConfigName": "MC",
#                                         "ConfigVersion": "2012",
#                                         "ConditionDescription": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"},
#                                        "/Sim08a/Digi13/Trig0x409f0045/Reco14a")
#     assert res["OK"], res["Message"]
#     assert len(res["Value"]) > 0
#     assertEqual(res['Value'][0]['Records'][0][0], "Stripping20NoPrescalingFlagged")

#     res = bk.getProcessingPass({"ConfigName": "MC",
#                                         "ConfigVersion": "2012",
#                                         "ConditionDescription": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"},
#                                        "/Sim08a/Digi13/Trig0x409f0045/Reco14a/Stripping20NoPrescalingFlagged")
#     assert res["OK"], res["Message"]
#     assert len(res["Value"]) > 0
#     assertEqual(res['Value'][1]['Records'][0][0], 12442001)

#     res = bk.getFileTypes({
#         "ConfigName": "MC",
#         "ConfigVersion": "2012",
#         "ConditionDescription": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8",
#         "ProcessingPass": "/Sim08a/Digi13/Trig0x409f0045/Reco14a/Stripping20NoPrescalingFlagged"})

#     assert res["OK"], res["Message"]
#     assert len(res["Value"]) > 0
#     assertEqual(res['Value']['TotalRecords'], 1)
#     assertEqual(res['Value']['Records'][0][0], 'ALLSTREAMS.DST')

#     res = bk.getFiles({
#         "ConfigName": "MC",
#         "ConfigVersion": "2012",
#         "SimulationConditions": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8",
#         "ProcessingPass": "/Sim08a/Digi13/Trig0x409f0045/Reco14a/Stripping20NoPrescalingFlagged",
#         "FileType": "ALLSTREAMS.DST"})
