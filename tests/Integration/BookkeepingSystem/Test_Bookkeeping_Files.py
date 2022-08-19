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
It tests the insert of XML Summaries to the BookkeepingDB.
"""
# pylint: disable=invalid-name,wrong-import-position

import pytest

from DIRAC.Core.Base.Script import parseCommandLine

parseCommandLine()

from DIRAC import gLogger

gLogger.setLevel("DEBUG")

from .Utilities import wipeOutDB, addBasicData, insertRAWFiles, rawFiles_1, runnb_1, runnb_2

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

    # Delete again the DB
    wipeOutDB(bkDB)


#############################################################################


def test_ping():
    """make sure we are able to contact the bkk service" """

    res = bk.ping()
    assert res["OK"]


def test_getRunInformation(wipeout):
    """
    Test the run metadata
    """
    retVal = bk.getRunInformation({"RunNumber": runnb_1})
    assert retVal["OK"], retVal["Message"]
    assert str(runnb_1) not in retVal["Value"]
    assert sorted(retVal["Value"][runnb_1]) == sorted(
        [
            "ConfigName",
            "JobEnd",
            "ConditionDescription",
            "ProcessingPass",
            "FillNumber",
            "DDDB",
            "JobStart",
            "TCK",
            "CONDDB",
            "ConfigVersion",
        ]
    )
    result = dict(retVal["Value"][runnb_1])
    result.pop("JobStart")
    result.pop("JobEnd")
    assert result == {
        "ConfigName": "Test",
        "ConditionDescription": "Beam450GeV-MagDown",
        "ProcessingPass": "/Real Data",
        "FillNumber": 29,
        "DDDB": "xyz",
        "TCK": "-0x7f6bffff",
        "CONDDB": "xy",
        "ConfigVersion": "Test01",
    }


def test_getListOfFills(wipeout):
    retVal = bk.getListOfFills({"ConfigName": "Test", "ConfigVersion": "Test01"})
    assert retVal["OK"], retVal["Message"]
    assert retVal["Value"] == [29]


def test_getRunsForFill(wipeout):
    retVal = bk.getRunsForFill(29)
    assert retVal["OK"], retVal["Message"]
    assert retVal["Value"] == [runnb_1]


def test_getRunInformations(wipeout):
    retVal = bk.getRunInformations(1124)
    assert retVal["OK"] is False

    res = bk.addReplica("test")
    assert res["OK"] is False

    retVal = bk.getRunInformations(runnb_1)
    assert retVal["OK"], retVal["Message"]
    assert retVal["Value"]["Configuration Name"] == "Test"
    assert retVal["Value"]["Configuration Version"] == "Test01"
    assert retVal["Value"]["DataTakingDescription"] == "Beam450GeV-MagDown"
    assert retVal["Value"]["File size"] == [8201582930]
    assert retVal["Value"]["FillNumber"] == 29
    assert retVal["Value"]["FullStat"] == [2145]
    assert retVal["Value"]["InstLuminosity"] == [0]
    assert retVal["Value"]["Number of events"] == [45000]
    assert retVal["Value"]["Number of file"] == [5]
    assert retVal["Value"]["ProcessingPass"] == "/Real Data"
    assert retVal["Value"]["Stream"] == [30000000]
    assert retVal["Value"]["Tck"] == "-0x7f6bffff"
    assert retVal["Value"]["TotalLuminosity"] == 121222.33
    assert retVal["Value"]["luminosity"] == [6061.165]

    retVal = bk.getRunInformations(runnb_2)
    assert retVal["OK"], retVal["Message"]
    assert retVal["Value"]["Configuration Name"] == "Test"
    assert retVal["Value"]["Configuration Version"] == "Test02"
    assert retVal["Value"]["DataTakingDescription"] == "Beam450GeV-MagDown"
    assert retVal["Value"]["File size"] == [9841899516]
    assert retVal["Value"]["FillNumber"] == 30
    assert retVal["Value"]["FullStat"] == [2574]
    assert retVal["Value"]["InstLuminosity"] == [0]
    assert retVal["Value"]["Number of events"] == [54000]
    assert retVal["Value"]["Number of file"] == [6]
    assert retVal["Value"]["ProcessingPass"] == "/Real Data"
    assert retVal["Value"]["Stream"] == [30000000]
    assert retVal["Value"]["Tck"] == "-0x7f6bffff"
    assert retVal["Value"]["TotalLuminosity"] == 121222.33
    assert retVal["Value"]["luminosity"] == [7273.398]


def test_getRunFiles(wipeout):
    retVal = bk.getRunFiles(runnb_1)
    assert retVal["OK"], retVal["Message"]
    assert len(retVal["Value"]) == 5

    files = [
        "/lhcb/data/2016/RAW/Test/test/1122/0001122_test_1.raw",
        "/lhcb/data/2016/RAW/Test/test/1122/0001122_test_0.raw",
        "/lhcb/data/2016/RAW/Test/test/1122/0001122_test_4.raw",
        "/lhcb/data/2016/RAW/Test/test/1122/0001122_test_3.raw",
        "/lhcb/data/2016/RAW/Test/test/1122/0001122_test_2.raw",
    ]

    runMeta = ["FullStat", "Luminosity", "FileSize", "EventStat", "GotReplica", "GUID", "InstLuminosity"]
    for rec in retVal["Value"]:
        assert rec in files
        assert sorted(retVal["Value"][rec]) == sorted(runMeta)


def test_getRunNbAndTck(wipeout):
    retVal = bk.getRunNbAndTck("/lhcb/data/2016/RAW/Test/test/1122/0001122_test_1.raw")
    assert retVal["OK"], retVal["Message"]
    assert retVal["Value"] == [(runnb_1, "-0x7f6bffff")]


def test_getRunFilesDataQuality(wipeout):
    retVal = bk.getRunFilesDataQuality(runnb_1)
    assert retVal["OK"], retVal["Message"]
    assert retVal["Value"] == [(runnb_1, "OK", 30000000)]


def test_getNbOfRawFiles(wipeout):
    retVal = bk.getNbOfRawFiles({"RunNumber": runnb_1})
    assert retVal["OK"], retVal["Message"]
    assert retVal["Value"] == 5


def test_getFiles(wipeout):
    """test of getFiles method"""
    bkQueryDict = {
        "ConfigName": "Test",
        "ConfigVersion": "Test01",
        "FileType": "RAW",
    }
    res = bk.getFiles(bkQueryDict)
    assert res["OK"], res["Message"]
    assert len(res["Value"]) == 5

    bkQueryDict = {
        "ConfigName": "Test",
        "ConfigVersion": "Test02",
        "FileType": "RAW",
    }
    res = bk.getFiles(bkQueryDict)
    assert res["OK"], res["Message"]
    assert len(res["Value"]) == 6

    bkQueryDict = {
        "ConfigName": "Test",
        "ConfigVersion": "Test02",
        "RunNumber": [runnb_1],
    }
    res = bk.getFiles(bkQueryDict)
    assert res["OK"], res["Message"]
    assert len(res["Value"]) == 0

    # FIXME: these following 2 ones fail because by adding RunNumber
    # the code buids a query involving prodrunview, which happens to be
    # empty on the test setup
    # prodrunview is not a view but a real table, that should be filled up
    # by a procedure. In the test setup the procedure is deployed but the table
    # is anywat empty.
    # 2 cases:
    # - this test setup is broken
    # - using prodrunview brakes these queries
    # in theory prodrunview is there for speeding up things

    # bkQueryDict = {
    #     "ConfigName": "Test",
    #     "ConfigVersion": "Test02",
    #     "RunNumber": [runnb_2],
    # }
    # res = bk.getFiles(bkQueryDict)
    # assert res["OK"], res["Message"]
    # assert len(res["Value"]) == 6

    # bkQueryDict = {
    #     "ConfigName": "Test",
    #     "RunNumber": [runnb_1, runnb_2],
    #     "FileType": "RAW",
    # }
    # res = bk.getFiles(bkQueryDict)
    # assert res["OK"], res["Message"]
    # assert len(res["Value"]) == 11

    bkQueryDict = {
        "ConfigName": "Test",
        "ConfigVersion": "Test02",
        "FileType": "NOT",
    }
    res = bk.getFiles(bkQueryDict)
    assert res["OK"], res["Message"]
    assert len(res["Value"]) == 0


def test_addFiles(wipeout):
    """
    add replica flag
    """
    retVal = bk.addFiles(rawFiles_1)
    assert retVal["OK"], retVal["Message"]
    assert retVal["Value"]["Failed"] == []
    assert retVal["Value"]["Successful"] == rawFiles_1

    retVal = bk.addFiles("test.txt")
    assert retVal["OK"], retVal["Message"]
    assert retVal["Value"]["Successful"] == []
    assert retVal["Value"]["Failed"] == ["test.txt"]


def test_fileMetadata(wipeout):
    """
    test the file metadata method
    """
    fileParams = [
        "GUID",
        "ADLER32",
        "FullStat",
        "EventType",
        "FileType",
        "MD5SUM",
        "VisibilityFlag",
        "InsertTimeStamp",
        "RunNumber",
        "JobId",
        "Luminosity",
        "FileSize",
        "EventStat",
        "GotReplica",
        "CreationDate",
        "InstLuminosity",
        "DataqualityFlag",
    ]
    retVal = bk.getFileMetadata(rawFiles_1)

    assert retVal["OK"], retVal["Message"]
    assert retVal["Value"]["Failed"] == []
    assert len(retVal["Value"]["Successful"]) == len(rawFiles_1)
    assert sorted(retVal["Value"]["Successful"]) == sorted(rawFiles_1)
    # make sure the files has all parameters
    for fName in retVal["Value"]["Successful"]:
        assert sorted(retVal["Value"]["Successful"][fName]) == sorted(fileParams)

    retVal = bk.getFileMetadata("test.txt")
    assert retVal["OK"], retVal["Message"]
    assert retVal["Value"]["Successful"] == {}
    assert retVal["Value"]["Failed"] == ["test.txt"]


def test_getAvailableFileTypes(wipeout):
    """
    retrieve the file types
    """

    retVal = bk.getAvailableFileTypes()
    assert retVal["OK"], retVal["Message"]
    assert len(retVal["Value"]) > 0


def test_removeFiles(wipeout):
    """
    Set the replica flag to no
    """

    retVal = bk.removeFiles(rawFiles_1)
    assert retVal["OK"], retVal["Message"]
    assert retVal["Value"]["Failed"] == []
    assert retVal["Value"]["Successful"] == rawFiles_1

    retVal = bk.removeFiles("test.txt")
    assert retVal["OK"], retVal["Message"]
    assert retVal["Value"]["Successful"] == []
    assert retVal["Value"]["Failed"] == ["test.txt"]
