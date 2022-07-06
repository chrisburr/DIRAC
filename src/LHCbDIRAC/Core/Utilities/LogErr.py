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
"""Reads .log-files and outputs summary of counters as a .json-file and a
.html-file."""
import os
import json

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import TimeUtilities


def readLogFile(logFile, jobID, prodID, wmsID, name="errors.json"):
    """The script that runs everything.

    :param str logFile: the name of the logfile
    :param str jobID: the JobID
    :param str prodID: the production ID
    :param str wmsID: the wmsID
    :param str name: the name of the output json file, standardised to 'errors.json'
    """
    logString = ""
    dictG4Errors = {
        "G4Exception": "",
        "G4 Exception": "",
        "ERROR ": "",
        "FATAL ": "",
        "PYTHIA WARNING ": "",
    }
    errorG4Dict = dict()
    errorDict = dict()
    if isinstance(logFile, str):
        if logFile.endswith(".log"):
            res = getLogString(logFile, logString)
            if not res["OK"]:
                gLogger.warn("Problems in reading %s" % logFile)
                return res
            logString = res["Value"]
        else:
            gLogger.debug("The log is already in a readable string")
            logString = logFile

    reversedKeys = sorted(dictG4Errors, reverse=True)
    for errorString in reversedKeys:
        ctest = logString.count(errorString)
        test = logString.find(errorString)
        for i in range(ctest):
            start = test
            test = logString.find(errorString, start)
            alreadyFound = False
            for error in reversedKeys:
                if error == errorString:
                    break
                checke = logString[test : test + 100].find(error)
                if checke != -1:
                    alreadyFound = True
                    test = test + len(error)
                    break
            if alreadyFound:
                continue

            if test != -1:
                if errorString.find("G4") != -1:
                    check = logString[test : test + 250].find("***")
                    if check != -1:
                        errorBase = logString[test : test + 250].split("***")[0]
                        strippedErrString = errorBase.rstrip().replace("\n", "")
                        if not strippedErrString.startswith("G4Exception-END") and not strippedErrString.startswith(
                            "G4Exception-START"
                        ):
                            if strippedErrString in errorG4Dict:
                                errorG4Dict[strippedErrString] = errorG4Dict[strippedErrString] + 1
                            else:
                                errorG4Dict[strippedErrString] = 1
                        lengthDump = len(errorBase)
                        test = test + lengthDump
                else:
                    errorBase = logString[test : test + 250].split("\n")[0].rstrip()
                    if errorBase in errorDict:
                        errorDict[errorBase] = errorDict[errorBase] + 1
                    else:
                        errorDict[errorBase] = 1
                    lengthDump = len(errorBase)
                    test = test + lengthDump

    for (k, v) in errorG4Dict.items():
	errorDict[k] = v
    createJSONtable(errorDict, name, jobID, prodID, wmsID)
    return S_OK()


################################################


def createJSONtable(errorDict, name, jobID, prodID, wmsID):
    """Creates a JSON file out of the collection of errors listed in dictTotal.

    :param dict errorDict: the dictionary of errors
    :param str name: the name of the JSON file
    :param str jobID: the JobID of the log
    :param str prodID: the ProductionID of the log
    :param str wmsID: the wmsID of the log
    """

    resultList = {}
    counter = 0
    with open(name, "w") as output:
	for errName, nrOfErrs in errorDict.items():
	    print("Error type: ", errName)
	    print("Nr of Err: ", nrOfErrs)
	    for i in range(1, nrOfErrs + 1):
		print(f"Loop {i}")
		result = {}
		result["JobID"] = jobID
		result["ProductionID"] = prodID
		result["wmsID"] = wmsID
		result["timestamp"] = int(TimeUtilities.toEpochMilliSeconds())
		result["Errors"] = 1
		result["ErrorType"] = errName
		resultList[counter] = result
		counter = counter + 1
	json.dump(resultList, output, indent=2)
    gLogger.notice("Finished creating the JSON file with Gauss Erros")


################################################


def getLogString(logFile, logString):
    """Checks if the log file can be opened, and saves the text in logFile into
    logString.

    :param str logFile: the name of the logFile
    :param str logStr: the name of the variable that will save the contents of logFile
    """

    gLogger.notice("Attempting to open %s" % logFile)
    if not os.path.exists(logFile):
        gLogger.error("%s could not be found" % logFile)
        return S_ERROR()
    if os.stat(logFile)[6] == 0:
        gLogger.error("%s is empty" % logFile)
        return S_ERROR()
    with open(logFile, "r") as f:
        logString = f.read()
    gLogger.notice("Successfully read %s" % logFile)
    return S_OK(logString)
