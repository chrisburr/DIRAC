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
from multiprocessing.sharedctypes import Value
import os
import json

from distutils.version import LooseVersion  # pylint:disable=import-error,no-name-in-module

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import TimeUtilities


def readLogFile(logFile, jobID, prodID, wmsID, name="errors.json"):
    """The script that runs everything.

    :param str logFile: the name of the logfile
    :param str project: the project string of the file
    :param str version: the versio of the project
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
    }
    errorG4Dict = dict()
    errorDict = dict()

    res = getLogString(logFile, logString)
    if not res["OK"]:
        gLogger.warn("Problems in reading %s" % logFile)
        return res
    logString = res["Value"]

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
                    errorBase = logString[test : test + 250].split("\n")[0]
                    if errorBase in errorDict:
                        errorDict[errorBase] = errorDict[errorBase] + 1
                    else:
                        errorDict[errorBase] = 1
                    lengthDump = len(errorBase)
                    test = test + lengthDump

    result = createJSONtable(errorDict, errorG4Dict, name, jobID, prodID, wmsID)
    return S_OK(result)


################################################

#   # Due to issues in the mapping of the ES DB, this mapping
#   (which is more clear than the one below) couldn't be used.
#   # I have still saved the function here.

# def createJSONtable(dictTotal, name):

#   ids = {}
#   ids['JobID'] = JOB_ID
#   ids['ProductionID'] = PROD_ID
#   ids['TransformationID'] = TRANS_ID

#   errors = []

#   with open(name, 'w') as output:
#     for error in dictTotal:
#       newrow = {}
#       for key, value in error.items():
#         newrow['Error type'] = key
#         newrow['Counter'] = len(value)
#         newrow['Events'] = value
#       errors.append(newrow)

#     errorDict = {'Errors' : errors}

#     result = {}
#     result['ID'] = ids
#     result['Errors'] = errors
#     result = {'Log_output' : result}

#     output.write(json.dumps(result, indent = 2))
#   return

# \


def createJSONtable(errorDict, errorG4Dict, name, jobID, prodID, wmsID):
    """Creates a JSON file out of the collection of errors listed in dictTotal.

    :param dict dictTotal: the dictionary of errors
    :param str name: the name of the JSON file
    :param str jobID: the JobID of the log
    :param str prodID: the ProductionID of the log
    :param str wmsID: the wmsID of the log
    """

    result = {}
    result["JobID"] = jobID
    result["ProductionID"] = prodID
    result["wmsID"] = wmsID
    result["timestamp"] = int(TimeUtilities.toEpochMilliSeconds())
    for k, v in errorDict.items():
        result[k] = v
    for (k, v) in errorG4Dict.items():
        result[k] = v
    with open(name, "w") as output:
        json.dump(result, output, indent=2)
    return result


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


# This is a relic from the previous version of the file, I'll keep it for the while

# global LOG_STRING
# global STRING_FILE
# global FILE_OK

# LOG_FILE = sys.argv[1]
# PROJECT = sys.argv[2]
# VERSION = sys.argv[3]

# global JOB_ID
# global PROD_ID
# global TRANS_ID

# JOB_ID = sys.argv[4]
# PROD_ID = sys.argv[5]
# TRANS_ID = sys.argv[6]

# #LOG_STRING = ''
# #FILE_OK = ''
# dictG4Errors = dict()
# dictG4ErrorsCount = dict()
# STRING_FILE = pickStringFile(PROJECT, VERSION)

# if STRING_FILE is not None:
#   if os.stat(STRING_FILE)[6] != 0:
#     main(LOG_FILE)
#   else:
#     print 'WARNING: STRINGFILE %s is empty' % STRING_FILE

# The file is run as follows:
# readLogFile('Example.log', 'project', 'version', 'jobID', 'prodID', 'wmsID')
