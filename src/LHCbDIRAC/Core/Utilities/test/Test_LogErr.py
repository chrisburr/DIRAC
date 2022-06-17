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

import json
import ast
import os
import pytest

# sut
from LHCbDIRAC.Core.Utilities import LogErr


def test_LogErr():
    jobID = "001"
    prodID = "100"
    wmsID = "123"
    res = LogErr.readLogFile("/testLogFile1.log", jobID, prodID, wmsID, "errorTest.json")
    assert res["OK"]
