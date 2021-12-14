###############################################################################
# (c) Copyright 2021 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
from datetime import datetime, timedelta
import sys
import random

import pytest

from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise

# Generate a random integer so this test can be run multiple times
RANINT = random.randint(100_000, 999_999)
ANALYSIS_1 = f"{RANINT}Analysis1"
ANALYSIS_2 = f"{RANINT}Analysis2"
WG = f"{RANINT}WG"
REQUEST_1 = {
    "request_id": RANINT * 10,
    "name": "MySample",
    "version": "v1r2p3",
    "wg": WG,
    "analysis": ANALYSIS_1,
    "extra_info": {
        "transformations": [],
        "merge_request": "https://gitlab.cern.ch/lhcb-datapkg/AnalysisProductions/-/merge_requests/0",
        "jira_task": "https://its.cern.ch/jira/browse/WGP-0",
    },
    "validity_start": datetime.now() - timedelta(days=1),
    "owners": [{"username": "johndoe"}],
    "auto_tags": [
        {"name": "config", "value": "MC"},
        {"name": "polarity", "value": "MagDown"},
        {"name": "eventtype", "value": "23133002"},
        {"name": "datatype", "value": "2012"},
        {"name": f"{RANINT}autotag", "value": "test"},
    ],
}


@pytest.fixture
def apc():
    from DIRAC.Core.Base.Script import parseCommandLine

    argv, sys.argv = sys.argv, sys.argv[:1]
    parseCommandLine()
    sys.argv = argv + ["-ddd"]
    from LHCbDIRAC.ProductionManagementSystem.Client.AnalysisProductionsClient import AnalysisProductionsClient

    yield AnalysisProductionsClient()


def test_returnTypes(apc):
    assert isinstance(returnValueOrRaise(apc.listAnalyses()), dict)
    assert isinstance(returnValueOrRaise(apc.getProductions()), list)
    assert isinstance(returnValueOrRaise(apc.getArchivedRequests()), list)
    assert isinstance(returnValueOrRaise(apc.getTags()), dict)
    assert isinstance(returnValueOrRaise(apc.getKnownAutoTags()), list)
    apc.registerTransformations
    apc.registerSamples


def test_basic(apc):
    # Ensure there are currently now known productions for this wg+analysis combo
    assert WG not in returnValueOrRaise(apc.listAnalyses())
    assert returnValueOrRaise(apc.getProductions(wg=WG, analysis=ANALYSIS_1)) == []
    assert returnValueOrRaise(apc.getTags(wg=WG, analysis=ANALYSIS_1)) == {}
    assert f"{RANINT}autotag" not in returnValueOrRaise(apc.getKnownAutoTags())
    assert REQUEST_1["request_id"] not in {r["request_id"] for r in returnValueOrRaise(apc.getArchivedRequests())}
    # Insert a production and check it's registered correctly
    returnValueOrRaise(apc.registerRequests([REQUEST_1]))
    samples = returnValueOrRaise(apc.getProductions(wg=WG, analysis=ANALYSIS_1))
    sampleID = samples[0]["sample_id"]
    requestID = samples[0]["request_id"]
    assert len(samples) == 1
    knownAutoTags = returnValueOrRaise(apc.getKnownAutoTags())
    assert not {"config", "polarity", "eventtype", "datatype", f"{RANINT}autotag"} - set(knownAutoTags)
    # Test moving to active
    returnValueOrRaise(apc.setState({requestID: {"state": "active", "progress": 0.1}}))
    sample = returnValueOrRaise(apc.getProductions(wg=WG, analysis=ANALYSIS_1))[0]
    assert sample["state"] == "active"
    assert sample["progress"] == 0.1
    # Test moving to replicating
    returnValueOrRaise(apc.setState({requestID: {"state": "replicating", "progress": 0.6}}))
    sample = returnValueOrRaise(apc.getProductions(wg=WG, analysis=ANALYSIS_1))[0]
    assert sample["state"] == "replicating"
    assert sample["progress"] == 0.6
    # Test moving to ready
    returnValueOrRaise(apc.setState({requestID: {"state": "ready", "progress": None}}))
    sample = returnValueOrRaise(apc.getProductions(wg=WG, analysis=ANALYSIS_1))[0]
    assert sample["state"] == "ready"
    assert "progress" not in sample
    # Archive the production
    returnValueOrRaise(apc.archiveSamples([sampleID]))
    # Ensure the samples were actually archived
    samples = returnValueOrRaise(apc.getProductions(wg=WG, analysis=ANALYSIS_1))
    assert len(samples) == 0
    # Automatic tags should still be registered
    assert f"{RANINT}autotag" in returnValueOrRaise(apc.getKnownAutoTags())
