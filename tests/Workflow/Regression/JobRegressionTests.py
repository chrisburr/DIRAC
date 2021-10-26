#!/usr/bin/env python
###############################################################################
# (c) Copyright 2021 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
"""Regression tests for LHCbDIRAC productions and user jobs

Should be ran using::

    pytest tests/Workflow/Regression/JobRegressionTests.py --forked -n auto --basetemp=$PWD/test-tmp
"""
import os
import shutil
import sys
from pathlib import Path

import pytest


PILOT_CFG = """DIRAC
{
    Setup=LHCb-Certification
}
LocalSite
{
    Site = DIRAC.Jenkins.ch
    GridCE = jenkins.cern.ch
    CEQueue = jenkins-queue_not_important
    LocalSE = CERN-DST-EOS
    LocalSE += CERN-HIST-EOS
    LocalSE += CERN-RAW
    LocalSE += CERN-FREEZER-EOS
    LocalSE += CERN-SWTEST
    Architecture = x86_64-slc6
    CPUScalingFactor = 5.7
    CPUNormalizationFactor = 5.7
    SharedArea = /cvmfs/lhcb.cern.ch/lib
    CPUTimeLeft = 123456
}
"""


@pytest.fixture()
def pilotDir(tmp_path):
    prev_dir = os.getcwd()

    (tmp_path / "pilot.cfg").write_text(PILOT_CFG)

    try:
        os.chdir(tmp_path)
        yield tmp_path
    finally:
        os.chdir(prev_dir)


@pytest.fixture
def diracLHCb(pilotDir):
    from DIRAC.Core.Base.Script import parseCommandLine
    from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb

    assert "pilot.cfg" not in sys.argv, "pytest must be ran with --forked"
    sys.argv = sys.argv[:1] + ["pilot.cfg", "-o", "/DIRAC/Security/UseServerCertificate=no", "-ddd"]
    parseCommandLine()

    yield DiracLHCb()


def test_RecoStripping(diracLHCb):
    from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

    # from request 62444 - Collision17-MagDown-Reco17
    location99554 = str(Path(__file__).parent / "99554.xml")

    j_reco_99554 = LHCbJob(location99554)
    j_reco_99554.setConfigArgs("pilot.cfg")

    res = j_reco_99554.runLocal(diracLHCb)
    assert res["OK"], res


def test_MCMerge(diracLHCb):
    from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

    location51753 = str(Path(__file__).parent / "51753.xml")

    j_MCmerge_51753 = LHCbJob(location51753)
    j_MCmerge_51753.setConfigArgs("pilot.cfg")

    res = j_MCmerge_51753.runLocal(diracLHCb)
    assert res["OK"], res


def test_MergeHISTO(diracLHCb):
    from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

    location66219 = str(Path(__file__).parent / "66219.xml")

    j_mergeHISTO_66219 = LHCbJob(location66219)
    j_mergeHISTO_66219.setConfigArgs("pilot.cfg")

    res = j_mergeHISTO_66219.runLocal(diracLHCb)
    assert res["OK"], res


def test_MCReco(diracLHCb):
    from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

    location104074 = str(Path(__file__).parent / "104074.xml")

    j_mcReco = LHCbJob(location104074)
    j_mcReco.setConfigArgs("pilot.cfg")

    res = j_mcReco.runLocal(diracLHCb)
    assert res["OK"], res


def test_MCSuccess(diracLHCb):
    from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

    location40651 = str(Path(__file__).parent / "40651.xml")

    j_mc_40651 = LHCbJob(location40651)
    j_mc_40651.setConfigArgs("pilot.cfg")

    res = j_mc_40651.runLocal(diracLHCb)
    assert res["OK"], res


def test_MCSuccess2(diracLHCb):
    from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

    location123456 = str(Path(__file__).parent / "123456.xml")

    j_mc_123456 = LHCbJob(location123456)
    j_mc_123456.setConfigArgs("pilot.cfg")

    res = j_mc_123456.runLocal(diracLHCb)
    assert res["OK"], res


@pytest.fixture()
def helloWorldDir(pilotDir):
    shutil.copy(Path(__file__).parent / "exe-script.py", pilotDir)
    shutil.copy(Path(__file__).parent / "helloWorld.py", pilotDir)
    yield pilotDir


def test_UserJob_HelloWorld(diracLHCb, helloWorldDir):
    from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

    helloWorldXMLLocation = str(Path(__file__).parent / "helloWorld.xml")
    j_u_hello = LHCbJob(helloWorldXMLLocation)
    j_u_hello.setConfigArgs("pilot.cfg")

    res = j_u_hello.runLocal(diracLHCb)
    assert res["OK"], res


def test_UserJob_HelloWorldPlus(diracLHCb, helloWorldDir):
    from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

    helloWorldXMLFewMoreLocation = str(Path(__file__).parent / "helloWorld.xml")
    j_u_helloPlus = LHCbJob(helloWorldXMLFewMoreLocation)
    j_u_helloPlus.setConfigArgs("pilot.cfg")

    res = j_u_helloPlus.runLocal(diracLHCb)
    assert res["OK"], res
