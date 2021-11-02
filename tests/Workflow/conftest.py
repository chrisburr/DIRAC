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
import os
import shutil
import sys

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
    import DIRAC
    from DIRAC.Core.Base.Script import parseCommandLine
    from DIRAC.Core.Security.ProxyInfo import getProxyInfo
    from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb

    proxyInfo = getProxyInfo()
    if not proxyInfo["OK"] or proxyInfo["Value"]["secondsLeft"] < 12 * 60 * 60:
        raise RuntimeError("Failed to find a proxy with at least 12 hours of validity")

    assert "pilot.cfg" not in sys.argv, "pytest must be ran with --forked"
    sys.argv = sys.argv[:1] + ["pilot.cfg", "-o", "/DIRAC/Security/UseServerCertificate=no", "-ddd"]
    parseCommandLine()

    if DIRAC.gConfig.getValue("/DIRAC/Setup") != "LHCb-Certification":
        raise RuntimeError("These tests must be ran against LHCb-Certification")

    yield DiracLHCb()


# Adds the --runslow command line arg based on the example in the docs
# https://docs.pytest.org/en/stable/example/simple.html
# #control-skipping-of-tests-according-to-command-line-option
def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)
