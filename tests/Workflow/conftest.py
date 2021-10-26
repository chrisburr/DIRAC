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
    from DIRAC.Core.Base.Script import parseCommandLine
    from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb

    assert "pilot.cfg" not in sys.argv, "pytest must be ran with --forked"
    sys.argv = sys.argv[:1] + ["pilot.cfg", "-o", "/DIRAC/Security/UseServerCertificate=no", "-ddd"]
    parseCommandLine()

    yield DiracLHCb()
