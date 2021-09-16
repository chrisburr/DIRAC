""" Test for WMS clients
"""
# pylint: disable=protected-access, missing-docstring, invalid-name, line-too-long

from __future__ import print_function
import os
import unittest
import importlib
import StringIO

from mock import MagicMock

from DIRAC.WorkloadManagementSystem.Client.Matcher import Matcher
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient


class ClientsTestCase(unittest.TestCase):
    """Base class for the clients test cases"""

    def setUp(self):

        from DIRAC import gLogger

        gLogger.setLevel("DEBUG")

        self.pilotAgentsDBMock = MagicMock()
        self.jobDBMock = MagicMock()
        self.tqDBMock = MagicMock()
        self.jlDBMock = MagicMock()
        self.opsHelperMock = MagicMock()
        self.matcher = Matcher(
            pilotAgentsDB=self.pilotAgentsDBMock,
            jobDB=self.jobDBMock,
            tqDB=self.tqDBMock,
            jlDB=self.jlDBMock,
            opsHelper=self.opsHelperMock,
        )

    def tearDown(self):
        try:
            os.remove("1.txt")
            os.remove("InputData_*")
        except OSError:
            pass


#############################################################################


class MatcherTestCase(ClientsTestCase):
    def test__processResourceDescription(self):

        resourceDescription = {
            "Architecture": "x86_64-slc6",
            "CEQueue": "jenkins-queue_not_important",
            "CPUNormalizationFactor": "9.5",
            "CPUScalingFactor": "9.5",
            "CPUTime": 1080000,
            "CPUTimeLeft": 5000,
            "DIRACVersion": "v8r0p1",
            "FileCatalog": "LcgFileCatalogCombined",
            "GridCE": "jenkins.cern.ch",
            "GridMiddleware": "DIRAC",
            "LocalSE": ["CERN-SWTEST"],
            "MaxTotalJobs": 100,
            "MaxWaitingJobs": 10,
            "OutputURL": "gsiftp://localhost",
            "PilotBenchmark": 9.5,
            "PilotReference": "somePilotReference",
            "Platform": "x86_64-slc6",
            "ReleaseProject": "LHCb",
            "ReleaseVersion": "v8r0p1",
            "Setup": "LHCb-Certification",
            "Site": "DIRAC.Jenkins.ch",
            "WaitingToRunningRatio": 0.05,
        }

        res = self.matcher._processResourceDescription(resourceDescription)
        resExpected = {
            "Setup": "LHCb-Certification",
            "ReleaseVersion": "v8r0p1",
            "CPUTime": 1080000,
            "DIRACVersion": "v8r0p1",
            "PilotReference": "somePilotReference",
            "PilotBenchmark": 9.5,
            "ReleaseProject": "LHCb",
            "Platform": "x86_64-slc6",
            "Site": "DIRAC.Jenkins.ch",
            "GridCE": "jenkins.cern.ch",
            "GridMiddleware": "DIRAC",
        }

        self.assertEqual(res, resExpected)


#############################################################################


class SandboxStoreTestCaseSuccess(ClientsTestCase):
    def test_uploadFilesAsSandbox(self):

        ourSSC = importlib.import_module(
            "DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient"
        )
        ourSSC.TransferClient = MagicMock()
        ssc = SandboxStoreClient()
        fileList = [StringIO.StringIO("try")]
        res = ssc.uploadFilesAsSandbox(fileList)
        print(res)


#############################################################################
# Test Suite run
#############################################################################

if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ClientsTestCase)
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MatcherTestCase))
    suite.addTest(
        unittest.defaultTestLoader.loadTestsFromTestCase(SandboxStoreTestCaseSuccess)
    )
    testResult = unittest.TextTestRunner(verbosity=2).run(suite)

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
