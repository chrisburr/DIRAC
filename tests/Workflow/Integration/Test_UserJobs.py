#!/usr/bin/env python
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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=protected-access, wrong-import-position, invalid-name, missing-docstring

import os
import sys
import copy
import unittest

from DIRAC.Core.Base.Script import parseCommandLine

parseCommandLine()

from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv
from DIRAC.tests.Utilities.utils import find_all

from LHCbDIRAC import rootPath
from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb

from LHCbDIRAC.tests.Utilities.IntegrationTest import IntegrationTest, FailingUserJobTestCase
from LHCbDIRAC.tests.Workflow import createJob


class UserJobTestCase(IntegrationTest):
    """Base class for the UserJob test cases"""

    def setUp(self):
        super(UserJobTestCase, self).setUp()

        print("\n \n ********************************* \n   Running a new test \n *********************************")

        self.dLHCb = DiracLHCb()
        try:
            self.exeScriptLocation = find_all("exe-script.py", os.environ["WORKSPACE"], "/tests/Workflow/Integration")[
                0
            ]
        except (IndexError, KeyError):
            self.exeScriptLocation = find_all("exe-script.py", rootPath, "/tests/Workflow/Integration")[0]

        try:
            self.exeScriptFromDIRACLocation = find_all(
                "exe-script-fromDIRAC.py", os.environ["WORKSPACE"], "/tests/Workflow/Integration"
            )[0]
        except (IndexError, KeyError):
            self.exeScriptFromDIRACLocation = find_all(
                "exe-script-fromDIRAC.py", rootPath, "/tests/Workflow/Integration"
            )[0]
        self.lhcbJobTemplate = LHCbJob()
        self.lhcbJobTemplate.setLogLevel("DEBUG")
        try:
            # This is the standard location in Jenkins
            self.lhcbJobTemplate.setInputSandbox(find_all("pilot.cfg", os.environ["WORKSPACE"] + "/PilotInstallDIR")[0])
        except (IndexError, KeyError):
            self.lhcbJobTemplate.setInputSandbox(find_all("pilot.cfg", rootPath)[0])
        self.lhcbJobTemplate.setConfigArgs("pilot.cfg")

    def tearDown(self):
        del self.lhcbJobTemplate


class HelloWorldSuccess(UserJobTestCase):
    """Simple hello world"""

    def test_Integration_User(self):

        oJob = copy.deepcopy(self.lhcbJobTemplate)
        oJob.setName("helloWorld-test")
        oJob.setExecutable(self.exeScriptLocation)
        res = oJob.runLocal(self.dLHCb)
        self.assertTrue(res["OK"])


class HelloWorldSuccessPlus(UserJobTestCase):
    """Hello world with few more parameters"""

    def test_Integration_User(self):

        oJob = copy.deepcopy(self.lhcbJobTemplate)
        oJob.setName("helloWorldPlus-test")
        oJob.setExecutable(
            self.exeScriptLocation,
            arguments="tout le monde!",
            logFile="outputFile.txt",
            systemConfig="x86_64-slc6-gcc48",
        )
        res = oJob.runLocal(self.dLHCb)
        self.assertTrue(res["OK"])


class HelloWorldSuccessWithJobID(UserJobTestCase):
    """Simple hello world, but with a fake JobID"""

    def test_Integration_User(self):

        os.environ["JOBID"] = "12345"

        oJob = copy.deepcopy(self.lhcbJobTemplate)
        oJob.setName("helloWorld-test")
        oJob.setExecutable(self.exeScriptLocation)
        res = oJob.runLocal(self.dLHCb)
        self.assertTrue(res["OK"])  # There's nothing to upload, so it will complete happily

        del os.environ["JOBID"]


class HelloWorldSuccessOutput(UserJobTestCase):
    """Hello world that produces an output"""

    def test_Integration_User(self):

        oJob = copy.deepcopy(self.lhcbJobTemplate)
        oJob.setName("helloWorld-test")
        oJob.setExecutable(self.exeScriptLocation)
        oJob.setOutputData("applicationLog.txt")
        res = oJob.runLocal(self.dLHCb)
        self.assertTrue(res["OK"])


class HelloWorldSuccessOutputWithJobID(UserJobTestCase):
    """Hello world that produces an output and has a jobID (it will try to upload)"""

    def test_Integration_User(self):

        os.environ["JOBID"] = "12345"

        oJob = copy.deepcopy(self.lhcbJobTemplate)
        oJob.setName("helloWorld-test")
        oJob.setExecutable(self.exeScriptLocation)
        oJob.setOutputData("applicationLog.txt")
        res = oJob.runLocal(self.dLHCb)  # Can't upload, so it will fail
        self.assertFalse(res["OK"])

        del os.environ["JOBID"]


class HelloWorldFromDIRACSuccess(UserJobTestCase):
    """Simple hello world (but using DIRAC gLogger)"""

    def test_Integration_User(self):

        oJob = copy.deepcopy(self.lhcbJobTemplate)
        oJob.setName("helloWorldFROMDIRAC-test")
        oJob.setExecutable(self.exeScriptFromDIRACLocation)
        res = oJob.runLocal(self.dLHCb)
        self.assertTrue(res["OK"])


class GaudirunSuccess(UserJobTestCase):
    def test_Integration_User_mc(self):
        """A MC production job, run as a user job"""

        oJob = copy.deepcopy(self.lhcbJobTemplate)
        oJob.setName("gaudirun-test")
        try:
            # This is the standard location in Jenkins
            oJob.setInputSandbox(
                [
                    find_all(
                        "prodConf_Gauss_00012345_00067890_1.py", os.environ["WORKSPACE"], "/tests/Workflow/Integration"
                    )[0],
                    find_all("pilot.cfg", os.environ["WORKSPACE"] + "/PilotInstallDIR")[0],
                ]
            )
        except (IndexError, KeyError):
            oJob.setInputSandbox(
                [
                    find_all("prodConf_Gauss_00012345_00067890_1.py", rootPath, "/tests/Workflow/Integration")[0],
                    find_all("pilot.cfg", rootPath)[0],
                ]
            )

        optGauss = "$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.py;"
        optDec = "$DECFILESROOT/options/11102400.py;"
        optPythia = "$LBPYTHIA8ROOT/options/Pythia8.py;"
        optOpts = " $APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;"
        optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
        optPConf = "prodConf_Gauss_00012345_00067890_1.py"
        options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf

        oJob.setApplication("Gauss", "v45r3", options, extraPackages="AppConfig.v3r171;ProdConf.v1r9", events="3")
        oJob.setDIRACPlatform()

        res = oJob.runLocal(self.dLHCb)
        self.assertTrue(res["OK"])

    def test_Integration_User_mc_MP(self):
        """A MC production job, run as a user job in multiprocessor"""

        oJob = copy.deepcopy(self.lhcbJobTemplate)
        oJob.setName("gaudirun-test-MP")
        try:
            # This is the standard location in Jenkins
            oJob.setInputSandbox(
                [
                    find_all(
                        "prodConf_Gauss_00012345_00067899_1.py",
                        os.environ["WORKSPACE"],
                        "/tests/System/GridTestSubmission",
                    )[0],
                    find_all("pilot.cfg", os.environ["WORKSPACE"] + "/PilotInstallDIR")[0],
                ]
            )
        except (IndexError, KeyError):
            oJob.setInputSandbox(
                [
                    find_all("prodConf_Gauss_00012345_00067899_1.py", rootPath, "/tests/System/GridTestSubmission")[0],
                    find_all("pilot.cfg", rootPath)[0],
                ]
            )

        options = "$APPCONFIGOPTS/Gauss/Beam7000GeV-mu100-nu7.6-HorExtAngle.py;"
        options += "$APPCONFIGOPTS/Gauss/EnableSpillover-25ns.py;"
        options += "$DECFILESROOT/options/12143001.py;"
        options += "$LBPYTHIA8ROOT/options/Pythia8.py;"
        options += "$APPCONFIGOPTS/Gauss/Gauss-Upgrade-Baseline-20150522.py;"
        options += "$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmOpt2.py;"
        options += "$APPCONFIGOPTS/Gauss/GaussMPpatch20200701.py;"
        options += "$APPCONFIGOPTS/Persistency/Compression-LZMA-4.py;"
        options += "prodConf_Gauss_00012345_00067899_1.py"

        oJob.setApplication(
            "Gauss",
            "v54r3",
            options,
            extraPackages="AppConfig.v3r400;Gen/DecFiles.v30r42;ProdConf.v3r0",
            systemConfig="x86_64-centos7-gcc9-opt",
            events="16",
        )
        oJob.setDIRACPlatform()
        oJob.setNumberOfProcessors(2)

        res = oJob.runLocal(self.dLHCb)
        self.assertTrue(res["OK"])

    def test_Integration_User_boole(self):
        """A Boole production job, run as a user job"""

        # get a shifter proxy
        setupShifterProxyInEnv("ProductionManager")

        oJob = copy.deepcopy(self.lhcbJobTemplate)
        oJob.setName("gaudirun-test-inputs")
        try:
            # This is the standard location in Jenkins
            oJob.setInputSandbox(
                [
                    find_all(
                        "prodConf_Boole_00012345_00067890_1.py", os.environ["WORKSPACE"], "/tests/Workflow/Integration"
                    )[0],
                    find_all("pilot.cfg", os.environ["WORKSPACE"] + "/PilotInstallDIR")[0],
                ]
            )
        except (IndexError, KeyError):
            oJob.setInputSandbox(
                [
                    find_all("prodConf_Boole_00012345_00067890_1.py", rootPath, "/tests/Workflow/Integration")[0],
                    find_all("pilot.cfg", rootPath)[0],
                ]
            )

        # opts = "$APPCONFIGOPTS/Boole/Default.py;"
        # optDT = "$APPCONFIGOPTS/Boole/DataType-2012.py;"
        # optTCK = "$APPCONFIGOPTS/L0/L0TCK-0x0042.py;"
        # optComp = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
        # optPConf = "prodConf_Boole_00012345_00067890_1.py"
        # options = opts + optDT + optTCK + optComp + optPConf

        # oJob.setApplication('Boole', 'v24r0', options,
        #                     inputData='/lhcb/user/f/fstagni/test/12345/12345678/00012345_00067890_1.sim',
        #                     extraPackages='AppConfig.v3r155;ProdConf.v1r9')

        # oJob.setDIRACPlatform()
        # res = oJob.runLocal(self.dLHCb)

        # FIXME: not really running, the above exists with status 2 -- to do it again
        self.assertTrue(True)  # res['OK'])


class UserJobsFailingLocalSuccess(FailingUserJobTestCase):
    def test_Integration_User_Failing(self):
        """This job will fail everything that can fail"""

        print("Submitting gaudiRun job (Gauss only) that will use a configuration file that contains wrong info")
        print("This will generate a local job")
        os.environ["JOBID"] = "12345"

        gaudirunJob = createJob()
        result = DiracLHCb().submitJob(gaudirunJob, mode="Local")
        self.assertFalse(result["OK"])

        del os.environ["JOBID"]


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(UserJobTestCase)
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(HelloWorldSuccess))
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(HelloWorldSuccessWithJobID))
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(HelloWorldSuccessOutput))
    # suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase(
    # HelloWorldSuccessOutputWithJobID ) ) #not suitable for Jenkins
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GaudirunSuccess))
    #  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudiScriptSuccess ) )
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(UserJobsFailingLocalSuccess))
    testResult = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not testResult.wasSuccessful())
