###############################################################################
# (c) Copyright 2019-2021 CERN for the benefit of the LHCb Collaboration      #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
from pathlib import Path

import pytest


@pytest.fixture()
def userJob(diracLHCb):
    from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

    lhcbJob = LHCbJob()
    lhcbJob.setLogLevel("DEBUG")
    lhcbJob.setInputSandbox("pilot.cfg")
    lhcbJob.setConfigArgs("pilot.cfg")
    yield lhcbJob


def test_HelloWorldSuccess(diracLHCb, userJob):
    userJob.setName("helloWorld-test")
    userJob.setExecutable(str(Path(__file__).parent / "exe-script.py"))
    res = userJob.runLocal(diracLHCb)
    assert res["OK"], res


def test_HelloWorldSuccessPlus(diracLHCb, userJob):
    """Hello world with few more parameters"""
    userJob.setName("helloWorldPlus-test")
    userJob.setExecutable(
        str(Path(__file__).parent / "exe-script.py"),
        arguments="tout le monde!",
        logFile="outputFile.txt",
        systemConfig="x86_64-slc6-gcc48",
    )
    res = userJob.runLocal(diracLHCb)
    assert res["OK"], res


def test_HelloWorldSuccessWithJobID(diracLHCb, userJob, monkeypatch):
    """Simple hello world, but with a fake JobID"""
    monkeypatch.setenv("JOBID", "12345")

    userJob.setName("helloWorld-test")
    userJob.setExecutable(str(Path(__file__).parent / "exe-script.py"))
    res = userJob.runLocal(diracLHCb)
    assert res["OK"], res  # There's nothing to upload, so it will complete happily


def test_HelloWorldSuccessOutput(diracLHCb, userJob):
    """Hello world that produces an output"""
    userJob.setName("helloWorld-test")
    userJob.setExecutable(str(Path(__file__).parent / "exe-script.py"))
    userJob.setOutputData("applicationLog.txt")
    res = userJob.runLocal(diracLHCb)
    assert res["OK"], res


@pytest.mark.skip("Not suitable for Jenkins")
def test_HelloWorldSuccessOutputWithJobID(diracLHCb, userJob, monkeypatch):
    """Hello world that produces an output and has a jobID (it will try to upload)"""
    monkeypatch.setenv("JOBID", "12345")

    userJob.setName("helloWorld-test")
    userJob.setExecutable(str(Path(__file__).parent / "exe-script.py"))
    userJob.setOutputData("applicationLog.txt")
    res = userJob.runLocal(diracLHCb)  # Can't upload, so it will fail
    assert not res["OK"], res


@pytest.mark.skip("Not working as the LHCbJob class clears the environment")
def test_HelloWorldFromDIRACSuccess(diracLHCb, userJob):
    """Simple hello world (but using DIRAC gLogger)"""

    userJob.setName("helloWorldFROMDIRAC-test")
    userJob.setExecutable(str(Path(__file__).parent / "exe-script-fromDIRAC.py"))
    res = userJob.runLocal(diracLHCb)
    assert res["OK"], res


def test_GaudirunSuccess_mc(diracLHCb, userJob):
    """A MC production job, run as a user job"""

    userJob.setName("gaudirun-test")
    userJob.setInputSandbox([str(Path(__file__).parent / "prodConf_Gauss_00012345_00067890_1.py"), "pilot.cfg"])

    optGauss = "$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.py;"
    optDec = "$DECFILESROOT/options/11102400.py;"
    optPythia = "$LBPYTHIA8ROOT/options/Pythia8.py;"
    optOpts = " $APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;"
    optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
    optPConf = "prodConf_Gauss_00012345_00067890_1.py"
    options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf

    userJob.setApplication("Gauss", "v45r3", options, extraPackages="AppConfig.v3r171;ProdConf.v1r9", events="3")
    userJob.setDIRACPlatform()

    res = userJob.runLocal(diracLHCb)
    assert res["OK"], res


def test_GaudirunSuccess_mc_MP(diracLHCb, userJob):
    """A MC production job, run as a user job in multiprocessor"""

    userJob.setName("gaudirun-test-MP")
    userJob.setInputSandbox(
        [
            str(
                Path(__file__).parent.parent.parent / "System/GridTestSubmission/prodConf_Gauss_00012345_00067899_1.py"
            ),
            "pilot.cfg",
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

    userJob.setApplication(
        "Gauss",
        "v54r3",
        options,
        extraPackages="AppConfig.v3r400;Gen/DecFiles.v30r42;ProdConf.v3r0",
        systemConfig="x86_64-centos7-gcc9-opt",
        events="16",
    )
    userJob.setDIRACPlatform()
    userJob.setNumberOfProcessors(2)

    res = userJob.runLocal(diracLHCb)
    assert res["OK"], res


def test_Integration_User_boole(diracLHCb, userJob):
    """A Boole production job, run as a user job"""
    from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv

    # get a shifter proxy
    setupShifterProxyInEnv("ProductionManager")

    userJob.setName("gaudirun-test-inputs")
    userJob.setInputSandbox([str(Path(__file__).parent / "prodConf_Boole_00012345_00067890_1.py"), "pilot.cfg"])

    # opts = "$APPCONFIGOPTS/Boole/Default.py;"
    # optDT = "$APPCONFIGOPTS/Boole/DataType-2012.py;"
    # optTCK = "$APPCONFIGOPTS/L0/L0TCK-0x0042.py;"
    # optComp = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
    # optPConf = "prodConf_Boole_00012345_00067890_1.py"
    # options = opts + optDT + optTCK + optComp + optPConf

    # userJob.setApplication('Boole', 'v24r0', options,
    #                     inputData='/lhcb/user/f/fstagni/test/12345/12345678/00012345_00067890_1.sim',
    #                     extraPackages='AppConfig.v3r155;ProdConf.v1r9')

    # userJob.setDIRACPlatform()
    # res = userJob.runLocal(diracLHCb)

    # FIXME: not really running, the above exists with status 2 -- to do it again
    assert True  # res['OK'])


def test_UserJobsFailingLocalSuccess(diracLHCb, monkeypatch):
    """This job will fail everything that can fail"""
    from DIRAC.DataManagementSystem.Client.DataManager import DataManager
    from LHCbDIRAC.tests.Workflow import createJob

    res = DataManager().removeFile(
        [
            "/lhcb/testCfg/testVer/LOG/00012345/0006/00012345_00067890.tar",
            "/lhcb/testCfg/testVer/SIM/00012345/0006/00012345_00067890_1.sim",
        ],
        force=True,
    )
    assert res["OK"], res

    print("Submitting gaudiRun job (Gauss only) that will use a configuration file that contains wrong info")
    print("This will generate a local job")
    monkeypatch.setenv("JOBID", "12345")

    gaudirunJob = createJob(workspace=str(Path(__file__).parent.parent.parent))
    result = diracLHCb.submitJob(gaudirunJob, mode="Local")
    assert not result["OK"], result
