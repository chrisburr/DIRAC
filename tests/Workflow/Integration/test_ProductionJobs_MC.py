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


def test_Production_MCSuccess(diracLHCb):
    from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest
    from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

    options = "$APPCONFIGOPTS/Gauss/Beam6500GeV-md100-2015-nu1.6.py;"
    options += "$APPCONFIGOPTS/Gauss/EnableSpillover-25ns.py;"
    options += "$APPCONFIGOPTS/Gauss/DataType-2015.py;"
    options += "$APPCONFIGOPTS/Gauss/RICHRandomHits.py;"
    options += "$DECFILESROOT/options/28144011.py;"
    options += "$LBPYTHIA8ROOT/options/Pythia8.py;"
    options += "$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py"

    # From request 48257
    stepsInProd = [
        {
            "StepId": 133659,
            "StepName": "Sim09d",
            "ApplicationName": "Gauss",
            "ApplicationVersion": "v49r10",
            "ExtraPackages": "AppConfig.v3r359;Gen/DecFiles.v30r17",
            "ProcessingPass": "Sim09d",
            "Visible": "Y",
            "Usable": "Yes",
            "DDDB": "dddb-20170721-3",
            "CONDDB": "sim-20161124-vc-md100",
            "DQTag": "",
            "OptionsFormat": "",
            "OptionFiles": options,
            "isMulticore": "N",
            "SystemConfig": "x86_64-slc6-gcc48-opt",
            "mcTCK": "",
            "ExtraOptions": "",
            "fileTypesIn": [],
            "fileTypesOut": ["SIM"],
            "visibilityFlag": [{"Visible": "N", "FileType": "SIM"}],
        }
    ]

    # First create the production object
    prod = ProductionRequest()._buildProduction(
        prodType="MCSimulation",
        stepsInProd=stepsInProd,
        outputSE={"SIM": "Tier1_MC-DST"},
        priority=0,
        cpu=100,
        outputFileMask="SIM",
    )
    prod.LHCbJob.setInputSandbox("pilot.cfg")
    prod.LHCbJob.setConfigArgs("pilot.cfg")
    prod.setParameter("numberOfEvents", "string", 2, "Number of events to test")
    # Then launch it
    res = DiracProduction().launchProduction(prod, False, True, 0)

    assert res["OK"], res


def test_Production_MCSuccess_MP(diracLHCb):
    from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest
    from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

    # From step 139522
    options = "$APPCONFIGOPTS/Gauss/Beam7000GeV-mu100-nu7.6-HorExtAngle.py;"
    options += "$APPCONFIGOPTS/Gauss/EnableSpillover-25ns.py;"
    options += "$DECFILESROOT/options/12143001.py;"
    options += "$LBPYTHIA8ROOT/options/Pythia8.py;"
    options += "$APPCONFIGOPTS/Gauss/TuningPythia8_Sim09.py;"
    options += "$APPCONFIGOPTS/Gauss/Gauss-Upgrade-Baseline-20150522.py;"
    options += "$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;"
    options += "$APPCONFIGOPTS/Gauss/GaussMPpatch.py"

    stepsInProd = [
        {
            "StepId": 139522,
            "StepName": "Sim10Dev01",
            "ApplicationName": "Gauss",
            "ApplicationVersion": "v53r1",
            "ExtraPackages": "AppConfig.v3r389;Gen/DecFiles.v30r35",
            "ProcessingPass": "Sim10-Up02-OldP8Tuning",
            "Visible": "Y",
            "Usable": "Yes",
            "DDDB": "dddb-20190223",
            "CONDDB": "sim-20180530-vc-mu100",
            "DQTag": "",
            "OptionsFormat": "",
            "OptionFiles": options,
            "isMulticore": "Y",
            "SystemConfig": "x86_64-slc6-gcc7-opt",
            "mcTCK": "",
            "ExtraOptions": "",
            "fileTypesIn": [],
            "fileTypesOut": ["SIM"],
            "visibilityFlag": [{"Visible": "N", "FileType": "SIM"}],
        }
    ]

    # First create the production object
    prod = ProductionRequest()._buildProduction(
        prodType="MCSimulation",
        stepsInProd=stepsInProd,
        outputSE={"SIM": "Tier1_MC-DST"},
        priority=0,
        cpu=100,
        outputFileMask="SIM",
    )
    prod.LHCbJob.setInputSandbox("pilot.cfg")
    prod.LHCbJob.setConfigArgs("pilot.cfg")
    prod.setParameter("numberOfEvents", "string", 4, "Number of events to test")
    # Then launch it
    res = DiracProduction().launchProduction(prod, False, True, 0)

    assert res["OK"], res
