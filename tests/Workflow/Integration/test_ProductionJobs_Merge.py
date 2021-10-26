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


def test_MCMergeSuccess(diracLHCb):
    from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
    from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest

    lfns = ["/lhcb/MC/2012/BDSTH.STRIP.DST/00051752/0000/00051752_00001263_2.bdsth.Strip.dst"]
    # From request 31139
    optionFiles = "$APPCONFIGOPTS/Merging/DVMergeDST.py;$APPCONFIGOPTS/DaVinci/DataType-2012.py;"
    optionFiles += "$APPCONFIGOPTS/Merging/WriteFSR.py;$APPCONFIGOPTS/Merging/MergeFSR.py"
    stepsInProd = [
        {
            "StepId": 129424,
            "StepName": "Stripping24NoPrescalingFlagged",
            "ApplicationName": "DaVinci",
            "ApplicationVersion": "v40r1p2",
            "ExtraPackages": "AppConfig.v3r263",
            "ProcessingPass": "Stripping24NoPrescalingFlagged",
            "Visible": "N",
            "Usable": "Yes",
            "DDDB": "dddb-20130929-1",
            "CONDDB": "sim-20130522-1-vc-md100",
            "DQTag": "",
            "OptionsFormat": "Merge",
            "OptionFiles": optionFiles,
            "mcTCK": "",
            "ExtraOptions": "",
            "isMulticore": "N",
            "SystemConfig": "",
            "fileTypesIn": ["BDSTH.STRIP.DST"],
            "fileTypesOut": ["BDSTH.STRIP.DST"],
            "visibilityFlag": [{"Visible": "Y", "FileType": "BDSTH.STRIP.DST"}],
        }
    ]

    prod = ProductionRequest()._buildProduction(
        "Merge",
        stepsInProd,
        {"BDSTH.STRIP.DST": "Tier1_MC-DST"},
        0,
        100,
        inputDataPolicy="protocol",
        inputDataList=lfns,
    )
    prod.LHCbJob.setInputSandbox("pilot.cfg")
    prod.LHCbJob.setConfigArgs("pilot.cfg")
    res = DiracProduction().launchProduction(prod, False, True, 0)
    assert res["OK"], res


def test_RootMergeSuccess(diracLHCb):
    from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
    from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest

    # From request 41740
    lfns = [
        "/lhcb/LHCb/Collision17/BRUNELHIST/00064899/0006/Brunel_00064899_00067792_1.Hist.root",
        "/lhcb/LHCb/Collision17/BRUNELHIST/00064899/0006/Brunel_00064899_00067793_1.Hist.root",
    ]

    stepsInProd = [
        {
            "StepId": 132150,
            "StepName": "Histo-merging-Reco17-6500GeV-MagUp-Full",
            "ApplicationName": "Noether",
            "ApplicationVersion": "v1r4",
            "ExtraPackages": "AppConfig.v3r337",
            "ProcessingPass": "HistoMerge02",
            "Visible": "Y",
            "Usable": "Yes",
            "DDDB": "",
            "CONDDB": "",
            "DQTag": "",
            "OptionsFormat": "",
            "OptionFiles": "$APPCONFIGOPTS/DataQuality/DQMergeRun.py",
            "mcTCK": "",
            "ExtraOptions": "",
            "isMulticore": "N",
            "SystemConfig": "",
            "fileTypesIn": ["BRUNELHIST", "DAVINCIHIST"],
            "fileTypesOut": ["HIST.ROOT"],
            "visibilityFlag": [{"Visible": "Y", "FileType": "HIST.ROOT"}],
        }
    ]

    prod = ProductionRequest()._buildProduction(
        "HistoMerge",
        stepsInProd,
        {"HIST.ROOT": "CERN-EOS-HIST"},
        0,
        100,
        inputDataPolicy="protocol",
        inputDataList=lfns,
    )
    prod.LHCbJob.setInputSandbox("pilot.cfg")
    prod.LHCbJob.setConfigArgs("pilot.cfg")
    res = DiracProduction().launchProduction(prod, False, True, 0)
    assert res["OK"], res
