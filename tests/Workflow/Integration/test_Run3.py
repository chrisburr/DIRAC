###############################################################################
# (c) Copyright 2022 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
import json

import pytest


@pytest.mark.slow
def test_Run3Experimental(diracLHCb):
    from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest
    from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

    lfns = ["/lhcb/user/c/cburr/spruce_all_lines_realtimereco_newPacking.dst"]

    # HACK This is to work around the lack of TCK infrastructure in the Run 3 stack
    # https://gitlab.cern.ch/lhcb/LHCb/-/issues/194
    annsvc_config = "root://eoslhcb.cern.ch//eos/lhcb/wg/dpa/wp3/tests/spruce_all_lines_realtime_newPacking.tck.json"
    options = json.dumps(
        {
            "entrypoint": "DaVinciExamples.tupling.AllFunctors:alg_config",
            "extra_options": {
                "annsvc_config": annsvc_config,
                "data_type": "Upgrade",
                "input_type": "ROOT",
                "simulation": True,
                "conddb_tag": "sim-20171127-vc-md100",
                "dddb_tag": "dddb-20171126",
                "input_raw_format": 0.3,
                "lumi": False,
                "print_freq": 1,
                "process": "Spruce",
                "stream": "default",
            },
            "extra_args": [],
        }
    )

    stepsInProd = [
        {
            "StepId": 133659,
            "StepName": "Sim09d",
            "ApplicationName": "DaVinci",
            "ApplicationVersion": "v60r3",
            "ExtraPackages": "",
            "ProcessingPass": "Sim09d",
            "Visible": "Y",
            "Usable": "Yes",
            "DDDB": "dddb-20171126",
            "CONDDB": "sim-20171127-vc-md100",
            "DQTag": "",
            "OptionsFormat": "",
            "OptionFiles": options,
            "isMulticore": "N",
            "SystemConfig": "",
            "mcTCK": "",
            "ExtraOptions": "",
            "fileTypesIn": [],
            "fileTypesOut": ["DVNTUPLE.ROOT"],
            "visibilityFlag": [{"Visible": "N", "FileType": "DVNTUPLE.ROOT"}],
        }
    ]

    # First create the production object
    prod = ProductionRequest()._buildProduction(
        prodType="WGProduction",
        stepsInProd=stepsInProd,
        outputSE={"DVNTUPLE.ROOT": "Tier1_MC-DST"},
        priority=0,
        cpu=100,
        inputDataList=lfns,
        outputFileMask="DVNTUPLE.ROOT",
    )
    prod.LHCbJob.setInputSandbox("pilot.cfg")
    prod.LHCbJob.setConfigArgs("pilot.cfg")
    # Then launch it
    res = DiracProduction().launchProduction(prod, False, True, 0)

    assert res["OK"], res
