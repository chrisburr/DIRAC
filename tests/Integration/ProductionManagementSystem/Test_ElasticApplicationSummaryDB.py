##############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
"""
Tests set(), get() and remove() from ElasticApplicationSummaryDB
"""

import time

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from LHCbDIRAC.ProductionManagementSystem.DB.ElasticApplicationSummaryDB import ElasticApplicationSummaryDB


db = ElasticApplicationSummaryDB()
data = {
    "wmsID": "5",
    "ProductionID": "4",
    "JobID": "3",
    "ITHitMonitor": {
        "betaGamma": 224730238,
        "DeltaRay": 4208,
        "numberHits": 86436
    },
    "MCITHitPacker": {
        "PackedData": 86436
    },
    "CheckITHits/Diff.": {
        "Energy": 0,
        "Parent |P|": 9,
        "TOF": 0,
        "Displacement": {
            "y": 0,
            "x": 0,
            "z": 0
        },
        "Entry Point": {
            "y": 0,
            "x": 0,
            "z": 0
        }
    }
}

realExample = {
    "wmsID": "5",
    "ProductionID": "4",
    "JobID": "3",
    "ITHitMonitor": {
        "betaGamma": 46211550,
        "DeltaRay": 295,
        "numberHits": 11010},
    "MCITHitPacker": {
        "PackedData": 11010},
    "UnpackOTHits": {
        "UnPackedData": 58307},
    "MCTTHitPacker": {
        "PackedData": 17105},
    "MCRichHitPacker": {
        "PackedData": 200230},
    "CheckVeloHits/Diff.": {
        "Entry Point": {
            "y": 0,
            "x": 0,
            "z": 0},
        "Energy": 0,
        "TOF": 0,
        "Parent |P|": -1,
        "Displacement": {
            "y": 0,
            "x": 0,
            "z": 0}},
    "UnpackRichTracks": {
        "UnPackedData": 7238},
    "SignalSim.SignalSimMemory": {
        "Total Memory/MB": 180762,
        "Delta Memory/MB": 69},
    "CounterSummarySvc": {
        "handled": 201},
    "MCRichOpPhotPacker": {
        "PackedData": 200226},
    "CheckRichOpPhot/Diff.": {
        "Energy": 0,
        "Sec. Mirr.": {
            "y": 0,
            "x": 0,
            "z": 0},
        "HPD In. Point": {
            "y": 0,
            "x": 0,
            "z": 0},
        "Parent Momentum": {
            "y": 46,
            "x": 38,
            "z": -33},
        "Prim. Mirr.": {
            "y": 0,
            "x": 0,
            "z": 0},
        "Cherenkov": {
            "Theta": 0,
            "Phi": 0},
        "Emission Point": {
            "y": 0,
            "x": 0,
            "z": 0},
        "HPD QW Point": {
            "y": 0,
            "x": 0,
            "z": 0}},
    "UnpackPrsHits": {
        "UnPackedData": 147436},
    "UnpackPuVetoHits": {
        "UnPackedData": 2500},
    "MCSpdHitPacker": {
        "PackedData": 70857},
    "UnpackITHits": {
        "UnPackedData": 11010},
    "UnpackRichHits": {
        "UnPackedData": 200230},
    "CheckRichSegments/Diff.": {
        "Traj. Momenta": {
            "y": 3,
            "x": 1,
            "z": -8},
        "Traj. Point": {
            "y": 0,
            "x": 0,
            "z": 0}},
    "UnpackEcalHits": {
        "UnPackedData": 180320},
    "MCRichTrackPacker": {
        "PackedData": 7238},
    "MCPrsHitPacker": {
        "PackedData": 147436},
    "Generation.SignalPlain.TightCut": {
        "accept_events": 1,
        "selected marked": 581,
        "Efficiency for  [pi+]cc": 133,
        "no cuts found for gamma": 98,
        "Efficiency for  [D0]cc": 1,
        "no cuts found for pi0": 49,
        "accept_particles": 1},
    "UnpackHcalHits": {
        "UnPackedData": 25571},
    "UnpackVeloHits": {
        "UnPackedData": 50809},
    "UnpackRichOpPhot": {
        "UnPackedData": 200226},
    "UnpackMuonHits": {
        "UnPackedData": 18277},
    "MCOTHitPacker": {
        "PackedData": 58307},
    "UnpackTTHits": {
        "UnPackedData": 17105},
    "OTHitMonitor": {
        "betaGamma": 86214332,
        "DeltaRay": 6062,
        "numberHits": 58307},
    "GenerationSignal.SignalPlain.TightCut": {
        "accept_events": 100,
        "selected marked": 3626,
        "Efficiency for  [pi+]cc": 1547,
        "no cuts found for gamma": 1036,
        "Efficiency for  [D0]cc": 100,
        "no cuts found for pi0": 518,
        "accept_particles": 100},
    "MCHcalHitPacker": {
        "PackedData": 25571},
    "MCEcalHitPacker": {
        "PackedData": 180320},
    "MCRichSegmentPacker": {
        "PackedData": 8931},
    "MCPuVetoHitPacker": {
        "PackedData": 2500},
    "GaussGen.GaussGenMemory": {
        "Total Memory/MB": 180549,
        "Delta Memory/MB": 69},
    "CheckMuonHits/Diff.": {
        "Entry Point": {
            "y": 0,
            "x": 0,
            "z": 0},
        "Energy": 0,
        "TOF": 0,
        "Parent |P|": 1,
        "Displacement": {
            "y": 0,
            "x": 0,
            "z": 0}},
    "SignalGen.SignalGenMemory": {
        "Total Memory/MB": 180762,
        "Delta Memory/MB": 69},
    "MainEventGaussSim.MainEventGaussSimMemory": {
        "Total Memory/MB": 180631,
        "Delta Memory/MB": 69},
    "UnpackRichSegments": {
        "UnPackedData": 8931},
    "MCMuonHitPacker": {
        "PackedData": 18277},
    "TTHitMonitor": {
        "betaGamma": 28101829,
        "DeltaRay": 1249,
        "numberHits": 17105},
    "MCVeloHitPacker": {
        "PackedData": 50809},
    "UnpackSpdHits": {
        "UnPackedData": 70857}}
