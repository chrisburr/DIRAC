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
"""Unittest for:
LHCbDIRAC.ProductionManagementSystem.Utilities.XMLtoJSON."""

import unittest
from LHCbDIRAC.Core.Utilities.XMLSummaries import XMLSummary


def test_xmltojson():
  XML1 = XMLSummary('summaryGauss_00105318_00000003_1.xml')
  XML2 = XMLSummary('summaryGauss_00105318_00000001_1.xml')

  JSON1 = XML1.xmltojson()
  JSON2 = XML2.xmltojson()

  assert JSON1 == {
      "Counters": {
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
              "UnPackedData": 70857}}}
  assert JSON2 == {
      "Counters": {
          "ITHitMonitor": {
              "betaGamma": 224730238, "DeltaRay": 4208, "numberHits": 86436}, "MCITHitPacker": {
              "PackedData": 86436}, "CheckITHits/Diff.": {
              "Entry Point": {
                  "y": 0, "x": 0, "z": 0}, "Energy": 0, "TOF": 0, "Parent |P|": 9, "Displacement": {
                  "y": 0, "x": 0, "z": 0}}, "UnpackOTHits": {
              "UnPackedData": 253748}, "MCTTHitPacker": {
                  "PackedData": 73624}, "MCRichHitPacker": {
              "PackedData": 775606}, "Generation.SignalPlain.TightCut": {
                      "accept_events": 1, "selected marked": 203, "Efficiency for  [pi+]cc": 47, "no cuts found for gamma": 38, "Efficiency for  [D0]cc": 1, "no cuts found for pi0": 19, "accept_particles": 1}, "UnpackRichTracks": {  # nopep8
              "UnPackedData": 32641}, "SignalSim.SignalSimMemory": {
                          "Total Memory/MB": 177999, "Delta Memory/MB": 69}, "CounterSummarySvc": {
              "handled": 201}, "MCRichOpPhotPacker": {
                              "PackedData": 775504}, "CheckRichOpPhot/Diff.": {
              "Energy": 0, "Sec. Mirr.": {
                  "y": 0, "x": 0, "z": 0}, "HPD In. Point": {
                  "y": 0, "x": 0, "z": 0}, "Parent Momentum": {
                  "y": 70, "x": -39, "z": -115}, "Prim. Mirr.": {
                  "y": 0, "x": 0, "z": 0}, "Cherenkov": {
                  "Theta": 0, "Phi": 0}, "Emission Point": {
                  "y": 0, "x": 0, "z": 0}, "HPD QW Point": {
                  "y": 0, "x": 0, "z": 0}}, "UnpackPrsHits": {
              "UnPackedData": 567900}, "UnpackPuVetoHits": {
              "UnPackedData": 9800}, "MCSpdHitPacker": {
              "PackedData": 284467}, "UnpackITHits": {
              "UnPackedData": 86436}, "MCPrsHitPacker": {
              "PackedData": 567900}, "UnpackMuonHits": {
              "UnPackedData": 78779}, "UnpackEcalHits": {
              "UnPackedData": 734334}, "MainEventGaussSim.MainEventGaussSimMemory": {
              "Total Memory/MB": 177815, "Delta Memory/MB": 69}, "MCRichTrackPacker": {
              "PackedData": 32641}, "UnpackRichHits": {
              "UnPackedData": 775606}, "CheckVeloHits/Diff.": {
              "Entry Point": {
                  "y": 0, "x": 0, "z": 0}, "Energy": 0, "TOF": 0, "Parent |P|": 3, "Displacement": {
                  "y": 0, "x": 0, "z": 0}}, "UnpackVeloHits": {
              "UnPackedData": 244708}, "UnpackRichOpPhot": {
              "UnPackedData": 775504}, "CheckRichSegments/Diff.": {
              "Traj. Momenta": {
                  "y": -20, "x": 16, "z": -12}, "Traj. Point": {
                  "y": 0, "x": 0, "z": 0}}, "UnpackSpdHits": {
              "UnPackedData": 284467}, "CheckOTHits/Diff.": {
              "Entry Point": {
                  "y": 0, "x": 0, "z": 0}, "Energy": 0, "TOF": 0, "Parent |P|": -31, "Displacement": {
                  "y": 0, "x": 0, "z": 0}}, "MCOTHitPacker": {
              "PackedData": 253748}, "UnpackTTHits": {
              "UnPackedData": 73624}, "CheckTTHits/Diff.": {
              "Entry Point": {
                  "y": 0, "x": 0, "z": 0}, "Energy": 0, "TOF": 0, "Parent |P|": 3, "Displacement": {
                  "y": 0, "x": 0, "z": 0}}, "OTHitMonitor": {
              "betaGamma": 338754733, "DeltaRay": 16893, "numberHits": 253748}, "GenerationSignal.SignalPlain.TightCut": {  # nopep8
              "accept_events": 100, "selected marked": 1988, "Efficiency for  [pi+]cc": 405, "no cuts found for gamma": 270, "Efficiency for  [D0]cc": 100, "no cuts found for pi0": 135, "accept_particles": 100}, "CheckPuVetoHits/Diff.": {  # nopep8
              "Entry Point": {
                  "y": 0, "x": 0, "z": 0}, "Energy": 0, "TOF": 0, "Parent |P|": 3, "Displacement": {
                  "y": 0, "x": 0, "z": 0}}, "MCEcalHitPacker": {
              "PackedData": 734334}, "MCRichSegmentPacker": {
              "PackedData": 38582}, "MCPuVetoHitPacker": {
              "PackedData": 9800}, "GaussGen.GaussGenMemory": {
              "Total Memory/MB": 177734, "Delta Memory/MB": 69}, "CheckMuonHits/Diff.": {
              "Entry Point": {
                  "y": 0, "x": 0, "z": 0}, "Energy": 0, "TOF": 0, "Parent |P|": 12, "Displacement": {
                  "y": 0, "x": 0, "z": 0}}, "MCVeloHitPacker": {
              "PackedData": 244708}, "UnpackHcalHits": {
              "UnPackedData": 130737}, "UnpackRichSegments": {
              "UnPackedData": 38582}, "MCMuonHitPacker": {
              "PackedData": 78779}, "TTHitMonitor": {
              "betaGamma": 72641099, "DeltaRay": 10184, "numberHits": 73624}, "MCHcalHitPacker": {
              "PackedData": 130737}, "SignalGen.SignalGenMemory": {
              "Total Memory/MB": 177999, "Delta Memory/MB": 69}}}
