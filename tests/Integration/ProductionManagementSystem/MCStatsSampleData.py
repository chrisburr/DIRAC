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


# #################################################################################
# Gauss errors

gauss_errors_1 = {
    "wmsID": "5",
    "ProductionID": "4",
    "JobID": "3",
    "G4Exception": 10,
    "The signal decay mode is not defined in the main DECAY.DEC table": 2,
    "G4Exception : StuckTrack": 3,
    "G4Exception : 001": 3,
}

# #################################################################################
# Boole errors

boole_errors_1 = {"wmsID": "5", "ProductionID": "4", "JobID": "3", "ERROR": 4, "WARNING": 23}

# #################################################################################
# XML Summaries

application_summary_1 = {
    "wmsID": "5",
    "ProductionID": "4",
    "JobID": "3",
    "ITHitMonitor": {"betaGamma": 224730238, "DeltaRay": 4208, "numberHits": 86436},
    "MCITHitPacker": {"PackedData": 86436},
    "CheckITHits/Diff.": {
        "Energy": 0,
        "Parent |P|": 9,
        "TOF": 0,
        "Displacement": {"y": 0, "x": 0, "z": 0},
        "Entry Point": {"y": 0, "x": 0, "z": 0},
    },
}

application_summary_2 = {
    "wmsID": "5",
    "ProductionID": "4",
    "JobID": "3",
    "ITHitMonitor": {"betaGamma": 46211550, "DeltaRay": 295, "numberHits": 11010},
    "MCITHitPacker": {"PackedData": 11010},
    "UnpackOTHits": {"UnPackedData": 58307},
    "MCTTHitPacker": {"PackedData": 17105},
    "MCRichHitPacker": {"PackedData": 200230},
    "CheckVeloHits/Diff.": {
        "Entry Point": {"y": 0, "x": 0, "z": 0},
        "Energy": 0,
        "TOF": 0,
        "Parent |P|": -1,
        "Displacement": {"y": 0, "x": 0, "z": 0},
    },
    "UnpackRichTracks": {"UnPackedData": 7238},
    "SignalSim.SignalSimMemory": {"Total Memory/MB": 180762, "Delta Memory/MB": 69},
    "CounterSummarySvc": {"handled": 201},
    "MCRichOpPhotPacker": {"PackedData": 200226},
    "CheckRichOpPhot/Diff.": {
        "Energy": 0,
        "Sec. Mirr.": {"y": 0, "x": 0, "z": 0},
        "HPD In. Point": {"y": 0, "x": 0, "z": 0},
        "Parent Momentum": {"y": 46, "x": 38, "z": -33},
        "Prim. Mirr.": {"y": 0, "x": 0, "z": 0},
        "Cherenkov": {"Theta": 0, "Phi": 0},
        "Emission Point": {"y": 0, "x": 0, "z": 0},
        "HPD QW Point": {"y": 0, "x": 0, "z": 0},
    },
    "UnpackPrsHits": {"UnPackedData": 147436},
    "UnpackPuVetoHits": {"UnPackedData": 2500},
    "MCSpdHitPacker": {"PackedData": 70857},
    "UnpackITHits": {"UnPackedData": 11010},
    "UnpackRichHits": {"UnPackedData": 200230},
    "CheckRichSegments/Diff.": {"Traj. Momenta": {"y": 3, "x": 1, "z": -8}, "Traj. Point": {"y": 0, "x": 0, "z": 0}},
    "UnpackEcalHits": {"UnPackedData": 180320},
    "MCRichTrackPacker": {"PackedData": 7238},
    "MCPrsHitPacker": {"PackedData": 147436},
    "Generation.SignalPlain.TightCut": {
        "accept_events": 1,
        "selected marked": 581,
        "Efficiency for  [pi+]cc": 133,
        "no cuts found for gamma": 98,
        "Efficiency for  [D0]cc": 1,
        "no cuts found for pi0": 49,
        "accept_particles": 1,
    },
    "UnpackHcalHits": {"UnPackedData": 25571},
    "UnpackVeloHits": {"UnPackedData": 50809},
    "UnpackRichOpPhot": {"UnPackedData": 200226},
    "UnpackMuonHits": {"UnPackedData": 18277},
    "MCOTHitPacker": {"PackedData": 58307},
    "UnpackTTHits": {"UnPackedData": 17105},
    "OTHitMonitor": {"betaGamma": 86214332, "DeltaRay": 6062, "numberHits": 58307},
    "GenerationSignal.SignalPlain.TightCut": {
        "accept_events": 100,
        "selected marked": 3626,
        "Efficiency for  [pi+]cc": 1547,
        "no cuts found for gamma": 1036,
        "Efficiency for  [D0]cc": 100,
        "no cuts found for pi0": 518,
        "accept_particles": 100,
    },
    "MCHcalHitPacker": {"PackedData": 25571},
    "MCEcalHitPacker": {"PackedData": 180320},
    "MCRichSegmentPacker": {"PackedData": 8931},
    "MCPuVetoHitPacker": {"PackedData": 2500},
    "GaussGen.GaussGenMemory": {"Total Memory/MB": 180549, "Delta Memory/MB": 69},
    "CheckMuonHits/Diff.": {
        "Entry Point": {"y": 0, "x": 0, "z": 0},
        "Energy": 0,
        "TOF": 0,
        "Parent |P|": 1,
        "Displacement": {"y": 0, "x": 0, "z": 0},
    },
    "SignalGen.SignalGenMemory": {"Total Memory/MB": 180762, "Delta Memory/MB": 69},
    "MainEventGaussSim.MainEventGaussSimMemory": {"Total Memory/MB": 180631, "Delta Memory/MB": 69},
    "UnpackRichSegments": {"UnPackedData": 8931},
    "MCMuonHitPacker": {"PackedData": 18277},
    "TTHitMonitor": {"betaGamma": 28101829, "DeltaRay": 1249, "numberHits": 17105},
    "MCVeloHitPacker": {"PackedData": 50809},
    "UnpackSpdHits": {"UnPackedData": 70857},
}


# application_summary_3 = {
#     "Counters": {
#         "ID": {
#             "prod_job_id": "000000001",
#             "ProductionID": "8",
#             "JobID": id3
#         },
#         "ITHitMonitor": {
#             "betaGamma": 224730238,
#             "DeltaRay": 4208,
#             "numberHits": 86436
#         },
#         "MCITHitPacker": {
#             "PackedData": 86436
#         },
#         "CheckITHits/Diff.": {
#             "Energy": 0,
#             "Parent |P|": 9,
#             "TOF": 0,
#             "Displacement": {
#                 "y": 0,
#                 "x": 0,
#                 "z": 0
#             },
#             "Entry Point": {
#                 "y": 0,
#                 "x": 0,
#                 "z": 0
#             }
#         }
#     }
# }


# #################################################################################
# PrMon

prMon_1 = {
    "wmsID": "5",
    "ProductionID": "4",
    "JobID": "3",
    "Avg": {
        "nprocs": 1.999,
        "nthreads": 4.995,
        "pss": 899358.0,
        "rchar": 1908.0,
        "read_bytes": 15371.0,
        "rss": 914666.0,
        "rx_bytes": 1199655.0,
        "rx_packets": 864.068,
        "swap": 0.0,
        "tx_bytes": 887762.0,
        "tx_packets": 669.966,
        "vmem": 1742437.0,
        "wchar": 8861.0,
        "write_bytes": 8862.0,
    },
    "Max": {
        "nprocs": 2,
        "nthreads": 5,
        "pss": 946200,
        "rchar": 114342317,
        "read_bytes": 921133056,
        "rss": 958424,
        "rx_bytes": 71887798947,
        "rx_packets": 51778138,
        "stime": 120,
        "swap": 0,
        "tx_bytes": 53197995525,
        "tx_packets": 40146840,
        "utime": 60292,
        "vmem": 1793628,
        "wchar": 531004162,
        "write_bytes": 531066880,
        "wtime": 59923,
    },
    "HW": {
        "cpu": {
            "CPUs": 48,
            "CoresPerSocket": 12,
            "ModelName": "Intel(R) Xeon(R) CPU E5-2670 v3 @ 2.30GHz",
            "Sockets": 2,
            "ThreadsPerCore": 2,
        },
        "mem": {"MemTotal": 98639096},
    },
}
