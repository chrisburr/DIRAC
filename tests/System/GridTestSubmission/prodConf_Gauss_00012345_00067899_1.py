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
from ProdConf import ProdConf

ProdConf(
    NOfEvents=16,
    DDDBTag="dddb-20190223",
    AppVersion="v53r1",
    XMLSummaryFile="summaryGauss_00012345_00067899_1.xml",
    Application="Gauss",
    OutputFilePrefix="00012345_00067899_1",
    RunNumber=2308595,
    XMLFileCatalog="pool_xml_catalog.xml",
    FirstEventNumber=518801,
    CondDBTag="sim-20180530-vc-mu100",
    OutputFileTypes=["sim"],
)
