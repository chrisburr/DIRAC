#! /usr/bin/env python
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
""" Sends the XML Bookkeeping Report
"""

from DIRAC.Core.Base import Script
Script.parseCommandLine()

xmlFile = Script.getPositionalArgs()[0]

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

with open(xmlFile, 'r') as fd:
  bkXML = fd.read()

res = BookkeepingClient().sendXMLBookkeepingReport(bkXML)
print(res)
