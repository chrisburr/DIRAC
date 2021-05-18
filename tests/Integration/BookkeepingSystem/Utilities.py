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


import sys

from LHCbDIRAC.BookkeepingSystem.DB.OracleBookkeepingDB import OracleBookkeepingDB

bkDB = OracleBookkeepingDB()


def wipeOutDB():
  """ (carefully) wipe out the content of the DB
  """

  if bkDB.dbHost in ['int12r', 'LHCB_DIRACBOOKKEEPING', 'LHCBR']:
    print("STOOOOOP")
    print("Why are you trying to run against %s?" % bkDB.dbHost)
    sys.exit(1)

  bkDB.dbW_.query("DELETE FROM newrunquality")
  bkDB.dbW_.query("DELETE FROM productionoutputfiles")
  bkDB.dbW_.query("DELETE FROM inputfiles")
  bkDB.dbW_.query("DELETE FROM stepscontainer")
  bkDB.dbW_.query("DELETE FROM runstatus")
  bkDB.dbW_.query("DELETE FROM dataquality")
  bkDB.dbW_.query("DELETE FROM filetypes")
  bkDB.dbW_.query("DELETE FROM files")
  bkDB.dbW_.query("DELETE FROM eventtypes")
  bkDB.dbW_.query("DELETE FROM jobs")
  bkDB.dbW_.query("DELETE FROM steps")
  bkDB.dbW_.query("DELETE FROM productionscontainer")
  bkDB.dbW_.query("DELETE FROM processing")
  bkDB.dbW_.query("DELETE FROM simulationconditions")
  bkDB.dbW_.query("DELETE FROM configurations")
  bkDB.dbW_.query("DELETE FROM data_taking_conditions")
  bkDB.dbW_.query("DELETE FROM newrunquality")

  # still needed?
  bkDB.dbW_.query("DELETE FROM applications")
  bkDB.dbW_.query("DELETE FROM prodrunview")
  bkDB.dbW_.query("DELETE FROM runtimeprojects")
  bkDB.dbW_.query("DELETE FROM stepstmp")
  bkDB.dbW_.query("DELETE FROM tags")


def addBasicData():
  bkDB.dbW_.query("INSERT INTO dataquality VALUES(1, 'OK')")
  bkDB.addProcessing(['Real Data'])
