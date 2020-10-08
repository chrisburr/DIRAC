#!/bin/sh
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

#-------------------------------------------------------------------------------
# A convenient way to run all the LHCbDIRAC integration tests for servers
#
# It supposes that LHCbDIRAC is installed in $SERVERINSTALLDIR
#-------------------------------------------------------------------------------


echo -e '****************************************'
echo -e '********** LHCb server tests ***********\n'

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** LHCb Bookkeeping TESTS ****\n"
pytest --ignore=. "$SERVERINSTALLDIR/LHCbDIRAC/tests/Integration/BookkeepingSystem/Test_Bookkeeping_DB_StepsAndProds.py" |& tee -a "$SERVER_TEST_OUTPUT"; (( ERR |= "${?}" ))
pytest "$SERVERINSTALLDIR/LHCbDIRAC/tests/Integration/BookkeepingSystem/Test_Bookkeeping_Files.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
pytest "$SERVERINSTALLDIR/LHCbDIRAC/tests/Integration/BookkeepingSystem/Test_Bookkeeping_MCProds.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** LHCb Accounting TESTS ****\n"
python "$SERVERINSTALLDIR/LHCbDIRAC/tests/Integration/AccountingSystem/Test_Plotter.py" |& tee -a "$SERVER_TEST_OUTPUT"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** LHCb DMS TESTS ****\n"
python "$SERVERINSTALLDIR/LHCbDIRAC/tests/Integration/DataManagementSystem/Test_RAWIntegrity.py" |& tee -a "$SERVER_TEST_OUTPUT"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** LHCb PMS TESTS ****\n"
pytest "$SERVERINSTALLDIR/LHCbDIRAC/tests/Integration/ProductionManagementSystem/Test_MCStatsElasticDB.py" |& tee -a "$SERVER_TEST_OUTPUT"; (( ERR |= "${?}" ))
TEST_CODE_LOC=$TESTCODE python "$SERVERINSTALLDIR/LHCbDIRAC/tests/Integration/ProductionXMLLogAnalysis/Test_XMLSummaryAnalysis.py" |& tee -a "$SERVER_TEST_OUTPUT"; (( ERR |= "${?}" ))
