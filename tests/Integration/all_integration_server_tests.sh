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
#-------------------------------------------------------------------------------


echo -e '****************************************'
echo -e '********** LHCb server tests ***********\n'

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo -e "THIS_DIR=${THIS_DIR}" |& tee -a "${SERVER_TEST_OUTPUT}"

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** LHCb Bookkeeping TESTS ****\n"
pytest --ignore=. "${THIS_DIR}/BookkeepingSystem/Test_Bookkeeping_DB_StepsAndProds.py" |& tee -a "$SERVER_TEST_OUTPUT"; (( ERR |= "${?}" ))
pytest "${THIS_DIR}/BookkeepingSystem/Test_Bookkeeping_Files.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
pytest "${THIS_DIR}/BookkeepingSystem/Test_Bookkeeping_MCProds.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** LHCb Accounting TESTS ****\n"
python "${THIS_DIR}/AccountingSystem/Test_Plotter.py" |& tee -a "$SERVER_TEST_OUTPUT"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** LHCb DMS TESTS ****\n"
python "${THIS_DIR}/DataManagementSystem/Test_RAWIntegrity.py" |& tee -a "$SERVER_TEST_OUTPUT"; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** LHCb PMS TESTS ****\n"
# pytest "${THIS_DIR}/ProductionManagementSystem/Test_ElasticApplicationSummaryDB.py" |& tee -a "$SERVER_TEST_OUTPUT"; (( ERR |= "${?}" ))
# pytest "${THIS_DIR}/ProductionManagementSystem/Test_ElasticGeneratorLogDB.py" |& tee -a "$SERVER_TEST_OUTPUT"; (( ERR |= "${?}" ))
pytest "${THIS_DIR}/ProductionManagementSystem/Test_ElasticMCBooleLogErrorsDB.py" |& tee -a "$SERVER_TEST_OUTPUT"; (( ERR |= "${?}" ))
pytest "${THIS_DIR}/ProductionManagementSystem/Test_ElasticMCGaussLogErrorsDB.py" |& tee -a "$SERVER_TEST_OUTPUT"; (( ERR |= "${?}" ))
pytest "${THIS_DIR}/ProductionManagementSystem/Test_ElasticPrMonDB.py" |& tee -a "$SERVER_TEST_OUTPUT"; (( ERR |= "${?}" ))
TEST_CODE_LOC=$TESTCODE python "${THIS_DIR}/ProductionXMLLogAnalysis/Test_XMLSummaryAnalysis.py" |& tee -a "$SERVER_TEST_OUTPUT"; (( ERR |= "${?}" ))
