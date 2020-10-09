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
# A convenient way to run all the LHCbDIRAC integration tests for client -> server interaction
#
# It supposes that LHCbDIRAC is installed in $CLIENTINSTALLDIR
#-------------------------------------------------------------------------------
set -x

echo -e '****************************************'
echo -e '******' "LHCb client -> server tests" '******\n'

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** LHCb PMS TESTS ****\n"
python "$CLIENTINSTALLDIR/LHCbDIRAC/tests/Integration/ProductionManagementSystem/Test_ProductionRequest.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** LHCb TS TESTS ****\n"
python "$CLIENTINSTALLDIR/LHCbDIRAC/tests/Integration/TransformationSystem/Test_ClientTransformation.py" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))

#-------------------------------------------------------------------------------#
echo -e "*** $(date -u) **** LHCb WMS TESTS ****\n"
"$CLIENTINSTALLDIR/LHCbDIRAC/tests/Integration/WorkloadManagementSystem/Test_dirac-jobexecLHCb.sh" |& tee -a clientTestOutputs.txt; (( ERR |= "${?}" ))
