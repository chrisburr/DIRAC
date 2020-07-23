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

#!/usr/bin/env bash

declare -a commands=(
'dirac-bookkeeping-decays-path 13264001'
'dirac-bookkeeping-file-metadata /lhcb/certification/test/XDIGI/00001447/0000/00001447_00000001_1.xdigi'
'dirac-bookkeeping-file-path --Full /lhcb/certification/test/XDIGI/00001447/0000/00001447_00000001_1.xdigi'
'dirac-bookkeeping-filetypes-list'
'dirac-bookkeeping-genXMLCatalog --LFNs /lhcb/certification/test/XDIGI/00001447/0000/00001447_00000001_1.xdigi'
'more pool_xml_catalog.xml'
'dirac-bookkeeping-get-file-ancestors /lhcb/certification/test/XDIGI/00001447/0000/00001447_00000001_1.xdigi --All'
'dirac-bookkeeping-get-file-descendants /lhcb/certification/test/XDIGI/00001447/0000/00001447_00000001_1.xdigi --All'
'dirac-bookkeeping-get-file-sisters /lhcb/certification/test/XDIGI/00001447/0000/00001447_00000001_1.xdigi --All'
'dirac-bookkeeping-get-files dirac-bookkeeping-get-stats --BKQuery=/certification/test/Beam2510GeV-2015-MagDown-Nu1.5-25ns-Pythia8/Sim09c/Digi14b/24142001/XDIGI'
'dirac-bookkeeping-get-processing-passes --BKQuery=/certification/test/Beam2510GeV-2015-MagDown-Nu1.5-25ns-Pythia8/Sim09c/24142001/'
'dirac-bookkeeping-get-run-ranges --Activity=Collision12 --Fast'
'dirac-bookkeeping-get-runsWithAGivenDate 2012-04-01 2012-05-01'
'dirac-bookkeeping-get-stats --BKQuery=/certification/test/Beam2510GeV-2015-MagDown-Nu1.5-25ns-Pythia8/Sim09c/24142001/'
'dirac-bookkeeping-get-tck --Runs=113146,111181'
'dirac-bookkeeping-getdataquality-runs 113146'
'dirac-bookkeeping-job-info /lhcb/certification/test/XDIGI/00001447/0000/00001447_00000001_1.xdigi'
'dirac-bookkeeping-job-input-output 1495303'
'dirac-bookkeeping-prod4path --BKQuery=/certification/test/Beam2510GeV-2015-MagDown-Nu1.5-25ns-Pythia8/Sim09c/24142001/'
'dirac-bookkeeping-production-files 1446 ALL'
'dirac-bookkeeping-production-information 1446'
'dirac-bookkeeping-production-jobs 1446'
'dirac-bookkeeping-productions-summary --BKQuery=/certification/test/Beam2510GeV-2015-MagDown-Nu1.5-25ns-Pythia8/Sim09c/24142001/'
'dirac-bookkeeping-rejection-stats --BKQuery=/certification/test/Beam2510GeV-2015-MagDown-Nu1.5-25ns-Pythia8/Sim09c/24142001/'
'dirac-bookkeeping-run-files 81789'
'dirac-bookkeeping-run-information 81789'
'dirac-bookkeeping-simulationconditions-list'
)

for command in "${commands[@]}"
do
  echo "************************************************"
  echo " "
  echo "${command}"
  $command
  if [[ ${?} -ne 0 ]]; then
    exit ${?}
  fi
done