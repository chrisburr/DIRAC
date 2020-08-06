#!/usr/bin/env bash
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

declare -a commands=(
'test $( dirac-bookkeeping-decays-path 13264001 | wc -l ) = 10'
'test $( dirac-bookkeeping-file-metadata /lhcb/certification/test/XDIGI/00001447/0000/00001447_00000001_1.xdigi | grep 2199 | wc -l ) = 1'
'test $( dirac-bookkeeping-file-path --Full /lhcb/certification/test/XDIGI/00001447/0000/00001447_00000001_1.xdigi | wc -l ) = 11'
'test $( dirac-bookkeeping-filetypes-list | grep -cE "^(RADIATIVE.DST|XGEN|SWIMTRIGGERD02KPI.DST|BHADRON.DST|ALLSTREAMS.DST)" ) = 5'
'dirac-bookkeeping-genXMLCatalog --LFNs /lhcb/certification/test/XDIGI/00001447/0000/00001447_00000001_1.xdigi'
'more pool_xml_catalog.xml'
'test $( dirac-bookkeeping-get-file-ancestors /lhcb/certification/test/XDIGI/00001447/0000/00001447_00000001_1.xdigi --All | grep /lhcb/certification/test/SIM/00001446/0000/00001446 | wc -l ) = 5'
'dirac-bookkeeping-get-file-descendants /lhcb/certification/test/XDIGI/00001447/0000/00001447_00000001_1.xdigi --All'
'test $( dirac-bookkeeping-get-file-sisters /lhcb/certification/test/XDIGI/00001447/0000/00001447_00000001_1.xdigi --All | grep NoSister | wc -l ) = 1'
'test $( dirac-bookkeeping-get-stats --BKQuery=/certification/test/Beam2510GeV-2015-MagDown-Nu1.5-25ns-Pythia8/Sim09c/Digi14b/24142001/XDIGI | grep "Nb of Files" | cut -d ":" -f 2 ) = 130'
'dirac-bookkeeping-get-processing-passes --BKQuery=/certification/test/Beam2510GeV-2015-MagDown-Nu1.5-25ns-Pythia8/Sim09c/24142001/'
'dirac-bookkeeping-get-run-ranges --Activity=Collision12 --Fast'
'test $( dirac-bookkeeping-get-runsWithAGivenDate 2012-04-01 2012-05-01 | wc -w ) = 283'
'dirac-bookkeeping-get-stats --BKQuery=/certification/test/Beam2510GeV-2015-MagDown-Nu1.5-25ns-Pythia8/Sim09c/24142001/'
'dirac-bookkeeping-get-tck --Runs=113146,111181'
'test $( dirac-bookkeeping-getdataquality-runs 113146 | grep 113146 | wc -l ) = 6'
'test $( dirac-bookkeeping-job-info /lhcb/certification/test/XDIGI/00001447/0000/00001447_00000001_1.xdigi | wc -l ) = 25'
'dirac-bookkeeping-job-input-output 1495303'
'dirac-bookkeeping-prod4path --BKQuery=/certification/test/Beam2510GeV-2015-MagDown-Nu1.5-25ns-Pythia8/Sim09c/24142001/'
'test $( dirac-bookkeeping-production-files 1446 ALL | grep "lhcb/certification/test/" | wc -l ) = 48'
'test $( dirac-bookkeeping-production-information 1447 | grep CONDDB | cut -d : -f 2 | sed "s/ //g" ) = "sim-20161124-2015at5TeV-vc-md100"'
'dirac-bookkeeping-production-jobs 1446 | grep LCG.CERN.cern'
'test $( dirac-bookkeeping-productions-summary --BKQuery=/certification/test/Beam2510GeV-2015-MagDown-Nu1.5-25ns-Pythia8/Sim09c/24142001/ | grep Beam2510GeV-2015-MagDown-Nu1.5-25ns-Pythia81099 | wc -l ) = 5'
'dirac-bookkeeping-rejection-stats --BKQuery=/certification/test/Beam2510GeV-2015-MagDown-Nu1.5-25ns-Pythia8/Sim09c/24142001/'
'test $( dirac-bookkeeping-run-files 81789 | grep .raw | wc -l ) = 73'
'test $( dirac-bookkeeping-run-information 81789 | grep Beam3500GeV-VeloClosed-MagDown-Excl-R1-R2 | wc -l ) = 1'
'test $( dirac-bookkeeping-simulationconditions-list | grep SimId | wc -l ) = 107'
)

for command in "${commands[@]}"
do
  echo "************************************************"
  echo " "
  # Run commands in bash to allow variables to be expanded while also printing a reasonable error message
  if ! bash -c "${command}"; then
    echo "${command}" gives error
    exit 1
  fi
done
