#!/bin/bash
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
# lhcb_ci
#
# Collection of utilities and functions for automated tests (using Jenkins)
#
# Requires dirac_ci.sh where most generic utility functions are
#-------------------------------------------------------------------------------


# Exit on error. If something goes wrong, we terminate execution
#TODO: fix
#set -o errexit

# first first: sourcing dirac_ci file # the location from where this script is sourced is critical
source TestCode/DIRAC/tests/Jenkins/dirac_ci.sh

#install file
readonly INSTALL_CFG_FILE="${TESTCODE}/LHCbDIRAC/tests/Jenkins/install.cfg"


#.............................................................................
#
# findRelease for LHCbDIRAC:
#
#   Legacy hack to prevent DIRAC's CI scripts from looking for a releases.cfg file
#
#.............................................................................

findRelease(){
  echo '[findRelease]'

  if [[ ! -n "${LHCBDIRAC_RELEASE}" ]]; then
    echo '==> LHCBDIRAC_RELEASE not set but required'
    exit 1
  fi
  projectVersion="${LHCBDIRAC_RELEASE}"

  # TODO: This should be made to fail to due set -u and -o pipefail
  if [[ ! "${projectVersion}" ]]; then
    echo "Failed to set projectVersion" >&2
    exit 1
  fi
}



#-------------------------------------------------------------------------------
# diracServices:
#
#   specialized, for fixing BKK DB
#
#-------------------------------------------------------------------------------

diracServices(){
  echo '==> [diracServices]'

  services=$(cat services | cut -d '.' -f 1 | grep -v Tornado | grep -Ev '(PilotsLogging|FTSManagerHandler|StorageElementHandler|^ConfigurationSystem|Plotting|RAWIntegrity|RunDBInterface|ComponentMonitoring|WMSSecureGW)' | sed -e 's/System / /g' -e 's/Handler//g' -e 's/ /\//g')

  for serv in ${services}
  do

    if [[ "${serv}" = 'Bookkeeping/BookkeepingManager' ]]; then
      setupBKKDB

      if [[ -z "${DIRACOSVER}" ]]; then
        export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:/afs/cern.ch/project/oracle/amd64_linux26/prod/lib/"
        pip install cx_Oracle==7.3
      fi
    fi

    echo "==> calling dirac-install-component ${serv} $DEBUG"
    dirac-install-component "${serv}" "$DEBUG"
  done

}


#-------------------------------------------------------------------------------
# diracAgents:
#
#   specialized, just adding some more agents to exclude
#
#-------------------------------------------------------------------------------

diracAgents(){
  echo '==> [diracAgents]'

  agents=$(cat agents | cut -d '.' -f 1 | grep -Ev '(FTSAgent|CleanFTSDBAgent|MyProxy|CAUpdate|GOCDB2CS|Bdii2CS|StatesMonitoringAgent|DataProcessingProgressAgent|RAWIntegrityAgent|Nagios|AncestorFiles|BKInputData|LHCbPRProxyAgent|StorageUsageAgent|PopularityAnalysisAgent|SEUsageAgent|NotifyAgent|ShiftDBAgent|VirtualMachineMonitorAgent)' | sed 's/System / /g' | sed 's/ /\//g')

  for agent in $agents; do
    if [[ $agent = *'JobAgent'* ]]; then
      echo '==> '
    else
      echo "==> calling dirac-cfg-add-option agent $agent"
      python "${TESTCODE}/DIRAC/tests/Jenkins/dirac-cfg-add-option.py" "agent" "$agent"
      echo "==> calling dirac-agent $agent -o MaxCycles=1 ${DEBUG}"
      if ! dirac-agent "$agent"  -o MaxCycles=1 "${DEBUG}"; then
        echo 'ERROR: dirac-agent failed' >&2
        exit 1
      fi
    fi
  done
}

#-------------------------------------------------------------------------------
# Here is where the real functions start
#-------------------------------------------------------------------------------

sourcingEnv() {

  echo -e "==> Sourcing the environment"
  source "${PILOTINSTALLDIR}/environmentLHCbDirac"
}

setupBKKDB() {
  echo -e "==> Setting up the Bookkeeping Database"
  if [[ -n "${ORACLEDB_PASSWORD}" ]]; then
    "${TESTCODE}/LHCbDIRAC/tests/Jenkins/dirac-bkk-cfg-update.py" -p "${ORACLEDB_PASSWORD}" "${DEBUG}"
  else
    "${TESTCODE}/LHCbDIRAC/tests/Jenkins/dirac-bkk-cfg-update.py" "${DEBUG}" \
      --password "bkdbpass" \
      --host "bkdb:1521/bkdbpdb" \
      --read-user "system" \
      --write-user "system"
  fi
}

#EOF
