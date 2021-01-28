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
#   If the environment variable "LHCBDIRAC_RELEASE" exists, and set, we use the specified release.
#   If the environment variable "LHCBDIRACBRANCH" exists, and set, we use the specified "branch", otherwise we take the last one.
#
#   It reads from releases.cfg and picks the latest version
#   which is written to {project,dirac,lhcbdirac}.version
#
#.............................................................................

findRelease(){
  echo '[findRelease]'

  # store the current branch
  currentBranch=$(git --git-dir="${TESTCODE}/LHCbDIRAC/.git" rev-parse --abbrev-ref HEAD)

  if [[ "${currentBranch}" = 'devel' ]]; then
    echo 'we were already on devel, no need to change'
    # get the releases.cfg file
    cp "${TESTCODE}/LHCbDIRAC/src/LHCbDIRAC/releases.cfg" "${TESTCODE}"/
  else
    (cd "${TESTCODE}/LHCbDIRAC"
     git remote add "ci-upstream" "https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC.git" || true
     git remote -v
     git fetch --all || true
     git show "remotes/ci-upstream/devel:LHCbDIRAC/releases.cfg" > "${TESTCODE}/releases.cfg")
  fi

  # Match project ( LHCbDIRAC ) version from releases.cfg
  # Example releases.cfg
  # v7r15-pre2
  # {
  #   Modules = LHCbDIRAC:v7r15-pre2, LHCbWebDIRAC:v3r3p5
  #   Depends = DIRAC:v6r10-pre12
  #   LcgVer = 2013-09-24
  # }

  if [[ -n "${LHCBDIRAC_RELEASE}" ]]; then
    echo '==> Specified release'
    echo "${LHCBDIRAC_RELEASE}"
    projectVersion="${LHCBDIRAC_RELEASE}"
  else
    if [[ -n "${LHCBDIRACBRANCH}" ]]; then
      echo "==> Looking for LHCBDIRAC branch ${LHCBDIRACBRANCH}"
    else
      echo '==> Running on last one'
    fi

    # If I don't specify a LHCBDIRACBRANCH, it will get the latest "production" release
    # First, try to find if we are on a production tag
    if [[ -n "${LHCBDIRACBRANCH}" ]]; then
      projectVersion=$(cat "${TESTCODE}/LHCbDIRAC/src/LHCbDIRAC/releases.cfg" | grep '[^:]v[[:digit:]]*r[[:digit:]]*p[[:digit:]]*' | grep "${LHCBDIRACBRANCH}" | head -1 | sed 's/ //g')
    else
      projectVersion=$(cat "${TESTCODE}/LHCbDIRAC/src/LHCbDIRAC/releases.cfg" | grep '[^:]v[[:digit:]]*r[[:digit:]]*p[[:digit:]]*' | head -1 | sed 's/ //g')
    fi

    # The special case is when there's no 'p'... (e.g. version v8r3)
    if [[ ! "$projectVersion" ]]; then
      if [[ -n "${LHCBDIRACBRANCH}" ]]
      then
        projectVersion=$(cat "${TESTCODE}/LHCbDIRAC/src/LHCbDIRAC/releases.cfg" | grep '[^:]v[[:digit:]]*r[[:digit:]]' | grep "${LHCBDIRACBRANCH}" | head -1 | sed 's/ //g')
      else
        projectVersion=$(cat "${TESTCODE}/LHCbDIRAC/src/LHCbDIRAC/releases.cfg" | grep '[^:]v[[:digit:]]*r[[:digit:]]' | head -1 | sed 's/ //g')
      fi
    fi

    # In case there are no production tags for the branch, look for pre-releases in that branch
    if [[ ! "$projectVersion" ]]; then
      if [[ -n "${LHCBDIRACBRANCH}" ]]; then
        projectVersion=$(cat "${TESTCODE}/LHCbDIRAC/src/LHCbDIRAC/releases.cfg" | grep '[^:]v[[:digit:]]*r[[:digit:]]*'-pre'' | grep ${LHCBDIRACBRANCH} | head -1 | sed 's/ //g')
      else
        projectVersion=$(cat "${TESTCODE}/LHCbDIRAC/src/LHCbDIRAC/releases.cfg" | grep '[^:]v[[:digit:]]*r[[:digit:]]*'-pre'' | head -1 | sed 's/ //g')
      fi
    fi

  fi

  # TODO: This should be made to fail to due set -u and -o pipefail
  if [[ ! "${projectVersion}" ]]; then
    echo "Failed to set projectVersion" >&2
    exit 1
  fi

  echo PROJECT:"${projectVersion}" && echo "${projectVersion}" > project.version

  # projectVersionLine : line number where v7r15-pre2 is
  projectVersionLine=$(cat "${TESTCODE}/LHCbDIRAC/src/LHCbDIRAC/releases.cfg" | grep -n "${projectVersion}" | cut -d ':' -f 1 | head -1)
  # start := line number after "{"
  start=$((projectVersionLine+2))
  # end   := line number after "}"
  end=$((start+2))
  # versions :=
  #   Modules = LHCbDIRAC:v7r15-pre2, LHCbWebDIRAC:v3r3p5
  #   Depends = DIRAC:v6r10-pre12
  #   LcgVer = 2013-09-24
  versions=$(sed -n "$start,$end p" "${TESTCODE}/LHCbDIRAC/src/LHCbDIRAC/releases.cfg")

  # Extract DIRAC version
  diracVersion=$(echo "$versions" | tr ' ' '\n' | grep "^DIRAC:v*[^,]" | sed 's/,//g' | cut -d ':' -f2)
  # Extract LHCbDIRAC version
  lhcbdiracVersion=$(echo "$versions" | tr ' ' '\n' | grep "^LHCbDIRAC:v*" | sed 's/,//g' | cut -d ':' -f2)

  # PrintOuts
  echo "==> DIRAC:${diracVersion}" && echo "${diracVersion}" > dirac.version
  echo "==> LHCbDIRAC:${lhcbdiracVersion}" && echo "${lhcbdiracVersion}" > lhcbdirac.version
}



#-------------------------------------------------------------------------------
# diracServices:
#
#   specialized, for fixing BKK DB
#
#-------------------------------------------------------------------------------

diracServices(){
  echo '==> [diracServices]'

  services=$(cat services | cut -d '.' -f 1 | grep -Ev '(PilotsLogging|FTSManagerHandler|StorageElementHandler|^ConfigurationSystem|Plotting|RAWIntegrity|RunDBInterface|ComponentMonitoring|WMSSecureGW)' | sed -e 's/System / /g' -e 's/Handler//g' -e 's/ /\//g')

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

  agents=$(cat agents | cut -d '.' -f 1 | grep -Ev '(FTSAgent|CleanFTSDBAgent|MyProxy|CAUpdate|GOCDB2CS|Bdii2CS|StatesMonitoringAgent|DataProcessingProgressAgent|RAWIntegrityAgent|Nagios|AncestorFiles|BKInputData|LHCbPRProxyAgent|StorageUsageAgent|PopularityAnalysisAgent|SEUsageAgent|NotifyAgent|ShiftDBAgent)' | sed 's/System / /g' | sed 's/ /\//g')

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
