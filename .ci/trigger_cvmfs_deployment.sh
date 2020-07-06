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
# Enable unofficial bash strict mode
# http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail
IFS=$'\n\t'

function show_help_and_error() {
    echo "ERROR: ${1}"
    echo ""
    echo "Usage: ${0} (deploy|set-prod) LHCB_DIRAC_SETUP LHCB_DIRAC_VERSION"
    echo "Examples:"
    echo "    ${0} deploy LHCb-Certification v10r0-pre1"
    echo "    ${0} set-prod LHCb-Certification v10r0-pre1"
    echo ""
    echo "The LBTASKWEB_DEPLOY_KEY environment variable must be set."
    exit 1
}

TASK_COMMAND=${1:-}
LHCB_DIRAC_SETUP=${2:-}
LHCB_DIRAC_VERSION=${3:-}

if [[ "${TASK_COMMAND}" != "deploy" ]] && [[ "${TASK_COMMAND}" != "set-prod" ]]; then
    show_help_and_error "Deployment must be \"deploy\" or \"set-prod\""
elif [[ -z "${LHCB_DIRAC_SETUP}" ]]; then
    show_help_and_error "LHCB_DIRAC_SETUP argument not found"
elif [[ -z "${LHCB_DIRAC_VERSION}" ]]; then
    show_help_and_error "LHCB_DIRAC_VERSION argument not found"
elif [[ -z "${LBTASKWEB_DEPLOY_KEY:-}" ]]; then
    show_help_and_error "LBTASKWEB_DEPLOY_KEY environment variable is not set"
fi

# Submit deployment request to lbtaskweb
TASK_UUID=$(curl --silent -L -X POST -H "Authorization: Bearer ${LBTASKWEB_DEPLOY_KEY}" "https://lhcb-core-tasks.web.cern.ch/hooks/lhcbdirac/${LHCB_DIRAC_SETUP}/${TASK_COMMAND}/${LHCB_DIRAC_VERSION}/" | cut -d '"' -f 2)
echo "Successfully sent task with ID ${TASK_UUID}"

# Poll until the task completes
while true; do
    CURRENT_STATUS=$(curl --silent -L "https://lhcb-core-tasks.web.cern.ch/tasks/status/${TASK_UUID}" | cut -d '"' -f 2);
    echo "$(date -u): Status is ${CURRENT_STATUS}";
    if [[ "${CURRENT_STATUS}" == "SUCCESS" ]]; then
        exit 0;
    elif [[ "${CURRENT_STATUS}" == "FAILURE" ]]; then
        exit 1;
    else
        sleep 30;
    fi
done
