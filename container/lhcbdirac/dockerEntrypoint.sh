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

# Entry point of docker to setup the DIRAC environment before executing the command

source /opt/dirac/diracosrc


if [[ -z ${DIRAC_REPO:-} ]]; then
  echo "No alternative DIRAC repository specified";
else
  echo "Using alternative DIRAC repository ${DIRAC_REPO}";
  if [[ -z ${DIRAC_BRANCH:-} ]]; then
    echo "Using default branch";
    pip install "git+${DIRAC_REPO}"
  else
    echo "Using specific branch ${DIRAC_BRANCH}";
    pip install "git+${DIRAC_REPO}@${DIRAC_BRANCH}"
  fi
fi


if [[ -z ${LHCB_DIRAC_REPO:-} ]]; then
  echo "No alternative LHCbDIRAC repository specified";
else
  echo "Using alternative LHCbDIRAC repository $LHCB_DIRAC_REPO";
  if [[  -z ${LHCB_DIRAC_BRANCH:-} ]]; then
    echo "Using default branch";
    pip install "git+${LHCB_DIRAC_REPO}"
  else
    echo "Using specific branch ${LHCB_DIRAC_BRANCH}";
    pip install "git+${LHCB_DIRAC_REPO}@${LHCB_DIRAC_BRANCH}"
  fi
fi

# Lookup in the environment for variables starting with
# DIRAC_CFG, sort them alphabeticaly, and add them to the
# command line
# Note 1: because ``sort``` works on line, we need the ``tr`` command
# Note 2: using ``IFS``` instead of ``tr`` only work from bash 5
# Note 3: the ``-V`` is to have a version sort (e.g. DIRAC_CFG_3 < DIRAC_CFG_10)

ALL_CFG_FILES=""
for cfgFile in $(sort -V <( tr -s ' ' '\n' <<< "${!DIRAC_CFG_@}" )); do
  echo "Found configuration file $cfgFile : ${!cfgFile}";
  ALL_CFG_FILES+="--cfg ${!cfgFile} ";
done

# By default, DIRAC_CMD would be dirac-service
# but it may be set to tornado-start-all

exec ${DIRAC_CMD:-dirac-service} ${ALL_CFG_FILES} -ddd $DIRAC_COMPONENT
