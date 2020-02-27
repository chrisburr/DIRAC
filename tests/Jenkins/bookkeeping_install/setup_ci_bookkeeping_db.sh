#!/usr/bin/env bash
###############################################################################
# (c) Copyright 2020 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo "Setting up bookkeeping database"
echo "@${ROOT_DIR}/enable_email.sql" | sqlplus sys/bkdbpass@//localhost:1521/BKDBPDB as sysdba
cd "${ROOT_DIR}/../../.."
echo "@${ROOT_DIR}/create_schema_and_procedures.sql" | sqlplus system/bkdbpass@//localhost:1521/BKDBPDB
