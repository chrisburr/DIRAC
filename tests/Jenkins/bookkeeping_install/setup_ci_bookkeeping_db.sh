#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo "Setting up bookkeeping database"
echo "@${ROOT_DIR}/enable_email.sql" | sqlplus sys/bkdbpass@//localhost:1521/BKDBPDB as sysdba
cd "${ROOT_DIR}/../../.."
echo "@${ROOT_DIR}/create_schema_and_procedures.sql" | sqlplus system/bkdbpass@//localhost:1521/BKDBPDB
