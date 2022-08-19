#!/usr/bin/env bash
###############################################################################
# (c) Copyright 2021 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################

# This is meant to run the DIRAC StorageElement system tests on gitlab

# Use bash strict mode
# http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail
IFS=$'\n\t'

for envVariable in LHCBDIRAC_CERTIF_HOSTCERT LHCBDIRAC_CERTIF_HOSTKEY SE_config; do
  if [[ -z "${!envVariable:-}" ]];
  then
    echo -e "\033[1m\033[31mError ${envVariable} variable is not defined!!!\033[0m";
    echo -e 'You need a valid host certificate/key to interact with the LHCbDIRAC certification instance,';
    echo -e 'as well as a dirac.cfg content which contains StorageElement definitions'
    echo -e "If running in Gitlab, you can then manually copy to a masked variable to your fork's configuration using the instructions at:";
    echo -e ' > https://docs.gitlab.com/ee/ci/variables/#via-the-ui';
    exit 67;
  fi;
done

mkdir /tmp/work
cd /tmp/work
mkdir /tmp/home
export HOME=/tmp/home

export DIRACSETUP=LHCb-Certification
export DIRACUSERDN="/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=chaen/CN=705305/CN=Christophe Haen"
export DIRACUSERROLE=lhcb_user

export DIRAC_test_repo=${DIRAC_test_repo-'DIRACGrid'}
export DIRAC_test_branch=${DIRAC_test_branch-'integration'}
export LHCbDIRAC_test_repo=${LHCbDIRAC_test_repo-'lhcb-dirac'}
export LHCbDIRAC_test_branch=${LHCbDIRAC_test_branch-'devel'}
export CSURL=${CSURL-'dips://lhcb-cert-conf-dirac.cern.ch:9135/Configuration/Server'}

export WORKSPACE=$PWD
mkdir TestCode
cd TestCode
git clone "https://github.com/${DIRAC_test_repo}/DIRAC.git" -b "${DIRAC_test_branch}"
# We fetch to get all the tags, such that the pip install can find the correct
# version number
# -> if you use your private repo, you need to have all your tags up to date !
cd DIRAC
git fetch --all
cd ..
git clone "https://gitlab.cern.ch/${LHCbDIRAC_test_repo}/LHCbDIRAC.git" -b "${LHCbDIRAC_test_branch}"
cd LHCbDIRAC
git fetch --all
cd ..
cd ..

# Get the certificates
mkdir -p "/tmp/home/certs"
echo "${LHCBDIRAC_CERTIF_HOSTCERT}" | base64 -d > "/tmp/home/certs/hostcert.pem"
echo "${LHCBDIRAC_CERTIF_HOSTKEY}" | base64 -d > "/tmp/home/certs/hostkey.pem"
chmod 400 "/tmp/home/certs/hostkey.pem"

curl -LO https://github.com/DIRACGrid/DIRACOS2/releases/latest/download/DIRACOS-Linux-x86_64.sh
bash DIRACOS-Linux-x86_64.sh
source diracos/diracosrc

# Install DIRAC and LHCbDIRAC
pip install "git+https://github.com/${DIRAC_test_repo}/DIRAC.git@${DIRAC_test_branch}"
pip install "git+https://gitlab.cern.ch/${LHCbDIRAC_test_repo}/LHCbDIRAC.git@${LHCbDIRAC_test_branch}"

dirac-configure -S ${DIRACSETUP} -C ${CSURL} -U --SkipCAChecks -o "/DIRAC/Security/UseServerCertificate=Yes" -o "/DIRAC/Security/CertFile=/tmp/home/certs/hostcert.pem" -o "/DIRAC/Security/KeyFile=/tmp/home/certs/hostkey.pem" -ddd

dirac-admin-get-proxy -a --out /tmp/x509up_u$(id -u) -o /DIRAC/Security/UseServerCertificate=True -ddd "$DIRACUSERDN" $DIRACUSERROLE

# Write the SE configuration
echo "$SE_config" >> diracos/etc/seconfig.cfg
export DIRACSYSCONFIG=diracos/etc/seconfig.cfg
# Loop through each SE and run the test
for se in $(diraccfg as-json diracos/etc/seconfig.cfg | python -c 'import json,sys;obj=json.load(sys.stdin);print(" ".join(list(obj["Resources"]["StorageElements"].keys())))');
do
  echo $se;
  pytest -s TestCode/DIRAC/tests/System/StorageTests/Test_Resources_StorageElement.py --seName=${se};
done
