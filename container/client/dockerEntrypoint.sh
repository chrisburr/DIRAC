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

source /opt/dirac/bashrc
cd /home/dirac

echo "******************************************************************************************"
echo "====> LHCbDIRAC client"
echo ""
echo "You should have started this container bind-mounting a dir with your cert pem files"
echo "e.g. with"
echo "  docker run -it -v /local/dir/to/user/certificate/pem/files:/home/dirac/.globus lhcbdirac:vXrY bash"
echo ""
echo "If you have done so, create a proxy and work normally"
echo ""

exec "$@"
