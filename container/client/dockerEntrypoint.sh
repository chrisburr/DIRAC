#!/bin/bash

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
