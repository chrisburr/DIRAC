DOCKERFILE FOR CREATING A CLIENT FRO INTERACTING WITH PRODUCTION ENVIRONMENT OF LHCb
====================================================================================

Build this with::

   docker build -t lhcbdirac:vXrY .

Then run it with::

   docker run -it -v /local/dir/to/user/certificate/pem/files:/home/dirac/.globus lhcbdirac:vXrY bash

This will install a client of LHCbDIRAC, version vXrY (see inside Dockerfile).
