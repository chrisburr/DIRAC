DOCKERFILE FOR INTERACTING WITH PRODUCTION ENVIRONMENT OF LHCb
==============================================================


Build this with::
   
   docker build -t lhcbdirac:vXrY .

Then run it with::

   docker run -it -v /local/dir/to/user/certificate/pem/files:/home/dirac/.globus lhcbdirac:vXrY bash

This will install vXrY (see inside Dockerfile) and connect to LHCb-Certification setup by default.


TODO::

- use arguments to build and start https://stackoverflow.com/questions/34254200/how-to-pass-arguments-to-a-dockerfile
- make a wrapper around build + run
- publish "to the people"
