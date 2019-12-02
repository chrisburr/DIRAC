.. _bashrc_variables:

==================================================
Environment Variables to Configure DIRAC Behaviour
==================================================

There is a small number of environment variables that can be set to control the behaviour of some DIRAC
components. These variables can either be set in the ``bashrc`` file of a client or server installation or set manually
when desired.

DIRAC_DEBUG_DENCODE_CALLSTACK
  If set, debug information for the encoding and decoding will be printed out

DIRAC_DEBUG_STOMP
  If set, the stomp library will print out debug information 

DIRAC_DEPRECATED_FAIL
  If set, the use of functions or objects that are marked ``@deprecated`` will fail. Useful for example in continuous
  integration tests against future versions of DIRAC

DIRAC_GFAL_GRIDFTP_SESSION_REUSE
  If set to ``true`` or ``yes`` the GRIDFT SESSION RESUSE option will be set to True, should be set on server
  installations. See the information in the :ref:`resourcesStorageElement` page.

DIRAC_USE_M2CRYPTO
  If ``true`` or ``yes`` DIRAC uses m2crypto instead of pyGSI for handling certificates, proxies, etc.

DIRAC_VOMSES
  Can be set to point to a folder containing VOMSES information. See :ref:`multi_vo_dirac`

DIRAC_USE_NEWTHREADPOOL
  If this environment is set to ``true`` or ``yes``, the concurrent.futures.ThreadPoolExecutor will be used.
