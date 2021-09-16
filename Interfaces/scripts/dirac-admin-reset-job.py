#!/usr/bin/env python
########################################################################
# File :    dirac-admin-reset-job
# Author :  Stuart Paterson
########################################################################
"""
Reset a job or list of jobs in the WMS

Usage:

  dirac-admin-reset-job [option|cfgfile] ... JobID ...

Arguments:

  JobID:    DIRAC ID of the Job

Example:

  $ dirac-admin-reset-job 1848
  Reset Job 1848
"""
from __future__ import print_function

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage(__doc__)
Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()

if len(args) < 1:
    Script.showHelp()

from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

diracAdmin = DiracAdmin()
exitCode = 0
errorList = []

for job in args:

    try:
        job = int(job)
    except Exception as x:
        errorList.append(("Expected integer for jobID", job))
        exitCode = 2
        continue

    result = diracAdmin.resetJob(job)
    if result["OK"]:
        print("Reset Job %s" % (job))
    else:
        errorList.append((job, result["Message"]))
        exitCode = 2

for error in errorList:
    print("ERROR %s: %s" % error)

DIRAC.exit(exitCode)
