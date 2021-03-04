#!/usr/bin/env python
########################################################################
# File :    dirac-wms-job-parameters
# Author :  Stuart Paterson
########################################################################
"""
  Retrieve parameters associated to the given DIRAC job
"""

from __future__ import print_function

__RCSID__ = "$Id$"

import DIRAC

from DIRAC import gLogger, S_OK
from DIRAC.Core.Base import Script

from LHCbDIRAC.DataManagementSystem.Client.DMScript import printDMResult

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ... JobID ...' % Script.scriptName,
                                  'Arguments:',
                                  '  JobID:    DIRAC Job ID']))
Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()

if len(args) < 1:
  Script.showHelp(exitCode=1)

from DIRAC.Interfaces.API.Dirac import Dirac, parseArguments
dirac = Dirac()
exitCode = 0

results = {'Successful': {}, 'Failed': {}}
success = results['Successful']
failed = results['Failed']
for job in parseArguments(args):

  result = dirac.getJobParameters(job, printOutput=False)
  if not result['OK']:
    failed[job] = result['Message']
    exitCode = 2
  else:
    success[job] = result['Value']

printDMResult(S_OK(results))

DIRAC.exit(exitCode)
