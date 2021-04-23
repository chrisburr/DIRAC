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
""" This submits user jobs using a second user, for which a proxy is downloaded locally
    This means that to run this test you need to have the KARMA!
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=wrong-import-position,unused-wildcard-import,wildcard-import

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger

from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb

from LHCbDIRAC.tests.Utilities.testJobDefinitions import *

gLogger.setLevel('VERBOSE')

jobsSubmittedList = []


class GridSubmissionTestCase(unittest.TestCase):
  """ Base class for the Regression test cases
  """

  def setUp(self):
    self.dirac = DiracLHCb()

    result = getProxyInfo()
    if result['Value']['group'] not in ['lhcb_admin']:
      print("GET A ADMIN GROUP")
      exit(1)

    result = ResourceStatus().getElementStatus('PIC-USER', 'StorageElement', 'WriteAccess')
    if result['Value']['PIC-USER']['WriteAccess'].lower() != 'banned':
      print("BAN PIC-USER in writing! and then restart this test")
      exit(1)

    res = DataManager().getReplicas(['/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt',
                                     '/lhcb/user/f/fstagni/test/testInputFile.txt'])
    if not res['OK']:
      print("DATAMANAGER.getRepicas failure: %s" % res['Message'])
      exit(1)
    if res['Value']['Failed']:
      print("DATAMANAGER.getRepicas failed for something: %s" % res['Value']['Failed'])
      exit(1)

    replicas = res['Value']['Successful']
    if list(replicas['/lhcb/user/f/fstagni/test/testInputFile.txt']) != ['CERN-USER', 'IN2P3-USER']:
      print("/lhcb/user/f/fstagni/test/testInputFile.txt locations are not correct")
    if list(replicas['/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt']) != ['CERN-USER']:
      print("/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt locations are not correct")

  def tearDown(self):
    pass


class LHCbsubmitSuccess(GridSubmissionTestCase):

  def test_LHCbsubmit(self):

    for uName, uGroup in [('cburr', 'lhcb_user'), ('chaen', 'lhcb_admin')]:

      res = helloWorldTestT2s(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = helloWorldTestCERN(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = helloWorldTestIN2P3(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = helloWorldTestGRIDKA(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = helloWorldTestARC(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = helloWorldTestARC(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = jobWithOutput(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = jobWithOutputAndPrepend(proxyUserName=uName, proxyUserGroup=uGroup)
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      jobWithOutputAndPrependWithUnderscore(proxyUserName=uName, proxyUserGroup=uGroup)

      res = jobWithOutputAndReplication(proxyUserName=uName, proxyUserGroup=uGroup)
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = jobWith2OutputsToBannedSE(proxyUserName=uName, proxyUserGroup=uGroup)
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = jobWithSingleInputData(proxyUserName=uName, proxyUserGroup=uGroup)
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = jobWithSingleInputDataCERN(proxyUserName=uName, proxyUserGroup=uGroup)
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = jobWithSingleInputDataRAL(proxyUserName=uName, proxyUserGroup=uGroup)
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = jobWithSingleInputDataIN2P3(proxyUserName=uName, proxyUserGroup=uGroup)
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = jobWithSingleInputDataRRCKI(proxyUserName=uName, proxyUserGroup=uGroup)
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = jobWithSingleInputDataSARA(proxyUserName=uName, proxyUserGroup=uGroup)
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = jobWithSingleInputDataPIC(proxyUserName=uName, proxyUserGroup=uGroup)
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = jobWithSingleInputDataSpreaded(
          proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = jobWithInputDataAndAncestor(proxyUserName=uName, proxyUserGroup=uGroup)
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = gaussJob(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = booleJob(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = booleJobWithConf(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = gaudiApplicationScriptJob(
          proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = wrongJob(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = gaussMPJob(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(GridSubmissionTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(LHCbsubmitSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
