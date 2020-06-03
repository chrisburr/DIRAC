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
"""
It tests the insert of XML Summaries to the BookkeepingDB.
"""

# pylint: disable=invalid-name,wrong-import-position

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient


__RCSID__ = "$Id$"


#############################################################################
# Test data

runnb = '1122'
# 5 fake files
files = ['/lhcb/data/2016/RAW/Test/test/%s/000%s_test_%d.raw' % (runnb,
								 runnb, i) for i in xrange(5)]


#############################################################################
# What's used for the tests
bk = BookkeepingClient()


#############################################################################
# Actual tests


def test_addFiles():
  """
  add replica flag
  """
  retVal = bk.addFiles(files)
  assert retVal['OK'] is True
  assert retVal['Value']['Failed'] == []
  assert retVal['Value']['Successful'] == files

  retVal = bk.addFiles('test.txt')
  assert retVal['OK'] is True
  assert retVal['Value']['Successful'] == []
  assert retVal['Value']['Failed'] == ['test.txt']

  bk.updateProductionOutputfiles()
  assert retVal['OK'] is True


def test_fileMetadata():
  """
  test the file metadata method
  """
  fileParams = ['GUID', 'ADLER32', 'FullStat', 'EventType', 'FileType',
		'MD5SUM', 'VisibilityFlag', 'InsertTimeStamp', 'RunNumber',
		'JobId', 'Luminosity', 'FileSize', 'EventStat', 'GotReplica',
		'CreationDate', 'InstLuminosity', 'DataqualityFlag']
  retVal = bk.getFileMetadata(files)

  assert retVal['OK'] is True
  assert retVal['Value']['Failed'] == []
  assert len(retVal['Value']['Successful']) == len(files)
  assert sorted(retVal['Value']['Successful']) == sorted(files)
  # make sure the files has all parameters
  for fName in retVal['Value']['Successful']:
    assert sorted(retVal['Value']['Successful'][fName]) == sorted(fileParams)

  retVal = bk.getFileMetadata('test.txt')
  assert retVal['OK'] is True
  assert retVal['Value']['Successful'] == {}
  assert retVal['Value']['Failed'] == ['test.txt']


def test_getRunFiles():
  """
  retrieve all the files for a given run
  """
  fileParams = ['FullStat', 'Luminosity', 'FileSize', 'EventStat', 'GotReplica', 'GUID', 'InstLuminosity']
  retVal = bk.getRunFiles(int(runnb))
  assert retVal['OK'] is True
  assert sorted(retVal['Value']) == sorted(files)
  for fName in retVal['Value']:
    assert sorted(retVal['Value'][fName]) == sorted(fileParams)


def test_getAvailableFileTypes():
  """
  retrieve the file types
  """

  retVal = bk.getAvailableFileTypes()
  assert retVal['OK'] is True
  assert len(retVal['Value']) > 0


def test_removeFiles():
  """
  Set the replica flag to no
  """

  retVal = bk.removeFiles(files)
  assert retVal['OK'] is True
  assert retVal['Value']['Failed'] == []
  assert retVal['Value']['Successful'] == files

  retVal = bk.removeFiles('test.txt')
  assert retVal['OK'] is True
  assert retVal['Value']['Successful'] == []
  assert retVal['Value']['Failed'] == ['test.txt']
