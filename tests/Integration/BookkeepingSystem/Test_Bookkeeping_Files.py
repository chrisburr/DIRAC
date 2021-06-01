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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=invalid-name,wrong-import-position


import datetime

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from .Utilities import wipeOutDB, addBasicData

# sut
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient


__RCSID__ = "$Id$"


#############################################################################
# Test data

runnb = '1122'
# 5 fake files
files = ['/lhcb/data/2016/RAW/Test/test/%s/000%s_test_%d.raw' % (runnb,
                                                                 runnb, i) for i in range(5)]

# Construction of an XML Job report
# (this should be similar to what comes from online)
xmlJob = """<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE Job SYSTEM "book.dtd">
<Job ConfigName="Test" ConfigVersion="Test01" Date="%jDate%" Time="%jTime%">
  <TypedParameter Name="Production" Value="%runnb%" Type="Info"/>
  <TypedParameter Name="Name" Value="%runnb%" Type="Info"/>
  <TypedParameter Name="Location" Value="LHCb Online" Type="Info"/>
  <TypedParameter Name="ProgramName" Value="Moore" Type="Info"/>
  <TypedParameter Name="ProgramVersion" Value="v0r111" Type="Info"/>
  <TypedParameter Name="NumberOfEvents" Value="500321" Type="Info"/>
  <TypedParameter Name="ExecTime" Value="90000.0" Type="Info"/>
  <TypedParameter Name="JobStart" Value="%jStart%" Type="Info"/>
  <TypedParameter Name="JobEnd" Value="%jEnd%" Type="Info"/>
  <TypedParameter Name="FirstEventNumber" Value="29" Type="Info"/>
  <TypedParameter Name="RunNumber" Value="%runnb%" Type="Info"/>
  <TypedParameter Name="FillNumber" Value="29" Type="Info"/>
  <TypedParameter Name="JobType" Value="Merge" Type="Info"/>
  <TypedParameter Name="TotalLuminosity" Value="121222.33" Type="Info"/>
  <TypedParameter Name="Tck" Value="-2137784319" Type="Info"/>
  <TypedParameter Name="HLT2Tck" Type="Info" Value="0xaa10c"/>
  <TypedParameter Name="CondDB" Value="xy" Type="Info"/>
  <TypedParameter Name="DDDB" Value="xyz" Type="Info"/>
"""

xmlFile = """
<Quality Group="Production Manager" Flag="Not Checked"/>
  <OutputFile Name="%filename%" TypeName="RAW" TypeVersion="MDF">
   <Parameter Name="MD5Sum" Value="24F71879BA006B91FB8ADC529ACB7CC6"/>
   <Parameter Name="EventTypeId" Value="30000000"/>
   <Parameter Name="EventStat" Value="9000"/>
   <Parameter Name="FileSize" Value="1640316586"/>
   <Parameter Name="Guid" Value="3cc1b6fe-63c8-11dd-852f-00188b8565aa"/>
   <Parameter Name="FullStat" Value="429"/>
   <Parameter Name="CreationDate" Value="%fileCreation%"/>
   <Parameter Name="Luminosity" Value="1212.233"/>
 </OutputFile>
 """

dqCond = """
  <DataTakingConditions>
  <Parameter Name="Description" Value="Real Data"/>
  <Parameter Name="BeamCond" Value="Collisions"/>
  <Parameter Name="BeamEnergy" Value="450.0"/>
  <Parameter Name="MagneticField" Value="Down"/>
  <Parameter Name="VELO" Value="INCLUDED"/>
  <Parameter Name="IT" Value="string"/>
  <Parameter Name="TT" Value="string"/>
  <Parameter Name="OT" Value=""/>
  <Parameter Name="RICH1" Value="string"/>
  <Parameter Name="RICH2" Value="string"/>
  <Parameter Name="SPD_PRS" Value="string"/>
  <Parameter Name="ECAL" Value="string"/>
  <Parameter Name="HCAL" Value="string"/>
  <Parameter Name="MUON" Value="string"/>
  <Parameter Name="L0" Value="string"/>
  <Parameter Name="HLT" Value="string"/>
  <Parameter Name="VeloPosition" Value="Open"/>
</DataTakingConditions>
</Job>"""


#############################################################################

# What's used for the tests
bk = BookkeepingClient()

# # first delete from DB
wipeOutDB()

# # then add some needed data
addBasicData()

#############################################################################


def test_ping():
  """make sure we are able to contact the bkk service"
  """

  res = bk.ping()
  assert res['OK']


def test_sendXMLBookkeepingReport():
  """
  Send online XML report
  """

  res = bk.insertFileTypes('RAW', 'Boole output, RAW buffer', 'MDF')
  assert res['OK']

  res = bk.insertEventType(30000000, 'This is 30000000', 'something Lambda X (blah)')
  assert res['OK']

  res = bk.insertEventType(30000000, 'This is 30000000', 'something Lambda X (blah)')
  assert res['OK']

  res = bk.setRunAndProcessingPassDataQuality(1122, '/Real Data', 'OK')
  assert res['OK']

  currentTime = datetime.datetime.now()
  jobXML = xmlJob.replace("%jDate%", currentTime.strftime('%Y-%m-%d'))
  jobXML = jobXML.replace("%jTime%", currentTime.strftime('%H:%M'))
  jobXML = jobXML.replace("%runnb%", runnb)
  jobXML = jobXML.replace("%jStart%", currentTime.strftime('%Y-%m-%d %H:%M'))
  jobXML = jobXML.replace("%jEnd%", currentTime.strftime('%Y-%m-%d %H:%M'))
  xmlReport = jobXML
  for f in files:
    xmlReport += xmlFile.replace("%filename%", f).replace('%fileCreation%',
                                                          currentTime.strftime('%Y-%m-%d %H:%M'))

  xmlReport += dqCond
  res = bk.sendXMLBookkeepingReport(xmlReport)
  assert res['OK']


def test_getRunInformation():
  """
  Test the run metadata
  """
  retVal = bk.getRunInformation({'RunNumber': runnb})
  assert retVal['OK'] is True
  assert runnb not in retVal['Value']
  assert sorted(retVal['Value'][int(runnb)]) == sorted(['ConfigName',
                                                        'JobEnd',
                                                        'ConditionDescription',
                                                        'ProcessingPass',
                                                        'FillNumber',
                                                        'DDDB',
                                                        'JobStart',
                                                        'TCK',
                                                        'CONDDB',
                                                        'ConfigVersion'])
  result = dict(retVal['Value'][int(runnb)])
  result.pop('JobStart')
  result.pop('JobEnd')
  assert result == {'ConfigName': 'Test',
                    'ConditionDescription': 'Beam450GeV-MagDown',
                    'ProcessingPass': '/Real Data',
                    'FillNumber': 29,
                    'DDDB': 'xyz',
                    'TCK': '-0x7f6bffff',
                    'CONDDB': 'xy',
                    'ConfigVersion': 'Test01'}


def test_getListOfFills():
  retVal = bk.getListOfFills({'ConfigName': 'Test', 'ConfigVersion': 'Test01'})
  assert retVal['OK'] is True
  assert retVal['Value'] == [29]


def test_getRunsForFill():
  retVal = bk.getRunsForFill(29)
  assert retVal['OK'] is True
  assert retVal['Value'] == [1122]


def test_getRunInformations():
  retVal = bk.getRunInformations(1123)
  assert retVal['OK'] is False

  res = bk.addReplica('test')
  assert res['OK'] is False

  res = bk.addReplica(files)
  assert res['OK'] is True
  assert res['Value']['Failed'] == []
  assert res['Value']['Successful'] == files

  retVal = bk.getRunInformations(1122)
  assert retVal['OK'] is True
  assert retVal['Value']['Configuration Name'] == 'Test'
  assert retVal['Value']['Configuration Version'] == 'Test01'
  assert retVal['Value']['DataTakingDescription'] == 'Beam450GeV-MagDown'
  assert retVal['Value']['File size'] == [8201582930]
  assert retVal['Value']['FillNumber'] == 29
  assert retVal['Value']['FullStat'] == [2145]
  assert retVal['Value']['InstLuminosity'] == [0]
  assert retVal['Value']['Number of events'] == [45000]
  assert retVal['Value']['Number of file'] == [5]
  assert retVal['Value']['ProcessingPass'] == '/Real Data'
  assert retVal['Value']['Stream'] == [30000000]
  assert retVal['Value']['Tck'] == '-0x7f6bffff'
  assert retVal['Value']['TotalLuminosity'] == 121222.33
  assert retVal['Value']['luminosity'] == [6061.165]


def test_getRunFiles():
  retVal = bk.getRunFiles(1122)
  assert retVal['OK'] is True
  assert len(retVal['Value']) == 5

  files = ['/lhcb/data/2016/RAW/Test/test/1122/0001122_test_1.raw',
           '/lhcb/data/2016/RAW/Test/test/1122/0001122_test_0.raw',
           '/lhcb/data/2016/RAW/Test/test/1122/0001122_test_4.raw',
           '/lhcb/data/2016/RAW/Test/test/1122/0001122_test_3.raw',
           '/lhcb/data/2016/RAW/Test/test/1122/0001122_test_2.raw']

  runMeta = ['FullStat',
             'Luminosity',
             'FileSize',
             'EventStat',
             'GotReplica',
             'GUID',
             'InstLuminosity']
  for rec in retVal['Value']:
    assert rec in files
    assert sorted(retVal['Value'][rec]) == sorted(runMeta)


def test_getRunNbAndTck():
  retVal = bk.getRunNbAndTck('/lhcb/data/2016/RAW/Test/test/1122/0001122_test_1.raw')
  assert retVal['OK'] is True
  assert retVal['Value'] == [(1122, '-0x7f6bffff')]


def test_getRunFilesDataQuality():
  retVal = bk.getRunFilesDataQuality(1122)
  assert retVal['OK'] is True
  assert retVal['Value'] == [(1122, 'OK', 30000000)]


def test_getNbOfRawFiles():
  retVal = bk.getNbOfRawFiles({'RunNumber': 1122})
  assert retVal['OK'] is True
  assert retVal['Value'] == 5


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
