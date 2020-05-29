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

import datetime

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient


__RCSID__ = "$Id$"


bk = BookkeepingClient()
runnb = '1122'
files = ['/lhcb/data/2016/RAW/Test/test/%s/000%s_test_%d.raw' % (runnb,
								 runnb, i) for i in xrange(5)]

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
  <TypedParameter Name="JobStart"        Value="%jStart%" Type="Info"/>
  <TypedParameter Name="JobEnd"          Value="%jEnd%" Type="Info"/>
  <TypedParameter Name="FirstEventNumber"          Value="29" Type="Info"/>
  <TypedParameter Name="RunNumber"          Value="%runnb%" Type="Info"/>
  <TypedParameter Name="FillNumber"         Value="29" Type="Info"/>
  <TypedParameter Name="JobType"         Value="Merge" Type="Info"/>
  <TypedParameter Name="TotalLuminosity"         Value="121222.33" Type="Info"/>
  <TypedParameter Name="Tck"             Value="-2137784319" Type="Info"/>
  <TypedParameter Name="HLT2Tck" Type="Info" Value="0xaa10c"/>
  <TypedParameter Name="CondDB"             Value="xy" Type="Info"/>
 <TypedParameter Name="DDDB"             Value="xyz" Type="Info"/>
"""

xmlFile = """
<Quality Group="Production Manager" Flag="Not Checked"/>
  <OutputFile Name="%filename%" TypeName="RAW" TypeVersion="MDF">
   <Parameter Name="MD5Sum" Value="24F71879BA006B91FB8ADC529ACB7CC6"/>
   <Parameter Name="EventTypeId" Value="30000000"/>
   <Parameter Name="EventStat" Value="9000"/>
   <Parameter Name="FileSize" Value="1640316586"/>
   <Parameter Name="Guid" Value="3cc1b6fe-63c8-11dd-852f-00188b8565aa"/>
   <Parameter Name="FullStat"          Value="429"/>
   <Parameter Name="CreationDate"        Value="%fileCreation%"/>
   <Parameter Name="Luminosity"          Value="1212.233"/>
 </OutputFile>
 """

dqCond = """
  <DataTakingConditions>
  <Parameter Name="Description" Value="Real data"/>
  <Parameter Name="BeamCond"   Value="Collisions"/>
  <Parameter Name="BeamEnergy" Value="450.0"/>
  <Parameter Name="MagneticField" Value="Down"/>
  <Parameter Name="VELO"          Value="INCLUDED"/>
  <Parameter Name="IT"          Value="string"/>
  <Parameter Name="TT"          Value="string"/>
  <Parameter Name="OT"          Value=""/>
  <Parameter Name="RICH1"          Value="string"/>
  <Parameter Name="RICH2"          Value="string"/>
  <Parameter Name="SPD_PRS"          Value="string"/>
  <Parameter Name="ECAL"          Value="string"/>
  <Parameter Name="HCAL"          Value="string"/>
  <Parameter Name="MUON"          Value="string"/>
  <Parameter Name="L0"          Value="string"/>
  <Parameter Name="HLT"          Value="string"/>
  <Parameter Name="VeloPosition"          Value="Open"/>
</DataTakingConditions>
</Job>"""


def test_echo():
  """make sure we are able to use the bkk"
  """

  res = bk.echo("Test")
  assert res['OK']
  assert res['Value'] == "Test"


def test_sendXMLBookkeepingReport():
  """
  Send different XML reports
  """

  # insert

  # FIXME: this fails even if it's already removed (by hand)
  # need to understand why, prob related to how the stored procedure works
  res = bk.insertFileTypes('RAW', 'Boole output, RAW buffer', 'MDF')
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

  # ToDO: now remove the file type
