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

import sys
import datetime

#############################################################################
# Test data

runnb_1 = 1122
runnb_2 = 1123

# 5 fake files
rawFiles_1 = ["/lhcb/data/2016/RAW/Test/test/%s/000%s_test_%d.raw" % (runnb_1, runnb_1, i) for i in range(5)]
rawFiles_2 = ["/lhcb/data/2021/RAW/Test/test/%s/000%s_test_%d.raw" % (runnb_2, runnb_2, i) for i in range(5, 11)]

# Construction of an XML Job report
# (this should be similar to what comes from online)
xmlJob_1 = """<?xml version="1.0" encoding="ISO-8859-1"?>
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

xmlJob_2 = """<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE Job SYSTEM "book.dtd">
<Job ConfigName="Test" ConfigVersion="Test02" Date="%jDate%" Time="%jTime%">
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
  <TypedParameter Name="FillNumber" Value="30" Type="Info"/>
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


def wipeOutDB(bkDB):
    """(carefully) wipe out the content of the DB"""

    if bkDB.dbHost in ["int12r", "LHCB_DIRACBOOKKEEPING", "LHCBR"]:
        print("STOOOOOP")
        print("Why are you trying to run against %s?" % bkDB.dbHost)
        sys.exit(1)

    bkDB.dbW_.query("DELETE FROM newrunquality")
    bkDB.dbW_.query("DELETE FROM productionoutputfiles")
    bkDB.dbW_.query("DELETE FROM inputfiles")
    bkDB.dbW_.query("DELETE FROM stepscontainer")
    bkDB.dbW_.query("DELETE FROM runstatus")
    bkDB.dbW_.query("DELETE FROM dataquality")
    bkDB.dbW_.query("DELETE FROM filetypes")
    bkDB.dbW_.query("DELETE FROM files")
    bkDB.dbW_.query("DELETE FROM eventtypes")
    bkDB.dbW_.query("DELETE FROM jobs")
    bkDB.dbW_.query("DELETE FROM steps")
    bkDB.dbW_.query("DELETE FROM productionscontainer")
    bkDB.dbW_.query("DELETE FROM processing")
    bkDB.dbW_.query("DELETE FROM simulationconditions")
    bkDB.dbW_.query("DELETE FROM configurations")
    bkDB.dbW_.query("DELETE FROM data_taking_conditions")
    bkDB.dbW_.query("DELETE FROM newrunquality")

    # still needed?
    bkDB.dbW_.query("DELETE FROM applications")
    bkDB.dbW_.query("DELETE FROM prodrunview")
    bkDB.dbW_.query("DELETE FROM runtimeprojects")
    bkDB.dbW_.query("DELETE FROM stepstmp")
    bkDB.dbW_.query("DELETE FROM tags")


def addBasicData(bkDB):
    bkDB.dbW_.query("INSERT INTO dataquality VALUES(1, 'OK')")
    bkDB.addProcessing(["Real Data"])


def insertRAWFiles(bk):

    res = bk.insertFileTypes("RAW", "Boole output, RAW buffer", "MDF")
    assert res["OK"], res["Message"]

    res = bk.insertEventType(30000000, "This is 30000000", "something Lambda X (blah)")
    assert res["OK"], res["Message"]

    res = bk.setRunAndProcessingPassDataQuality(runnb_1, "/Real Data", "OK")
    assert res["OK"], res["Message"]
    res = bk.setRunAndProcessingPassDataQuality(runnb_2, "/Real Data", "OK")
    assert res["OK"], res["Message"]

    currentTime = datetime.datetime.now()

    # first group
    jobXML = xmlJob_1.replace("%jDate%", currentTime.strftime("%Y-%m-%d"))
    jobXML = jobXML.replace("%jTime%", currentTime.strftime("%H:%M"))
    jobXML = jobXML.replace("%runnb%", runnb_1)
    jobXML = jobXML.replace("%jStart%", currentTime.strftime("%Y-%m-%d %H:%M"))
    jobXML = jobXML.replace("%jEnd%", currentTime.strftime("%Y-%m-%d %H:%M"))
    xmlReport = jobXML
    for f in rawFiles_1:
	xmlReport += xmlFile.replace("%filename%", f).replace("%fileCreation%", currentTime.strftime("%Y-%m-%d %H:%M"))

    xmlReport += dqCond
    res = bk.sendXMLBookkeepingReport(xmlReport)
    assert res["OK"], res["Message"]

    res = bk.addReplica(rawFiles_1)
    assert res["OK"] is True
    assert res["Value"]["Failed"] == []
    assert res["Value"]["Successful"] == rawFiles_1

    # second group
    jobXML = xmlJob_2.replace("%jDate%", currentTime.strftime("%Y-%m-%d"))
    jobXML = jobXML.replace("%jTime%", currentTime.strftime("%H:%M"))
    jobXML = jobXML.replace("%runnb%", runnb_2)
    jobXML = jobXML.replace("%jStart%", currentTime.strftime("%Y-%m-%d %H:%M"))
    jobXML = jobXML.replace("%jEnd%", currentTime.strftime("%Y-%m-%d %H:%M"))
    xmlReport = jobXML
    for f in rawFiles_2:
	xmlReport += xmlFile.replace("%filename%", f).replace("%fileCreation%", currentTime.strftime("%Y-%m-%d %H:%M"))

    xmlReport += dqCond
    res = bk.sendXMLBookkeepingReport(xmlReport)
    assert res["OK"], res["Message"]

    res = bk.addReplica(rawFiles_2)
    assert res["OK"] is True
    assert res["Value"]["Failed"] == []
    assert res["Value"]["Successful"] == rawFiles_2
