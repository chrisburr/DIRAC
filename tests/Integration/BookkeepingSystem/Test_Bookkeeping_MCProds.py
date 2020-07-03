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
Test all the methods, which are used to register a production to the bkk. To register a
production to db requites:
-existance of the simulation conditions
-steps
-production
"""

# pylint: disable=invalid-name,wrong-import-position

import io
import os
import datetime

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.tests.Utilities.utils import find_all

from LHCbDIRAC.BookkeepingSystem.DB.OracleBookkeepingDB import OracleBookkeepingDB

# sut
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient


__RCSID__ = "$Id$"


#############################################################################
# Test data

simCondDict = {"SimDescription": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8",
               "BeamCond": "beta*~3m, zpv=25.7mm, xAngle=0.236mrad and yAngle=0.100mrad",
               "BeamEnergy": "4000 GeV",
               "Generator": "Pythia8",
               "MagneticField": "1",
               "DetectorCond": "2012, Velo Closed around offset beam",
               "Luminosity": "pp collisions nu = 2.5, no spillover",
               "G4settings": "specified in sim step",
               "Visible": 'Y'}
productionSteps = {"SimulationConditions": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8",
                   "ConfigName": "test",
                   "ConfigVersion": "Jenkins",
                   "Production": 12345,
                   "Steps": []}

xmlStep1 = """<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE Job SYSTEM "book.dtd">
<Job ConfigName="test" ConfigVersion="Jenkins" Date="%jDate%" Time="%jTime%">
  <TypedParameter Name="CPUTIME" Type="Info" Value="36196.1"/>
  <TypedParameter Name="ExecTime" Type="Info" Value="36571.0480781"/>
  <TypedParameter Name="WNMODEL" Type="Info" Value="Intel(R)Xeon(R)CPUE5-2650v2@2.60GHz"/>
  <TypedParameter Name="WNCPUPOWER" Type="Info" Value="1"/>
  <TypedParameter Name="WNCACHE" Type="Info" Value="2593.748"/>
  <TypedParameter Name="WorkerNode" Type="Info" Value="b6bd1ec9ae.cern.ch"/>
  <TypedParameter Name="WNMEMORY" Type="Info" Value="1667656.0"/>
  <TypedParameter Name="WNCPUHS06" Type="Info" Value="11.4"/>
  <TypedParameter Name="Production" Type="Info" Value="%jProduction%"/>
  <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
  <TypedParameter Name="Name" Type="Info" Value="00056438_00001025_test_1"/>
  <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
  <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
  <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
  <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
  <TypedParameter Name="ProgramName" Type="Info" Value="Gauss"/>
  <TypedParameter Name="ProgramVersion" Type="Info" Value="v49r5"/>
  <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
  <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
  <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
  <TypedParameter Name="StepID" Type="Info" Value="%jStepid%"/>
  <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
  <OutputFile Name="/lhcb/MC/2012/SIM/00056438/0000/00056438_00001025_test_1.sim" TypeName="SIM" TypeVersion="ROOT">
          <Parameter Name="EventTypeId" Value="11104131"/>
          <Parameter Name="EventStat" Value="411"/>
          <Parameter Name="FileSize" Value="862802861"/>
          <Parameter Name="MD5Sum" Value="ae647981ea419cc9f8e8fa0a2d6bfd3d"/>
          <Parameter Name="Guid" Value="546014C4-55C6-E611-8E94-02163E00F6B2"/>
  </OutputFile>
  <OutputFile Name="/lhcb/MC/2012/LOG/00056438/0000/00001025/Gauss_00056438_00001025_test_1.log" """ +\
    """TypeName="LOG" TypeVersion="1">
          <Parameter Name="FileSize" Value="319867"/>
          <Replica Location="Web" Name="http://lhcb-logs.cern.ch/""" +\
    """storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Gauss_00056438_00001025_test_1.log"/>
          <Parameter Name="MD5Sum" Value="e4574c9083d1163d43ba6ac033cbd769"/>
          <Parameter Name="Guid" Value="E4574C90-83D1-163D-43BA-6AC033CBD769"/>
  </OutputFile>
  <SimulationCondition>
          <Parameter Name="SimDescription" Value="Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"/>
  </SimulationCondition>
</Job>
"""

xmlStep2 = """<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE Job SYSTEM "book.dtd">
<Job ConfigName="test" ConfigVersion="Jenkins" Date="%jDate%" Time="%jTime%">
  <TypedParameter Name="CPUTIME" Type="Info" Value="234.52"/>
  <TypedParameter Name="ExecTime" Type="Info" Value="342.997269869"/>
  <TypedParameter Name="WNMODEL" Type="Info" Value="Intel(R)Xeon(R)CPUE5-2650v2@2.60GHz"/>
  <TypedParameter Name="WNCPUPOWER" Type="Info" Value="1"/>
  <TypedParameter Name="WNCACHE" Type="Info" Value="2593.748"/>
  <TypedParameter Name="WorkerNode" Type="Info" Value="b6bd1ec9ae.cern.ch"/>
  <TypedParameter Name="WNMEMORY" Type="Info" Value="1297688.0"/>
  <TypedParameter Name="WNCPUHS06" Type="Info" Value="11.4"/>
  <TypedParameter Name="Production" Type="Info" Value="%jProduction%"/>
  <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
  <TypedParameter Name="Name" Type="Info" Value="00056438_00001025_test_2"/>
  <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
  <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
  <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
  <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
  <TypedParameter Name="ProgramName" Type="Info" Value="Boole"/>
  <TypedParameter Name="ProgramVersion" Type="Info" Value="v30r1"/>
  <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
  <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
  <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
  <TypedParameter Name="StepID" Type="Info" Value="%jStepid%"/>
  <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
  <InputFile Name="/lhcb/MC/2012/SIM/00056438/0000/00056438_00001025_test_1.sim"/>
  <OutputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_2.digi" TypeName="DIGI" TypeVersion="ROOT">
          <Parameter Name="EventTypeId" Value="11104131"/>
          <Parameter Name="EventStat" Value="411"/>
          <Parameter Name="FileSize" Value="241904920"/>
          <Parameter Name="MD5Sum" Value="a76f78f3c86cc36c663d18b4e16861b9"/>
          <Parameter Name="Guid" Value="7EF857D2-AAC6-E611-BBBC-02163E00F6B2"/>
  </OutputFile>
  <OutputFile Name="/lhcb/MC/2012/LOG/00056438/0000/00001025/Boole_00056438_00001025_test_2.log" """ +\
    """TypeName="LOG" TypeVersion="1">
          <Parameter Name="FileSize" Value="131897"/>
          <Replica Location="Web" Name="http://lhcb-logs.cern.ch/""" +\
    """storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Boole_00056438_00001025_test_2.log"/>
          <Parameter Name="MD5Sum" Value="2d9cdd2116535cd484cf06cdb1620d75"/>
          <Parameter Name="Guid" Value="2D9CDD21-1653-5CD4-84CF-06CDB1620D75"/>
  </OutputFile>
</Job>
"""

xmlStep3 = """<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE Job SYSTEM "book.dtd">
<Job ConfigName="test" ConfigVersion="Jenkins" Date="%jDate%" Time="%jTime%">
  <TypedParameter Name="CPUTIME" Type="Info" Value="521.94"/>
  <TypedParameter Name="ExecTime" Type="Info" Value="576.953828096"/>
  <TypedParameter Name="WNMODEL" Type="Info" Value="Intel(R)Xeon(R)CPUE5-2650v2@2.60GHz"/>
  <TypedParameter Name="WNCPUPOWER" Type="Info" Value="1"/>
  <TypedParameter Name="WNCACHE" Type="Info" Value="2593.748"/>
  <TypedParameter Name="WorkerNode" Type="Info" Value="b6bd1ec9ae.cern.ch"/>
  <TypedParameter Name="WNMEMORY" Type="Info" Value="899072.0"/>
  <TypedParameter Name="WNCPUHS06" Type="Info" Value="11.4"/>
  <TypedParameter Name="Production" Type="Info" Value="%jProduction%"/>
  <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
  <TypedParameter Name="Name" Type="Info" Value="00056438_00001025_test_3"/>
  <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
  <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
  <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
  <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
  <TypedParameter Name="ProgramName" Type="Info" Value="Moore"/>
  <TypedParameter Name="ProgramVersion" Type="Info" Value="v20r4"/>
  <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
  <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
  <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
  <TypedParameter Name="StepID" Type="Info" Value="%jStepid%"/>
  <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
  <InputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_2.digi"/>
  <OutputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_3.digi" TypeName="DIGI" TypeVersion="ROOT">
          <Parameter Name="EventTypeId" Value="11104131"/>
          <Parameter Name="EventStat" Value="411"/>
          <Parameter Name="FileSize" Value="164753549"/>
          <Parameter Name="MD5Sum" Value="a47bd5214a02b77f2507e0f4dd0b1fb5"/>
          <Parameter Name="Guid" Value="6A6A5873-ABC6-E611-A680-02163E00F6B2"/>
  </OutputFile>
  <OutputFile Name="/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_test_3.log" """ +\
    """TypeName="LOG" TypeVersion="1">
          <Parameter Name="FileSize" Value="57133"/>
          <Replica Location="Web" Name="http://lhcb-logs.cern.ch/""" +\
    """storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_test_3.log"/>
          <Parameter Name="MD5Sum" Value="c62640e23c464305ff1c3b7b58b3027c"/>
          <Parameter Name="Guid" Value="C62640E2-3C46-4305-FF1C-3B7B58B3027C"/>
  </OutputFile>
</Job>
"""

xmlStep4 = """<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE Job SYSTEM "book.dtd">
<Job ConfigName="test" ConfigVersion="Jenkins" Date="%jDate%" Time="%jTime%">
  <TypedParameter Name="CPUTIME" Type="Info" Value="677.39"/>
  <TypedParameter Name="ExecTime" Type="Info" Value="836.60585618"/>
  <TypedParameter Name="WNMODEL" Type="Info" Value="Intel(R)Xeon(R)CPUE5-2650v2@2.60GHz"/>
  <TypedParameter Name="WNCPUPOWER" Type="Info" Value="1"/>
  <TypedParameter Name="WNCACHE" Type="Info" Value="2593.748"/>
  <TypedParameter Name="WorkerNode" Type="Info" Value="b6bd1ec9ae.cern.ch"/>
  <TypedParameter Name="WNMEMORY" Type="Info" Value="1918032.0"/>
  <TypedParameter Name="WNCPUHS06" Type="Info" Value="11.4"/>
  <TypedParameter Name="Production" Type="Info" Value="%jProduction%"/>
  <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
  <TypedParameter Name="Name" Type="Info" Value="00056438_00001025_test_4"/>
  <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
  <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
  <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
  <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
  <TypedParameter Name="ProgramName" Type="Info" Value="Moore"/>
  <TypedParameter Name="ProgramVersion" Type="Info" Value="v14r2p1"/>
  <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
  <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
  <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
  <TypedParameter Name="StepID" Type="Info" Value="%jStepid%"/>
  <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
  <InputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_3.digi"/>
  <OutputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_4.digi" TypeName="DIGI" TypeVersion="ROOT">
            <Parameter Name="EventTypeId" Value="11104131"/>
            <Parameter Name="EventStat" Value="411"/>
            <Parameter Name="FileSize" Value="159740940"/>
            <Parameter Name="MD5Sum" Value="b062307166b1a8e4fb905d3fb38394c7"/>
            <Parameter Name="Guid" Value="48911F46-ADC6-E611-BD04-02163E00F6B2"/>
  </OutputFile>
  <OutputFile Name="/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_test_4.log" """ +\
    """TypeName="LOG" TypeVersion="1">
            <Parameter Name="FileSize" Value="1621948"/>
            <Replica Location="Web" Name="http://lhcb-logs.cern.ch/""" +\
    """storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_test_4.log"/>
            <Parameter Name="MD5Sum" Value="ea8bc998c1905a1c6ff192393a931766"/>
            <Parameter Name="Guid" Value="EA8BC998-C190-5A1C-6FF1-92393A931766"/>
  </OutputFile>
</Job>
"""

xmlStep5 = """<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE Job SYSTEM "book.dtd">
<Job ConfigName="test" ConfigVersion="Jenkins" Date="%jDate%" Time="%jTime%">
  <TypedParameter Name="CPUTIME" Type="Info" Value="494.27"/>
  <TypedParameter Name="ExecTime" Type="Info" Value="617.996832132"/>
  <TypedParameter Name="WNMODEL" Type="Info" Value="Intel(R)Xeon(R)CPUE5-2650v2@2.60GHz"/>
  <TypedParameter Name="WNCPUPOWER" Type="Info" Value="1"/>
  <TypedParameter Name="WNCACHE" Type="Info" Value="2593.748"/>
  <TypedParameter Name="WorkerNode" Type="Info" Value="b6bd1ec9ae.cern.ch"/>
  <TypedParameter Name="WNMEMORY" Type="Info" Value="692064.0"/>
  <TypedParameter Name="WNCPUHS06" Type="Info" Value="11.4"/>
  <TypedParameter Name="Production" Type="Info" Value="%jProduction%"/>
  <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
  <TypedParameter Name="Name" Type="Info" Value="00056438_00001025_test_5"/>
  <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
  <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
  <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
  <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
  <TypedParameter Name="ProgramName" Type="Info" Value="Noether"/>
  <TypedParameter Name="ProgramVersion" Type="Info" Value="v1r4"/>
  <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
  <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
  <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
  <TypedParameter Name="StepID" Type="Info" Value="%jStepid%"/>
  <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
  <InputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_4.digi"/>
  <OutputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_5.digi" TypeName="DIGI" TypeVersion="ROOT">
            <Parameter Name="EventTypeId" Value="11104131"/>
            <Parameter Name="EventStat" Value="411"/>
            <Parameter Name="FileSize" Value="166543538"/>
            <Parameter Name="MD5Sum" Value="3f15ea07bc80df0e8fd7a00bf21bf426"/>
            <Parameter Name="Guid" Value="E88994D2-AEC6-E611-9D2C-02163E00F6B2"/>
  </OutputFile>
  <OutputFile Name="/lhcb/MC/2012/LOG/00056438/0000/00001025/Noether_00056438_00001025_test_5.log" """ +\
    """TypeName="LOG" TypeVersion="1">
            <Parameter Name="FileSize" Value="30967"/>
            <Replica Location="Web" Name="http://lhcb-logs.cern.ch/""" +\
    """storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Noether_00056438_00001025_test_5.log"/>
            <Parameter Name="MD5Sum" Value="bbe1d585f4961281968c48ed6f115f98"/>
            <Parameter Name="Guid" Value="BBE1D585-F496-1281-968C-48ED6F115F98"/>
  </OutputFile>
</Job>
"""

xmlStep6 = """<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE Job SYSTEM "book.dtd">
<Job ConfigName="test" ConfigVersion="Jenkins" Date="%jDate%" Time="%jTime%">
  <TypedParameter Name="CPUTIME" Type="Info" Value="518.02"/>
  <TypedParameter Name="ExecTime" Type="Info" Value="585.730292082"/>
  <TypedParameter Name="WNMODEL" Type="Info" Value="Intel(R)Xeon(R)CPUE5-2650v2@2.60GHz"/>
  <TypedParameter Name="WNCPUPOWER" Type="Info" Value="1"/>
  <TypedParameter Name="WNCACHE" Type="Info" Value="2593.748"/>
  <TypedParameter Name="WorkerNode" Type="Info" Value="b6bd1ec9ae.cern.ch"/>
  <TypedParameter Name="WNMEMORY" Type="Info" Value="899044.0"/>
  <TypedParameter Name="WNCPUHS06" Type="Info" Value="11.4"/>
  <TypedParameter Name="Production" Type="Info" Value="%jProduction%"/>
  <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
  <TypedParameter Name="Name" Type="Info" Value="00056438_00001025_test_6"/>
  <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
  <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
  <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
  <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
  <TypedParameter Name="ProgramName" Type="Info" Value="Moore"/>
  <TypedParameter Name="ProgramVersion" Type="Info" Value="v20r4"/>
  <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
  <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
  <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
  <TypedParameter Name="StepID" Type="Info" Value="%jStepid%"/>
  <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
  <InputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_5.digi"/>
  <OutputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_6.digi" TypeName="DIGI" TypeVersion="ROOT">
            <Parameter Name="EventTypeId" Value="11104131"/>
            <Parameter Name="EventStat" Value="411"/>
            <Parameter Name="FileSize" Value="166994688"/>
            <Parameter Name="MD5Sum" Value="e1d47cd7962d2dc4181508ab26b19fba"/>
            <Parameter Name="Guid" Value="58DB8F37-B0C6-E611-9B3C-02163E00F6B2"/>
  </OutputFile>
  <OutputFile Name="/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_test_6.log" """ +\
    """TypeName="LOG" TypeVersion="1">
            <Parameter Name="FileSize" Value="56250"/>
            <Replica Location="Web" Name="http://lhcb-logs.cern.ch/""" +\
    """storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_test_6.log"/>
            <Parameter Name="MD5Sum" Value="6521d54c12608adc7b06c92e43d7d824"/>
            <Parameter Name="Guid" Value="6521D54C-1260-8ADC-7B06-C92E43D7D824"/>
  </OutputFile>
</Job>
"""

xmlStep7 = """<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE Job SYSTEM "book.dtd">
<Job ConfigName="test" ConfigVersion="Jenkins" Date="%jDate%" Time="%jTime%">
  <TypedParameter Name="CPUTIME" Type="Info" Value="641.7"/>
  <TypedParameter Name="ExecTime" Type="Info" Value="709.81375289"/>
  <TypedParameter Name="WNMODEL" Type="Info" Value="Intel(R)Xeon(R)CPUE5-2650v2@2.60GHz"/>
  <TypedParameter Name="WNCPUPOWER" Type="Info" Value="1"/>
  <TypedParameter Name="WNCACHE" Type="Info" Value="2593.748"/>
  <TypedParameter Name="WorkerNode" Type="Info" Value="b6bd1ec9ae.cern.ch"/>
  <TypedParameter Name="WNMEMORY" Type="Info" Value="2001584.0"/>
  <TypedParameter Name="WNCPUHS06" Type="Info" Value="11.4"/>
  <TypedParameter Name="Production" Type="Info" Value="%jProduction%"/>
  <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
  <TypedParameter Name="Name" Type="Info" Value="00056438_00001025_test_7"/>
  <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
  <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
  <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
  <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
  <TypedParameter Name="ProgramName" Type="Info" Value="Moore"/>
  <TypedParameter Name="ProgramVersion" Type="Info" Value="v14r6"/>
  <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
  <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
  <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
  <TypedParameter Name="StepID" Type="Info" Value="%jStepid%"/>
  <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
  <InputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_6.digi"/>
  <OutputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_7.digi" TypeName="DIGI" TypeVersion="ROOT">
            <Parameter Name="EventTypeId" Value="11104131"/>
            <Parameter Name="EventStat" Value="411"/>
            <Parameter Name="FileSize" Value="161641611"/>
            <Parameter Name="MD5Sum" Value="f7f2d353164382712bec0ddfc46943ec"/>
            <Parameter Name="Guid" Value="EAB2A0D4-B1C6-E611-A70C-02163E00F6B2"/>
  </OutputFile>
  <OutputFile Name="/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_test_7.log" """ +\
    """TypeName="LOG" TypeVersion="1">
            <Parameter Name="FileSize" Value="1709809"/>
            <Replica Location="Web" Name="http://lhcb-logs.cern.ch/""" +\
    """storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_test_7.log"/>
            <Parameter Name="MD5Sum" Value="a2209db13ee25ba252c6c52839232999"/>
            <Parameter Name="Guid" Value="A2209DB1-3EE2-5BA2-52C6-C52839232999"/>
  </OutputFile>
</Job>
"""

xmlStep8 = """<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE Job SYSTEM "book.dtd">
<Job ConfigName="test" ConfigVersion="Jenkins" Date="%jDate%" Time="%jTime%">
  <TypedParameter Name="CPUTIME" Type="Info" Value="472.93"/>
  <TypedParameter Name="ExecTime" Type="Info" Value="493.59373498"/>
  <TypedParameter Name="WNMODEL" Type="Info" Value="Intel(R)Xeon(R)CPUE5-2650v2@2.60GHz"/>
  <TypedParameter Name="WNCPUPOWER" Type="Info" Value="1"/>
  <TypedParameter Name="WNCACHE" Type="Info" Value="2593.748"/>
  <TypedParameter Name="WorkerNode" Type="Info" Value="b6bd1ec9ae.cern.ch"/>
  <TypedParameter Name="WNMEMORY" Type="Info" Value="700256.0"/>
  <TypedParameter Name="WNCPUHS06" Type="Info" Value="11.4"/>
  <TypedParameter Name="Production" Type="Info" Value="%jProduction%"/>
  <TypedParameter Name="DiracJobId" Type="Info" Value="147844677"/>
  <TypedParameter Name="Name" Type="Info" Value="00056438_00001025_test_8"/>
  <TypedParameter Name="JobStart" Type="Info" Value="%jStart%"/>
  <TypedParameter Name="JobEnd" Type="Info" Value="%jEnd%"/>
  <TypedParameter Name="Location" Type="Info" Value="LCG.CERN.ch"/>
  <TypedParameter Name="JobType" Type="Info" Value="MCSimulation"/>
  <TypedParameter Name="ProgramName" Type="Info" Value="Noether"/>
  <TypedParameter Name="ProgramVersion" Type="Info" Value="v1r4"/>
  <TypedParameter Name="DiracVersion" Type="Info" Value="v6r15p9"/>
  <TypedParameter Name="FirstEventNumber" Type="Info" Value="1"/>
  <TypedParameter Name="StatisticsRequested" Type="Info" Value="-1"/>
  <TypedParameter Name="StepID" Type="Info" Value="%jStepid%"/>
  <TypedParameter Name="NumberOfEvents" Type="Info" Value="411"/>
  <InputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_7.digi"/>
  <OutputFile Name="/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_8.digi" TypeName="DIGI" TypeVersion="ROOT">
            <Parameter Name="EventTypeId" Value="11104131"/>
            <Parameter Name="EventStat" Value="411"/>
            <Parameter Name="FileSize" Value="168778046"/>
            <Parameter Name="MD5Sum" Value="391359b6a37856985b831c09e0653427"/>
            <Parameter Name="Guid" Value="342F831B-B3C6-E611-94AA-02163E00F6B2"/>
  </OutputFile>
  <OutputFile Name="/lhcb/MC/2012/LOG/00056438/0000/00001025/Noether_00056438_00001025_test_8.log" """ +\
    """TypeName="LOG" TypeVersion="1">
            <Parameter Name="FileSize" Value="31116"/>
            <Replica Location="Web" Name="http://lhcb-logs.cern.ch/""" +\
    """storage/lhcb/MC/2012/LOG/00056438/0000/00001025/Noether_00056438_00001025_test_8.log"/>
            <Parameter Name="MD5Sum" Value="0a622440c036b46811912e48ceee076f"/>
            <Parameter Name="Guid" Value="0A622440-C036-B468-1191-2E48CEEE076F"/>
  </OutputFile>
</Job>
"""


#############################################################################

# What's used for the tests
bk = BookkeepingClient()

# # first delete from DB ####################
bkDB = OracleBookkeepingDB()

bkDB.dbW_._query("DELETE FROM productionoutputfiles")
bkDB.dbW_._query("DELETE FROM stepscontainer")
bkDB.dbW_._query("DELETE FROM inputfiles")
bkDB.dbW_._query("DELETE FROM files")
bkDB.dbW_._query("DELETE FROM filetypes")
bkDB.dbW_._query("DELETE FROM eventtypes")
bkDB.dbW_._query("DELETE FROM jobs")
bkDB.dbW_._query("DELETE FROM steps")
bkDB.dbW_._query("DELETE FROM productionscontainer")
bkDB.dbW_._query("DELETE FROM processing")
bkDB.dbW_._query("DELETE FROM simulationconditions")
bkDB.dbW_._query("DELETE FROM configurations")
bkDB.dbW_._query("DELETE FROM data_taking_conditions")
bkDB.dbW_._query("DELETE FROM newrunquality")

# # #########################################

#############################################################################


def test_insertSimConditions():
  """
  register a simulation condition to the db
  """
  retVal = bk.insertSimConditions(simCondDict)
  if retVal["OK"]:
    assert retVal['OK'] is True
  else:
    assert 'unique constraint' in retVal["Message"]


def test_registerProduction():
  """
  insert all steps which will be used by the production and register the production
  """

  # preparing
  bk.insertFileTypes('SIM', 'sim', 'ROOT')
  bk.insertFileTypes('DIGI', 'digi', 'ROOT')
  bk.insertEventType(11104131, 'This is 11104131L', 'something Lambda Xyz (blah)')

  # actual tests
  retVal = bk.insertStep(
      {
          'Step': {
              'ApplicationName': 'Gauss',
              'Usable': 'Yes',
              'StepId': '',
              'ApplicationVersion': 'v49r5',
              'ExtraPackages': 'AppConfig.v3r277;Gen/DecFiles.v29r10',
              'StepName': 'Cert-Sim09b - 2012 - MU - Pythia8',
              'ProcessingPass': 'Sim09b',
              'isMulticore': 'N',
              'Visible': 'Y',
              'DDDB': 'dddb-20150928',
              'SystemConfig': 'x86_64-slc6-gcc48-opt',
              'OptionFiles': '$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.py;' +
                             '$APPCONFIGOPTS/Gauss/DataType-2012.py;$APPCONFIGOPTS/Gauss/RICHRandomHits.py;' +
                             '$APPCONFIGOPTS/Gauss/NoPacking.py;$DECFILESROOT/options/@{eventType}.py;' +
                             '$LBPYTHIA8ROOT/options/Pythia8.py;$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;' +
                             '$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
              'CONDDB': 'sim-20160321-2-vc-mu100'},
          'OutputFileTypes': [
              {
                  'Visible': 'Y',
                  'FileType': 'SIM'}]})

  assert retVal['OK'] is True
  assert retVal['Value'] > 0
  gauss_sid = retVal['Value']
  productionSteps['Steps'].append({'StepId': gauss_sid,
                                   'Visible': 'Y',
                                   'OutputFileTypes': [{'Visible': 'Y',
                                                        'FileType': 'SIM'}]})

  bkFile = find_all('Job_Report_MCFastSimulation.xml', '..', 'BookkeepingSystem')[0]
  with open(bkFile, 'r') as fd:
    filedata = fd.read()
  filedata = filedata.replace('#STEP_ID#', str(gauss_sid))
  with open(bkFile + '.temp', 'w') as fd:
    fd.write(filedata)

  retVal = bk.insertStep(
      {'Step': {'ApplicationName': 'Boole',
                'Usable': 'Yes',
                'StepId': '',
                'ApplicationVersion': 'v30r1',
                'ExtraPackages': 'AppConfig.v3r266',
                'StepName': 'Cert-Digi14a for 2012 (to use w Sim09)',
                'ProcessingPass': 'Digi14a',
                'isMulticore': 'N',
                'Visible': 'N',
                'SystemConfig': 'x86_64-slc6-gcc48-opt',
                'DDDB': 'fromPreviousStep',
                'OptionFiles': '$APPCONFIGOPTS/Boole/Default.py;$APPCONFIGOPTS/Boole/DataType-2012.py;' +
                               '$APPCONFIGOPTS/Boole/NoPacking.py;' +
                               '$APPCONFIGOPTS/Boole/Boole-SetOdinRndTrigger.py;' +
                               '$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                'CONDDB': 'fromPreviousStep'},
       'InputFileTypes': [{'Visible': 'N', 'FileType': 'SIM'}],
       'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]})

  assert retVal['OK'] is True
  assert retVal['Value'] > 0
  productionSteps['Steps'].append({'StepId': retVal['Value'], 'Visible': 'N',
                                   'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]})

  retVal = bk.insertStep(
      {'Step': {'ApplicationName': 'Moore',
                'Usable': 'Yes',
                'StepId': '',
                'ApplicationVersion': 'v20r4',
                'ExtraPackages': 'AppConfig.v3r200',
                'StepName': 'Cert-L0 emulation - TCK 003d',
                'ProcessingPass': 'L0Trig0x003d',
                'OptionsFormat': 'l0app',
                'isMulticore': 'N',
                'Visible': 'N',
                'SystemConfig': 'x86_64-slc6-gcc48-opt',
                'DDDB': 'fromPreviousStep',
                'OptionFiles': '$APPCONFIGOPTS/L0App/L0AppSimProduction.py;' +
                               '$APPCONFIGOPTS/L0App/L0AppTCK-0x003d.py;' +
                               '$APPCONFIGOPTS/L0App/DataType-2012.py',
                'CONDDB': 'fromPreviousStep'},
       'InputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}],
       'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]})

  assert retVal['OK'] is True
  assert retVal['Value'] > 0
  moore_sid = retVal['Value']
  productionSteps['Steps'].append({'StepId': moore_sid,
                                   'Visible': 'N',
                                   'OutputFileTypes': [{'Visible': 'N',
                                                        'FileType': 'DIGI'}]})

  bkFile = find_all('Job_Report_MCReconstruction_1.xml', '..', 'BookkeepingSystem')[0]
  with open(bkFile, 'r') as fd:
    filedata = fd.read()
  filedata = filedata.replace('#STEP_ID#', str(moore_sid))
  with open(bkFile + '.temp', 'w') as fd:
    fd.write(filedata)

  bkFile = find_all('Job_Report_MCReconstruction_2.xml', '..', 'BookkeepingSystem')[0]
  with open(bkFile, 'r') as fd:
    filedata = fd.read()
  filedata = filedata.replace('#STEP_ID#', str(moore_sid))
  with open(bkFile + '.temp', 'w') as fd:
    fd.write(filedata)

  retVal = bk.insertStep(
      {'Step': {'ApplicationName': 'Moore',
                'Usable': 'Yes',
                'StepId': '',
                'ApplicationVersion': 'v14r2p1',
                'ExtraPackages': 'AppConfig.v3r288',
                'StepName': 'Cert-TCK-0x4097003d Flagged MC - 2012 - to be used in multipleTCKs',
                'ProcessingPass': 'Trig0x4097003d',
                'isMulticore': 'N',
                'Visible': 'N',
                'DDDB': 'fromPreviousStep',
                'OptionFiles': '$APPCONFIGOPTS/Moore/MooreSimProductionForSeparateL0AppStep.py;' +
                               '$APPCONFIGOPTS/Conditions/TCK-0x4097003d.py;' +
                               '$APPCONFIGOPTS/Moore/DataType-2012.py',
                'CONDDB': 'fromPreviousStep'},
       'InputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}],
       'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]})

  assert retVal['OK'] is True
  assert retVal['OK'] > 0
  productionSteps['Steps'].append({'StepId': retVal['Value'],
                                   'Visible': 'N',
                                   'OutputFileTypes': [{'Visible': 'N',
                                                        'FileType': 'DIGI'}]})

  retVal = bk.insertStep(
      {'Step': {'ApplicationName': 'Noether',
                'Usable': 'Yes',
                'StepId': '',
                'ApplicationVersion': 'v1r4',
                'ExtraPackages': 'AppConfig.v3r200',
                'StepName': 'Cert-Move TCK-0x4097003d from default location',
                'ProcessingPass': 'MoveTCK0x4097003d',
                'isMulticore': 'N',
                'Visible': 'N',
                'DDDB': 'fromPreviousStep',
                'OptionFiles': '$APPCONFIGOPTS/Moore/MoveTCK.py;$APPCONFIGOPTS/Moore/MoveTCK-0x4097003d.py',
                'CONDDB': 'fromPreviousStep'},
       'InputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}],
       'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]})

  assert retVal['OK'] is True
  assert retVal['OK'] > 0
  noether_sid = retVal['Value']
  productionSteps['Steps'].append({'StepId': noether_sid,
                                   'Visible': 'N',
                                   'OutputFileTypes': [{'Visible': 'N',
                                                        'FileType': 'DIGI'}]})

  bkFile = find_all('Job_Report_MCMerge.xml', '..', 'BookkeepingSystem')[0]
  with open(bkFile, 'r') as fd:
    filedata = fd.read()
  filedata = filedata.replace('#STEP_ID#', str(noether_sid))
  with open(bkFile + '.temp', 'w') as fd:
    fd.write(filedata)

  retVal = bk.insertStep(
      {'Step': {'ApplicationName': 'Moore',
                'Usable': 'Yes',
                'StepId': '',
                'ApplicationVersion': 'v20r4',
                'ExtraPackages': 'AppConfig.v3r200',
                'StepName': 'Cert-L0 emulation - TCK 0042',
                'ProcessingPass': 'L0Trig0x0042',
                'OptionsFormat': 'l0a',
                'isMulticore': 'N',
                'Visible': 'N',
                'DDDB': 'fromPreviousStep',
                'OptionFiles': '$APPCONFIGOPTS/L0App/L0AppSimProduction.py;' +
                               '$APPCONFIGOPTS/L0App/L0AppTCK-0x0042.py;' +
                               '$APPCONFIGOPTS/L0App/DataType-2012.py',
                'CONDDB': 'fromPreviousStep'},
       'InputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}],
       'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]})

  assert retVal['OK'] is True
  assert retVal['OK'] > 0
  productionSteps['Steps'].append({'StepId': retVal['Value'], 'Visible': 'N',
                                   'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]})

  retVal = bk.insertStep(
      {'Step': {'ApplicationName': 'Moore',
                'Usable': 'Yes',
                'StepId': '',
                'ApplicationVersion': 'v14r6',
                'ExtraPackages': 'AppConfig.v3r300',
                'StepName': 'Cert-TCK-0x40990042 Flagged MC - 2012 - to be used in multipleTCKs',
                'ProcessingPass': 'Trig0x40990042',
                'OptionsFormat': 'l0a',
                'isMulticore': 'N',
                'Visible': 'N',
                'DDDB': 'fromPreviousStep',
                'OptionFiles': '$APPCONFIGOPTS/Moore/MooreSimProductionForSeparateL0AppStep.py;' +
                               '$APPCONFIGOPTS/Conditions/TCK-0x40990042.py;' +
                               '$APPCONFIGOPTS/Moore/DataType-2012.py',
                'CONDDB': 'fromPreviousStep'},
       'InputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}],
       'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]})

  assert retVal['OK'] is True
  assert retVal['OK'] > 0
  productionSteps['Steps'].append({'StepId': retVal['Value'], 'Visible': 'N',
                                   'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]})

  retVal = bk.insertStep(
      {'Step': {'ApplicationName': 'Noether',
                'Usable': 'Yes',
                'StepId': '',
                'ApplicationVersion': 'v1r4',
                'ExtraPackages': 'AppConfig.v3r200',
                'StepName': 'Cert-Move TCK-0x40990042 from default location',
                'ProcessingPass': 'MoveTCK0x40990042',
                'OptionsFormat': 'l0a',
                'isMulticore': 'N',
                'Visible': 'N',
                'DDDB': 'fromPreviousStep',
                'OptionFiles': '$APPCONFIGOPTS/Moore/MoveTCK.py;$APPCONFIGOPTS/Moore/MoveTCK-0x40990042.py',
                'CONDDB': 'fromPreviousStep'},
       'InputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}],
       'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]})

  assert retVal['OK'] is True
  assert retVal['OK'] > 0
  productionSteps['Steps'].append({'StepId': retVal['Value'], 'Visible': 'N',
                                   'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]})
  productionSteps['EventType'] = 11104131
  retVal = bk.addProduction(productionSteps)
  assert retVal['OK'] is True

  res = bk.getProductionInformations(12345)
  assert res['OK']
  assert len(res['Value']['Steps']) == 8


def test_sendMCXMLBookkeepingReport():

  # preparing
  bk.insertFileTypes('SIM', 'sim', 'ROOT')
  bk.insertFileTypes('DIGI', 'digi', 'ROOT')
  bk.insertFileTypes('LOG', 'log', '1')
  bk.insertEventType(27165000, 'This is 11104131', 'something GammaBeta Xyz (blah)')

  jobStart = jobEnd = datetime.datetime.now()
  jobStart = jobEnd = jobStart.replace(second=0, microsecond=0)

  currentTime = datetime.datetime.now()
  # jobEnd = currentTime.strftime( '%Y-%m-%d %H:%M' )
  # jobStart = currentTime.strftime( '%Y-%m-%d %H:%M' )
  step1 = xmlStep1.replace("%jDate%", currentTime.strftime('%Y-%m-%d'))
  step1 = step1.replace("%jTime%", currentTime.strftime('%H:%M'))
  step1 = step1.replace("%jStart%", jobStart.strftime('%Y-%m-%d %H:%M'))
  step1 = step1.replace("%jEnd%", jobEnd.strftime('%Y-%m-%d %H:%M'))
  step1 = step1.replace("%jProduction%", '12345')
  retVal = bk.getAvailableSteps({'StepName': 'Cert-Sim09b - 2012 - MU - Pythia8'})
  assert retVal['OK'] is True
  assert len(retVal['Value']['Records']) > 0
  step1 = step1.replace("%jStepid%", str(retVal['Value']['Records'][0][0]))
  retVal = bk.sendXMLBookkeepingReport(step1)
  assert retVal['OK'] is True

  currentTime = datetime.datetime.now()
  step2 = xmlStep2.replace("%jDate%", currentTime.strftime('%Y-%m-%d'))
  step2 = step2.replace("%jTime%", currentTime.strftime('%H:%M'))
  step2 = step2.replace("%jStart%", jobStart.strftime('%Y-%m-%d %H:%M'))
  step2 = step2.replace("%jEnd%", jobEnd.strftime('%Y-%m-%d %H:%M'))
  step2 = step2.replace("%jProduction%", '12345')
  retVal = bk.getAvailableSteps({'StepName': 'Cert-Digi14a for 2012 (to use w Sim09)'})
  assert retVal['OK'] is True
  assert len(retVal['Value']['Records']) > 0
  step2 = step2.replace("%jStepid%", str(retVal['Value']['Records'][0][0]))
  retVal = bk.sendXMLBookkeepingReport(step2)
  assert retVal['OK'] is True

  currentTime = datetime.datetime.now()
  step3 = xmlStep3.replace("%jDate%", currentTime.strftime('%Y-%m-%d'))
  step3 = step3.replace("%jTime%", currentTime.strftime('%H:%M'))
  step3 = step3.replace("%jStart%", jobStart.strftime('%Y-%m-%d %H:%M'))
  step3 = step3.replace("%jEnd%", jobEnd.strftime('%Y-%m-%d %H:%M'))
  step3 = step3.replace("%jProduction%", '12345')
  retVal = bk.getAvailableSteps({'StepName': 'Cert-L0 emulation - TCK 003d'})
  assert retVal['OK'] is True
  assert len(retVal['Value']['Records']) > 0
  step3 = step3.replace("%jStepid%", str(retVal['Value']['Records'][0][0]))
  retVal = bk.sendXMLBookkeepingReport(step3)
  assert retVal['OK'] is True

  currentTime = datetime.datetime.now()
  step4 = xmlStep4.replace("%jDate%", currentTime.strftime('%Y-%m-%d'))
  step4 = step4.replace("%jTime%", currentTime.strftime('%H:%M'))
  step4 = step4.replace("%jStart%", jobStart.strftime('%Y-%m-%d %H:%M'))
  step4 = step4.replace("%jEnd%", jobEnd.strftime('%Y-%m-%d %H:%M'))
  step4 = step4.replace("%jProduction%", '12345')
  retVal = bk.getAvailableSteps(
      {'StepName': 'Cert-TCK-0x4097003d Flagged MC - 2012 - to be used in multipleTCKs'})
  assert retVal['OK'] is True
  assert len(retVal['Value']['Records']) > 0
  step4 = step4.replace("%jStepid%", str(retVal['Value']['Records'][0][0]))
  retVal = bk.sendXMLBookkeepingReport(step4)
  assert retVal['OK'] is True

  currentTime = datetime.datetime.now()
  step5 = xmlStep5.replace("%jDate%", currentTime.strftime('%Y-%m-%d'))
  step5 = step5.replace("%jTime%", currentTime.strftime('%H:%M'))
  step5 = step5.replace("%jStart%", jobStart.strftime('%Y-%m-%d %H:%M'))
  step5 = step5.replace("%jEnd%", jobEnd.strftime('%Y-%m-%d %H:%M'))
  step5 = step5.replace("%jProduction%", '12345')
  retVal = bk.getAvailableSteps({'StepName': 'Cert-Move TCK-0x4097003d from default location'})
  assert retVal['OK'] is True
  assert len(retVal['Value']['Records']) > 0
  step5 = step5.replace("%jStepid%", str(retVal['Value']['Records'][0][0]))
  retVal = bk.sendXMLBookkeepingReport(step5)
  assert retVal['OK'] is True

  currentTime = datetime.datetime.now()
  step6 = xmlStep6.replace("%jDate%", currentTime.strftime('%Y-%m-%d'))
  step6 = step6.replace("%jTime%", currentTime.strftime('%H:%M'))
  step6 = step6.replace("%jStart%", jobStart.strftime('%Y-%m-%d %H:%M'))
  step6 = step6.replace("%jEnd%", jobEnd.strftime('%Y-%m-%d %H:%M'))
  step6 = step6.replace("%jProduction%", '12345')
  retVal = bk.getAvailableSteps({'StepName': 'Cert-L0 emulation - TCK 0042'})
  assert retVal['OK'] is True
  assert len(retVal['Value']['Records']) > 0
  step6 = step6.replace("%jStepid%", str(retVal['Value']['Records'][0][0]))
  retVal = bk.sendXMLBookkeepingReport(step6)
  assert retVal['OK'] is True

  currentTime = datetime.datetime.now()
  step7 = xmlStep7.replace("%jDate%", currentTime.strftime('%Y-%m-%d'))
  step7 = step7.replace("%jTime%", currentTime.strftime('%H:%M'))
  step7 = step7.replace("%jStart%", jobStart.strftime('%Y-%m-%d %H:%M'))
  step7 = step7.replace("%jEnd%", jobEnd.strftime('%Y-%m-%d %H:%M'))
  step7 = step7.replace("%jProduction%", '12345')
  retVal = bk.getAvailableSteps(
      {'StepName': 'Cert-TCK-0x40990042 Flagged MC - 2012 - to be used in multipleTCKs'})
  assert retVal['OK'] is True
  assert len(retVal['Value']['Records']) > 0
  step7 = step7.replace("%jStepid%", str(retVal['Value']['Records'][0][0]))
  retVal = bk.sendXMLBookkeepingReport(step7)
  assert retVal['OK'] is True

  currentTime = datetime.datetime.now()
  step8 = xmlStep8.replace("%jDate%", currentTime.strftime('%Y-%m-%d'))
  step8 = step8.replace("%jTime%", currentTime.strftime('%H:%M'))
  step8 = step8.replace("%jStart%", jobStart.strftime('%Y-%m-%d %H:%M'))
  step8 = step8.replace("%jEnd%", jobEnd.strftime('%Y-%m-%d %H:%M'))
  step8 = step8.replace("%jProduction%", '12345')
  retVal = bk.getAvailableSteps({'StepName': 'Cert-Move TCK-0x40990042 from default location'})
  assert retVal['OK'] is True
  assert len(retVal['Value']['Records']) > 0
  step8 = step8.replace("%jStepid%", str(retVal['Value']['Records'][0][0]))
  retVal = bk.sendXMLBookkeepingReport(step8)
  assert retVal['OK'] is True


def test_getSimConditions():
  """
  check the existence of the sim cond
  """
  retVal = bk.getSimConditions()
  assert retVal['OK'] is True
  assert len(retVal['Value']) >= 1
  assert simCondDict['SimDescription'] in (i[1] for i in retVal['Value'])


def test_getJobInformation():
  """
  test the job information method
  """
  jobStart = jobEnd = datetime.datetime.now()
  jobStart = jobEnd = jobStart.replace(second=0, microsecond=0)

  retVal = bk.getJobInformation(
      {'LFN': ['/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_8.digi',
               '/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_7.digi']})
  assert len(retVal['Value']) == 2
  params = [
      'WNMJFHS06',
      'WNCPUPower',
      'JobName',
      'Production',
      'EventInputStat',
      'Location',
      'TotalLuminosity',
      'WNCPUHS06',
      'StatisticsRequested',
      'Exectime',
      'JobId',
      'DiracVersion',
      'WNCache',
      'WNModel',
      'NumberOfEvents',
      'ConfigName',
      'WNMemory',
      'RunNumber',
      'FirstEventNumber',
      'CPUTime',
      'FillNumber',
      'WorkerNode',
      'ConfigVersion',
      'JobStart',
      'StepId',
      'JobEnd',
      'Tck',
      'DiracJobId']

  d1 = {'WNMJFHS06': 0.0,
        'WNCPUPower': '1',
        'JobName': '00056438_00001025_test_8',
        'Production': 12345,
        'EventInputStat': 411,
        'Location': 'LCG.CERN.ch',
        'TotalLuminosity': 0,
        'WNCPUHS06': 11.4,
        'StatisticsRequested': -1,
        'Exectime': 493.59373498,
        'DiracVersion': 'v6r15p9',
        'WNCache': '2593.748',
        'WNModel': 'Intel(R)Xeon(R)CPUE5-2650v2@2.60GHz',
        'NumberOfEvents': 411,
        'ConfigName': 'test',
        'WNMemory': '700256.0',
        'RunNumber': None,
        'FirstEventNumber': 1,
        'CPUTime': 472.93,
        'FillNumber': None,
        'WorkerNode': 'b6bd1ec9ae.cern.ch',
        'ConfigVersion': 'Jenkins',
        'JobStart': jobStart,
        'JobEnd': jobEnd,
        'Tck': 'None',
        'DiracJobId': 147844677}

  d2 = {'WNMJFHS06': 0.0,
        'WNCPUPower': '1',
        'JobName': '00056438_00001025_test_7',
        'Production': 12345,
        'EventInputStat': 411,
        'Location': 'LCG.CERN.ch',
        'TotalLuminosity': 0,
        'WNCPUHS06': 11.4,
        'StatisticsRequested': -1,
        'Exectime': 709.81375289,
        'DiracVersion': 'v6r15p9',
        'WNCache': '2593.748',
        'WNModel': 'Intel(R)Xeon(R)CPUE5-2650v2@2.60GHz',
        'NumberOfEvents': 411,
        'ConfigName': 'test',
        'WNMemory': '2001584.0',
        'RunNumber': None,
        'FirstEventNumber': 1,
        'CPUTime': 641.7,
        'FillNumber': None,
        'WorkerNode': 'b6bd1ec9ae.cern.ch',
        'ConfigVersion': 'Jenkins',
        'JobStart': jobStart,
        'JobEnd': jobEnd,
        'Tck': 'None',
        'DiracJobId': 147844677}

  for record in retVal['Value']:
    assert sorted(params) == sorted(record)
    record.pop('JobId')
    record.pop('StepId')
    if record['CPUTime'] == d1['CPUTime']:
      assert sorted(record.items()) == sorted(d1.items())  # can be an iterator
    elif record['CPUTime'] == d2['CPUTime']:
      assert sorted(record.items()) == sorted(d2.items())  # can be an iterator
    else:
      assert False

  retVal = bk.getJobInformation({'Production': 12345})
  assert retVal['OK'] is True
  assert len(retVal['Value']) == 8

  retVal = bk.getJobInformation({'DiracJobId': 147844677})
  assert retVal['OK'] is True
  assert len(retVal['Value']) == 8

  retVal = bk.getJobInformation({'DiracJobId': [147844677]})
  assert retVal['OK'] is True
  assert len(retVal['Value']) == 8

  retVal = bk.getJobInformation({'DiracJobId': [147844677, 147844677]})
  assert retVal['OK'] is True
  assert len(retVal['Value']) == 8


def test_sendJobReport():
  """
  Send real job XML report
  """

  # preparing
  bk.insertFileTypes('ALLSTREAMS.DST', 'bof', 'ROOT')
  bk.insertFileTypes('DSTARD02HHHH.HLTFILTER.MDST', 'booof', 'ROOT')
  bk.insertFileTypes('LOG', 'log', '1')
  res = bk.insertEventType(27165000, 'This is 27165000', 'something Lambda Xyz (blah)')
  assert res['OK']

  # actual test
  for rep in ['Job_Report_MCFastSimulation.xml.temp',
              'Job_Report_MCReconstruction_1.xml.temp',
              'Job_Report_MCReconstruction_2.xml.temp',
              'Job_Report_MCMerge.xml.temp']:
    bkFile = find_all(rep, '..', 'BookkeepingSystem')[0]
    with io.open(bkFile, 'r') as fd:
      bkXML = fd.read()
    res = bk.sendXMLBookkeepingReport(bkXML)
    assert res['OK']
    os.remove(bkFile)


def test_addFiles():
  lfns = ['/lhcb/MC/2012/SIM/00056438/0000/00056438_00001025_test_1.sim',
          '/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_2.digi']
  retVal = bk.addFiles(lfns)
  assert retVal['Value']['Successful']
  assert retVal['Value']['Failed'] == []
  assert retVal['Value']['Successful'] == lfns

  bk.updateProductionOutputfiles()
  assert retVal['OK'] is True


# FIXME: the below one fails, to understand why!

# def test_getFileTypes():
#   bkQuery = {'ConfigName': 'test',
#              'ConfigVersion': 'Jenkins'}
#   retVal = bk.getFileTypes(bkQuery)
#   assert retVal['OK'] is True
#   print retVal
#   assert retVal['Value']['ParameterNames']
#   assert retVal['Value']['Records']
#   assert retVal['Value']['TotalRecords']
#   assert retVal['Value']['TotalRecords'] == 1
#   outputFileTypes = ['SIM', 'DIGI']
#   for rec in retVal['Value']['Records']:
#     assert rec[0] in outputFileTypes

#   bkQuery = {'ConfigName': 'test',
#              'ConfigVersion': 'Jenkins',
#              'Production': 12345,
#              'Visible': 'N'}
#   retVal = bk.getFileTypes(bkQuery)
#   assert retVal['OK'] is True
#   print retVal
#   assert retVal['Value']['ParameterNames']
#   assert retVal['Value']['Records']
#   assert retVal['Value']['TotalRecords']
#   assert retVal['Value']['TotalRecords'] == 1
#   outputFileTypes = ['SIM', 'DIGI']
#   for rec in retVal['Value']['Records']:
#     assert rec[0] in outputFileTypes

#   bkQuery['EventType'] = 11104131
#   retVal = bk.getFileTypes(bkQuery)
#   assert retVal['OK'] is True
#   assert retVal['Value']['ParameterNames']
#   assert retVal['Value']['Records']
#   assert retVal['Value']['TotalRecords']
#   assert retVal['Value']['TotalRecords'] == 1
#   outputFileTypes = ['SIM', 'DIGI']
#   for rec in retVal['Value']['Records']:
#     assert rec[0] in outputFileTypes

#   bkQuery['ConditionDescription'] = 'Beam4000GeV-2012-MagUp-Nu2.5-Pythia8'
#   retVal = bk.getFileTypes(bkQuery)
#   assert retVal['OK'] is True
#   assert retVal['Value']['ParameterNames']
#   assert retVal['Value']['Records']
#   assert retVal['Value']['TotalRecords']
#   assert retVal['Value']['TotalRecords'] == 1
#   outputFileTypes = ['SIM', 'DIGI']
#   for rec in retVal['Value']['Records']:
#     assert rec[0] in outputFileTypes

#   bkQuery['ProcessingPass'] = '/Sim09b'
#   retVal = bk.getFileTypes(bkQuery)
#   assert retVal['OK'] is True
#   assert retVal['Value']['ParameterNames']
#   assert retVal['Value']['Records']
#   assert retVal['Value']['TotalRecords']
#   assert retVal['Value']['TotalRecords'] == 1
#   outputFileTypes = ['SIM', 'DIGI']
#   for rec in retVal['Value']['Records']:
#     assert rec[0] in outputFileTypes
