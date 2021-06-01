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
It tests the RAW data insert to the db.
It requires an Oracle database
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# FIXME: restore + move to pytest


# pylint: disable=invalid-name,wrong-import-position

# import sys
# import unittest

# from DIRAC.Core.Base.Script import parseCommandLine
# parseCommandLine()

# from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient


# __RCSID__ = "$Id$"


# class BaseTestCase(unittest.TestCase):
#   """ Base
#   """

#   def setUp(self):
#     super(BaseTestCase, self).setUp()
#     self.bk = BookkeepingClient()
#     self.bk.insertFileTypes('RAW', 'Boole output, RAW buffer', 'MDF')


# class TestMethods(BaseTestCase):

#   def test_getAvailableSteps(self):
#     """
#     make sure the step is created
#     """
#     retVal = self.bk.getAvailableSteps({"ApplicationName": "Moore",
#                                         "ApplicationVersion": "v0r111",
#                                         "ProcessingPadd": "/Real Data"})

#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(sorted(retVal['Value']['ParameterNames']),
#                      sorted(['StepId',
#                              'StepName',
#                              'ApplicationName',
#                              'ApplicationVersion',
#                              'OptionFiles',
#                              'DDDB',
#                              'CONDDB',
#                              'ExtraPackages',
#                              'Visible',
#                              'ProcessingPass',
#                              'Usable',
#                              'DQTag',
#                              'OptionsFormat',
#                              'isMulticore',
#                              'SystemConfig',
#                              'mcTCK',
#                              'RuntimeProjects']))

#   def test_Steps(self):
#     """
#     insert a new step
#     """
#     paramNames = ['StepId', 'StepName', 'ApplicationName', 'ApplicationVersion',
#                   'OptionFiles', 'DDDB', 'CONDDB', 'ExtraPackages', 'Visible',
#                   'ProcessingPass', 'Usable', 'DQTag', 'OptionsFormat', 'isMulticore',
#                   'SystemConfig', 'mcTCK', 'RuntimeProjects']

#     step1 = {'Step': {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': '',
#                       'ApplicationVersion': 'v29r1',
#                       'ext-comp-1273': 'CHARM.MDST (Charm micro dst)', 'ExtraPackages': '',
#                       'StepName': 'davinci prb2',
#                       'ProcessingPass': 'WG-Coool', 'ext-comp-1264': 'CHARM.DST (Charm stream)',
#                       'Visible': 'Y', 'DDDB': '', 'OptionFiles': '',
#                       'CONDDB': '', 'DQTag': 'OK', 'OptionsFormat': 'txt',
#                       'isMulticore': 'N', 'SystemConfig': 'centos7', 'mcTCK': 'ax1'},
#              'OutputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.MDST'}],
#              'InputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.DST'}]}

#     retVal = self.bk.insertStep(step1)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value'] > 0)
#     stepid1 = retVal['Value']

#     step2 = {'Step': {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': '',
#                       'ApplicationVersion': 'v29r1',
#                       'ext-comp-1273': 'CHARM.MDST (Charm micro dst)', 'ExtraPackages': '',
#                       'StepName': 'davinci prb2',
#                       'ProcessingPass': 'WG-Coool', 'ext-comp-1264': 'CHARM.DST (Charm stream)',
#                       'Visible': 'Y', 'DDDB': '',
#                       'OptionFiles': '', 'CONDDB': '', 'DQTag': 'OK', 'OptionsFormat': 'txt',
#                       'isMulticore': 'N', 'SystemConfig': 'centos7', 'mcTCK': 'ax1'},
#              'OutputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.MDST'}],
#              'InputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.DST'}],
#              'RuntimeProjects': [{'StepId': stepid1}]}

#     retVal = self.bk.insertStep(step2)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value'] > 0)
#     stepid2 = retVal['Value']

#     retVal = self.bk.getAvailableSteps({"StepId": stepid1})
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(paramNames))

#     self.assertEqual(len(retVal['Value']['Records']), 1)
#     record = dict(zip(paramNames[1:-1], retVal['Value']['Records'][0][1:-1]))
#     for stepParams in paramNames[1:-1]:
#       if step1['Step'][stepParams]:
#         self.assertEqual(record[stepParams], step1['Step'][stepParams])
#       else:
#         self.assertEqual(record[stepParams], None)

#     retVal = self.bk.getAvailableSteps({"StepId": stepid2})
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(len(retVal['Value']['Records']), 1)
#     self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(paramNames))
#     record = dict(zip(paramNames[1:-1], retVal['Value']['Records'][0][1:-1]))
#     for stepParams in paramNames[1:-1]:
#       if step2['Step'][stepParams]:
#         self.assertEqual(record[stepParams], step2['Step'][stepParams])
#       else:
#         self.assertEqual(record[stepParams], None)

#     retVal = self.bk.getStepInputFiles(stepid1)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(['FileType', 'Visible']))

#     retVal = self.bk.getStepOutputFiles(stepid1)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(['FileType', 'Visible']))

#     retVal = self.bk.setStepInputFiles(stepid1, [{"FileType": "Test.DST", "Visible": "Y"}])
#     self.assertTrue(retVal['OK'])  # make sure the change works
#     retVal = self.bk.getStepInputFiles(stepid1)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(['FileType', 'Visible']))
#     self.assertEqual(retVal['Value']['Records'], [['Test.DST', 'Y']])

#     retVal = self.bk.setStepOutputFiles(stepid2, [{"FileType": "Test.DST", "Visible": "Y"}])
#     self.assertTrue(retVal['OK'])  # make sure the change works
#     retVal = self.bk.getStepOutputFiles(stepid2)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(['FileType', 'Visible']))
#     self.assertEqual(retVal['Value']['Records'], [['Test.DST', 'Y']])

#     step1['Step']['StepName'] = 'test1'
#     step1['Step']['StepId'] = stepid1
#     retVal = self.bk.updateStep({'StepId': stepid1, 'StepName': 'test1'})
#     self.assertTrue(retVal['OK'])
#     # check after the modification
#     retVal = self.bk.getAvailableSteps({"StepId": stepid1})
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(paramNames))
#     self.assertEqual(len(retVal['Value']['Records']), 1)
#     record = dict(zip(paramNames[1:-1], retVal['Value']['Records'][0][1:-1]))
#     for stepParams in paramNames[1:-1]:
#       if step1['Step'][stepParams]:
#         self.assertEqual(record[stepParams], step1['Step'][stepParams])
#       else:
#         self.assertEqual(record[stepParams], None)

#     # at the end delete the steps
#     retVal = self.bk.deleteStep(stepid2)
#     self.assertTrue(retVal['OK'])

#     retVal = self.bk.deleteStep(stepid1)
#     self.assertTrue(retVal['OK'])

#   def test_getAvailableConfigNames(self):
#     """
#     Must have one configuration name
#     """
#     retVal = self.bk.getAvailableConfigNames()
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)

#   def test_getConfigVersions(self):
#     """
#     The bookkeeping view is isued, we can not use the newly inserted configuration name: Test
#     """
#     retVal = self.bk.getConfigVersions({"ConfigName": "MC"})
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertEqual(retVal['Value']['TotalRecords'], 14)

#   def test_getConditions(self):
#     """

#     Get the available configurations for a given bk dict

#     """
#     simParams = [
#         'SimId',
#         'Description',
#         'BeamCondition',
#         'BeamEnergy',
#         'Generator',
#         'MagneticField',
#         'DetectorCondition',
#         'Luminosity',
#         'G4settings']
#     dataParams = ['DaqperiodId', 'Description', 'BeamCondition', 'BeanEnergy', 'MagneticField', 'VELO',
#                   'IT', 'TT', 'OT', 'RICH1', 'RICH2', 'SPD_PRS', 'ECAL', 'HCAL', 'MUON', 'L0', 'HLT', 'VeloPosition']

#     retVal = self.bk.getConditions({"ConfigName": "MC", "ConfigVersion": "2012"})
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertEqual(len(retVal['Value']), 2)
#     self.assertEqual(retVal['Value'][1]['TotalRecords'], 0)
#     self.assertEqual(retVal['Value'][1]['ParameterNames'], dataParams)
#     self.assertTrue(retVal['Value'][0]['TotalRecords'] > 0)
#     self.assertEqual(retVal['Value'][0]['ParameterNames'], simParams)

#   def test_getProcessingPass(self):
#     """

#     Check the available processing passes for a given bk path. Again bkk view...

#     """

#     retVal = self.bk.getProcessingPass({"ConfigName": "MC", "ConfigVersion": "2012", })
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertEqual(len(retVal['Value']), 2)
#     self.assertTrue(retVal['Value'][0]['TotalRecords'] > 0)

#   def test_bookkeepingtree(self):
#     """
#     Browse the bookkeeping database
#     """
#     bkQuery = {"ConfigName": "MC"}
#     retVal = self.bk.getAvailableConfigNames()
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertTrue(bkQuery['ConfigName'] in [cName[0] for cName in retVal['Value']['Records']])

#     retVal = self.bk.getConfigVersions({"ConfigName": "MC"})
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertEqual(retVal['Value']['TotalRecords'], 14)

#     retVal = self.bk.getConditions({"ConfigName": "MC",
#                                     "ConfigVersion": "2012"})
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertEqual(retVal['Value'][0]['TotalRecords'], 2)

#     retVal = self.bk.getProcessingPass({"ConfigName": "MC",
#                                         "ConfigVersion": "2012",
#                                         "ConditionDescription": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"})
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertEqual(retVal['Value'][0]['Records'][0][0], "Sim08a")

#     retVal = self.bk.getProcessingPass({"ConfigName": "MC",
#                                         "ConfigVersion": "2012",
#                                         "ConditionDescription": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"},
#                                        "/Sim08a")
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertEqual(retVal['Value'][0]['Records'][0][0], "Digi13")

#     retVal = self.bk.getProcessingPass({"ConfigName": "MC",
#                                         "ConfigVersion": "2012",
#                                         "ConditionDescription": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"},
#                                        "/Sim08a/Digi13")
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertEqual(retVal['Value'][0]['Records'][0][0], "Trig0x409f0045")

#     retVal = self.bk.getProcessingPass({"ConfigName": "MC",
#                                         "ConfigVersion": "2012",
#                                         "ConditionDescription": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"},
#                                        "/Sim08a/Digi13/Trig0x409f0045")
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertEqual(retVal['Value'][0]['Records'][0][0], "Reco14a")

#     retVal = self.bk.getProcessingPass({"ConfigName": "MC",
#                                         "ConfigVersion": "2012",
#                                         "ConditionDescription": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"},
#                                        "/Sim08a/Digi13/Trig0x409f0045/Reco14a")
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertEqual(retVal['Value'][0]['Records'][0][0], "Stripping20NoPrescalingFlagged")

#     retVal = self.bk.getProcessingPass({"ConfigName": "MC",
#                                         "ConfigVersion": "2012",
#                                         "ConditionDescription": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8"},
#                                        "/Sim08a/Digi13/Trig0x409f0045/Reco14a/Stripping20NoPrescalingFlagged")
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertEqual(retVal['Value'][1]['Records'][0][0], 12442001)

#     retVal = self.bk.getFileTypes({
#         "ConfigName": "MC",
#         "ConfigVersion": "2012",
#         "ConditionDescription": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8",
#         "ProcessingPass": "/Sim08a/Digi13/Trig0x409f0045/Reco14a/Stripping20NoPrescalingFlagged"})

#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(retVal['Value']['Records'][0][0], 'ALLSTREAMS.DST')

#     retVal = self.bk.getFiles({
#         "ConfigName": "MC",
#         "ConfigVersion": "2012",
#         "SimulationConditions": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8",
#         "ProcessingPass": "/Sim08a/Digi13/Trig0x409f0045/Reco14a/Stripping20NoPrescalingFlagged",
#         "FileType": "ALLSTREAMS.DST"})

#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 10)

#     retVal = self.bk.getFiles({
#         "ConfigName": "MC",
#         "ConfigVersion": "2012",
#         "SimulationConditions": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8",
#         "ProcessingPass": "/Sim08a/Digi13/Trig0x409f0045/Reco14a/Stripping20NoPrescalingFlagged",
#         "FileType": "ALLSTREAMS.DST",
#         "EventType": 12442001})

#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 10)

#     retVal = self.bk.getFiles({
#         "ConfigName": "MC",
#         "ConfigVersion": "2012",
#         "SimulationConditions": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8",
#         "ProcessingPass": "/Sim08a/Digi13/Trig0x409f0045/Reco14a/Stripping20NoPrescalingFlagged",
#         "FileType": "ALLSTREAMS.DST",
#         "EventType": 12442001,
#         "Visible": "N"})
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 0)

#     retVal = self.bk.getFiles({
#         "ConfigName": "MC",
#         "ConfigVersion": "2012",
#         "SimulationConditions": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8",
#         "ProcessingPass": "/Sim08a/Digi13/Trig0x409f0045/Reco14a/Stripping20NoPrescalingFlagged",
#         "FileType": "ALLSTREAMS.DST",
#         "EventType": 12442001,
#         "Visible": "All"})
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 10)

#     retVal = self.bk.getFiles({
#         "ConfigName": "MC",
#         "ConfigVersion": "2012",
#         "SimulationConditions": "Beam4000GeV-2012-MagUp-Nu2.5-Pythia8",
#         "ProcessingPass": "/Sim08a/Digi13/Trig0x409f0045/Reco14a/Stripping20NoPrescalingFlagged",
#         "FileType": "ALLSTREAMS.DST",
#         "EventType": 12442001,
#         "Visible": "All",
#         'NbOfEvents': True})
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value'], [180])

#   def test_getFiles1(self):
#     """
#     This is used to test the getFiles method.
#     """
#     bkQuery = {'ConfigName': 'MC',
#                'SimulationConditions': 'Beam3500GeV-May2010-MagOff-Fix1',
#                'EventType': '30000000',
#                'FileType': 'DST',
#                'ProcessingPass': '/Sim01/Reco08',
#                'Visible': 'Y',
#                'ConfigVersion': 'MC10',
#                'DataQuality': 'ALL'}

#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 301)

#     bkQuery = {'ConfigName': 'MC',
#                'SimulationConditions': 'Beam3500GeV-May2010-MagOff-Fix1',
#                'EventType': '30000000',
#                'FileType': 'DST',
#                'ProcessingPass': '/Sim01/Reco08',
#                'Visible': 'Y',
#                'ConfigVersion': 'MC10',
#                'DataQuality': "ALL"}

#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 301)

#     bkQuery['NbOfEvents'] = True
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value'], [6020000])

#   def test_getFiles2(self):
#     """
#     It is used to test the getFiles method
#     """
#     bkQuery = {'Production': 10917,
#                'EventType': '30000000',
#                'FileType': 'DST',
#                'Visible': 'Y',
#                'DataQuality': 'OK'}

#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 301)

#     bkQuery.pop('Visible')
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 301)

#     bkQuery['NbOfEvents'] = True
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value'], [6020000])

#   def test_getFiles3(self):
#     """
#     It is used to test the getFiles method
#     """
#     bkQuery = {'ConfigName': 'MC',
#                'SimulationConditions': 'Beam3500GeV-Oct2010-MagUp-Nu2.5',
#                'EventType': 28144012,
#                'FileType': 'XGEN',
#                'ProcessingPass': '/MC10Gen01',
#                'Visible': 'Y',
#                'ConfigVersion': 'MC10',
#                'DataQuality': 'ALL'}
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 100)

#     bkQuery['NbOfEvents'] = True
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value'], [1000000])

#   def test_getFiles4(self):

#     bkQuery = {'ConfigName': 'MC',
#                'SimulationConditions': 'Beam3500GeV-May2010-MagDown-Fix1',
#                'ProcessingPass': '/Sim01/Trig0x002e002aFlagged/Reco08-MINBIAS',
#                'Visible': 'N',
#                'ConfigVersion': 'MC10'}

#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 393)

#     bkQuery['Visible'] = 'Y'
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 707)

#     retVal = self.bk.getFileTypes(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(retVal['Value']['Records'][0][0], 'DST')

#     bkQuery['NbOfEvents'] = True
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value'], [35002426])

#   def test_getProductions2(self):
#     bkQuery = {'ConfigName': 'MC',
#                'ConditionDescription': 'Beam3500GeV-May2010-MagDown-Fix1',
#                'ProcessingPass': '/Sim01/Trig0x002e002aFlagged/Reco08-MINBIAS',
#                'Visible': 'N',
#                'ConfigVersion': 'MC10'}

#     retVal = self.bk.getProductions(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(retVal['Value']['Records'][0][0], 10713)

#     bkQuery['Visible'] = 'Y'
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(retVal['Value']['Records'][0][0], 10713)

#   def test_getFile5(self):
#     """
#     Get list of file types
#     """
#     bkQuery = {'ConfigName': 'MC',
#                'SimulationConditions': 'Beam3500GeV-Oct2010-MagUp-Nu2.5',
#                'EventType': 28144012,
#                'ProcessingPass': '/MC10Gen01',
#                'Visible': 'Y',
#                'ConfigVersion': 'MC10',
#                'DataQuality': 'ALL'}

#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 100)

#   def test_getProductions1(self):
#     """
#     This is used to test the getFiles method.
#     """
#     bkQuery = {'ConfigName': 'MC',
#                'ConditionDescription': 'Beam3500GeV-May2010-MagOff-Fix1',
#                'EventType': '30000000',
#                'FileType': 'DST',
#                'ProcessingPass': '/Sim01/Reco08',
#                'Visible': 'Y',
#                'ConfigVersion': 'MC10',
#                'DataQuality': 'ALL'}

#     retVal = self.bk.getProductions(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(retVal['Value']['Records'][0][0], 10917)

#   def test_getVisibleFilesWithMetadata1(self):
#     """
#     This si used to test the ganga queries
#     """

#     bkQuery = {'ConfigName': 'LHCb',
#                'ConfigVesrion': 'Collision12',
#                'ProcessingPass': '/Real Data/Reco13a/Stripping19a',
#                'FileType': 'BHADRON.MDST',
#                'Visible': 'Y',
#                'EventType': 90000000,
#                'DataTakingConditions': 'Beam4000GeV-VeloClosed-MagDown',
#                'DataQuality': 'OK'}

#     summary = {'EventInputStat': 56223169,
#                'FileSize': 783.777593157,
#                'InstLuminosity': 0,
#                'Luminosity': 156438151.738,
#                'Number Of Files': 439,
#                'Number of Events': 68170375,
#                'TotalLuminosity': 0}

#     paramNames = [
#         'TotalLuminosity',
#         'Luminosity',
#         'Fillnumber',
#         'EventInputStat',
#         'FileSize',
#         'EventStat',
#         'Runnumber',
#         'InstLuminosity',
#         'TCK']

#     retVal = self.bk.getVisibleFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['Summary'])
#     self.assertTrue(retVal['Value']['LFNs'])

#     self.assertEqual(retVal['Value']['Summary'], summary)
#     self.assertEqual(len(retVal['Value']['LFNs']), 439)
#     self.assertEqual(retVal['Value']['LFNs'][retVal['Value']['LFNs'].keys()[0]].keys(), paramNames)

#     retVal = self.bk.getVisibleFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['Summary'])
#     self.assertTrue(retVal['Value']['LFNs'])

#     self.assertEqual(retVal['Value']['Summary'], summary)
#     self.assertEqual(len(retVal['Value']['LFNs']), 439)

#     bkQuery['RunNumber'] = [115055],
#     retVal = self.bk.getVisibleFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['Summary'])
#     self.assertTrue(retVal['Value']['LFNs'])

#     self.assertEqual(retVal['Value']['Summary'], summary)
#     self.assertEqual(len(retVal['Value']['LFNs']), 439)

#     bkQuery['TCK'] = ['0x95003d']
#     retVal = self.bk.getVisibleFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['Summary'])
#     self.assertTrue(retVal['Value']['LFNs'])
#     self.assertEqual(retVal['Value']['Summary'], summary)
#     self.assertEqual(len(retVal['Value']['LFNs']), 439)

#   def test_getVisibleFilesWithMetadata2(self):
#     """
#     This si used to test the ganga queries
#     Now test the run numbers
#     """

#     bkQuery = {'ConfigName': 'LHCb',
#                'DataTakingConditions': 'Beam3500GeV-VeloClosed-MagDown',
#                'EventType': 90000000,
#                'FileType': 'EW.DST',
#                'ProcessingPass': '/Real Data/Reco10/Stripping13b',
#                'Visible': 'Y',
#                'ConfigVersion': 'Collision11',
#                'DataQuality': 'ALL'}

#     summary = {'TotalLuminosity': 0,
#                'Number Of Files': 2316,
#                'Luminosity': 175005777.674,
#                'Number of Events': 69545408,
#                'EventInputStat': 2177589640,
#                'FileSize': 6315.58588745,
#                'InstLuminosity': 0}

#     paramNames = [
#         'TotalLuminosity',
#         'Luminosity',
#         'Fillnumber',
#         'EventInputStat',
#         'FileSize',
#         'EventStat',
#         'Runnumber',
#         'InstLuminosity',
#         'TCK']

#     retVal = self.bk.getVisibleFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['Summary'])
#     self.assertTrue(retVal['Value']['LFNs'])

#     self.assertEqual(retVal['Value']['Summary'], summary)
#     self.assertEqual(len(retVal['Value']['LFNs']), 2316)
#     self.assertEqual(retVal['Value']['LFNs'][retVal['Value']['LFNs'].keys()[0]].keys(), paramNames)

#     bkQuery['DataQuality'] = 'OK'
#     summary = {'EventInputStat': 2177397109,
#                'FileSize': 6315.18232392,
#                'InstLuminosity': 0,
#                'Luminosity': 174996481.488,
#                'Number Of Files': 2314,
#                'Number of Events': 69542587,
#                'TotalLuminosity': 0}
#     retVal = self.bk.getVisibleFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['Summary'])
#     self.assertTrue(retVal['Value']['LFNs'])

#     self.assertEqual(retVal['Value']['Summary'], summary)
#     self.assertEqual(len(retVal['Value']['LFNs']), 2314)
#     self.assertEqual(retVal['Value']['LFNs'][retVal['Value']['LFNs'].keys()[0]].keys(), paramNames)

#     bkQuery['RunNumber'] = [90104, 92048, 87851]
#     summary = {'TotalLuminosity': 0,
#                'Number Of Files': 2,
#                'Luminosity': 57308.7467796,
#                'Number of Events': 17852,
#                'EventInputStat': 889893,
#                'FileSize': 2.148836844,
#                'InstLuminosity': 0}

#     retVal = self.bk.getVisibleFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['Summary'])
#     self.assertTrue(retVal['Value']['LFNs'])

#     self.assertEqual(retVal['Value']['Summary'], summary)
#     self.assertEqual(len(retVal['Value']['LFNs']), 2)

#     bkQuery.pop('RunNumber')
#     bkQuery['StartRun'] = 93102
#     bkQuery['EndRun'] = 93407
#     summary = {'TotalLuminosity': 0,
#                'Number Of Files': 178,
#                'Luminosity': 19492211.2845,
#                'Number of Events': 8688923,
#                'EventInputStat': 222920612,
#                'FileSize': 740.377729246,
#                'InstLuminosity': 0}

#     retVal = self.bk.getVisibleFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['Summary'])
#     self.assertTrue(retVal['Value']['LFNs'])
#     self.assertEqual(retVal['Value']['Summary'], summary)
#     self.assertEqual(len(retVal['Value']['LFNs']), 178)

#     bkQuery.pop('StartRun')
#     bkQuery.pop('EndRun')
#     bkQuery['StartDate'] = "2011-06-15 19:15:25"
#     summary = {'EventInputStat': 135629659,
#                'FileSize': 449.439082551,
#                'InstLuminosity': 0,
#                'Luminosity': 11832088.6133,
#                'Number Of Files': 125,
#                'Number of Events': 5226863,
#                'TotalLuminosity': 0}

#     retVal = self.bk.getVisibleFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['Summary'])
#     self.assertTrue(retVal['Value']['LFNs'])
#     self.assertEqual(retVal['Value']['Summary'], summary)
#     self.assertEqual(len(retVal['Value']['LFNs']), 125)

#     bkQuery['EndDate'] = "2011-06-16 19:15:25"
#     summary = {'TotalLuminosity': 0,
#                'Number Of Files': 125,
#                'Luminosity': 11832088.6133,
#                'Number of Events': 5226863,
#                'EventInputStat': 135629659,
#                'FileSize': 449.439082551,
#                'InstLuminosity': 0}
#     retVal = self.bk.getVisibleFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['Summary'])
#     self.assertTrue(retVal['Value']['LFNs'])
#     self.assertEqual(retVal['Value']['Summary'], summary)
#     self.assertEqual(len(retVal['Value']['LFNs']), 125)

#   def test_getVisibleFilesWithMetadata3(self):
#     """
#     This si used to test the ganga queries
#     Now test the MC datasets
#     """
#     bkQuery = {'ConfigName': 'MC',
#                'SimulationConditions': 'Beam3500GeV-May2010-MagOff-Fix1',
#                'EventType': '30000000',
#                'FileType': 'DST',
#                'ProcessingPass': '/Sim01/Reco08',
#                'Visible': 'Y',
#                'ConfigVersion': 'MC10',
#                'DataQuality': ['OK']}

#     summary = {'EventInputStat': 6020000,
#                'FileSize': 468.227136723,
#                'InstLuminosity': 0,
#                'Luminosity': 0,
#                'Number Of Files': 301,
#                'Number of Events': 6020000,
#                'TotalLuminosity': 0}
#     retVal = self.bk.getVisibleFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['Summary'])
#     self.assertTrue(retVal['Value']['LFNs'])
#     self.assertEqual(retVal['Value']['Summary'], summary)
#     self.assertEqual(len(retVal['Value']['LFNs']), 301)

#   def test_getFiles6(self):
#     """
#     This is used to test the getFiles
#     """
#     bkQuery = {'ConfigName': 'LHCb',
#                'ConfigVesrion': 'Collision12',
#                'ProcessingPass': '/Real Data/Reco13a/Stripping19a',
#                'FileType': 'BHADRON.MDST',
#                'Visible': 'Y',
#                'EventType': 90000000,
#                'DataTakingConditions': 'Beam4000GeV-VeloClosed-MagDown',
#                'DataQuality': 'OK'}

#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 439)

#     bkQuery['NbOfEvents'] = True
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value'], [68170375])
#     bkQuery.pop('NbOfEvents')

#     bkQuery['RunNumber'] = [115055]
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 439)

#     bkQuery['TCK'] = ['0x95003d']
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 439)

#   def test_getFiles7(self):
#     """
#     This si used to test the ganga queries
#     Now test the run numbers
#     """
#     bkQuery = {'ConfigName': 'LHCb',
#                'DataTakingConditions': 'Beam3500GeV-VeloClosed-MagDown',
#                'EventType': 90000000,
#                'FileType': 'EW.DST',
#                'ProcessingPass': '/Real Data/Reco10/Stripping13b',
#                'Visible': 'Y',
#                'ConfigVersion': 'Collision11',
#                'DataQuality': ['OK']}

#     retVal = self.bk.getFiles(bkQuery)
#     self.assertEqual(len(retVal['Value']), 2314)

#     bkQuery['JobStartDate'] = "2011-06-03 01:14:00"
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertEqual(len(retVal['Value']), 2049)

#     bkQuery['JobEndDate'] = "2011-06-03 23:00:00"
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertEqual(len(retVal['Value']), 31)
#     bkQuery.pop('JobStartDate')
#     bkQuery.pop('JobEndDate')

#     bkQuery['NbOfEvents'] = True
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertEqual(retVal['Value'], [69542587])
#     bkQuery.pop('NbOfEvents')

#     bkQuery['DataQuality'] = 'ALL'
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertEqual(len(retVal['Value']), 2316)

#     bkQuery['RunNumber'] = [90104, 92048, 87851]
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertEqual(len(retVal['Value']), 2)

#     bkQuery.pop('RunNumber')
#     bkQuery['StartRun'] = 93102
#     bkQuery['EndRun'] = 93407

#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 178)

#     bkQuery.pop('StartRun')
#     bkQuery.pop('EndRun')
#     bkQuery['StartDate'] = "2011-06-15 19:15:25"

#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 127)

#     bkQuery['EndDate'] = "2011-06-16 19:15:25"
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 125)

#     bkQuery['EventType'] = [90000000, 91000000]
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 125)

#   def test_getFiles8(self):
#     """
#     This is used to test the ganga queries
#     Now test the MC datasets
#     """
#     bkQuery = {'ConfigName': 'MC',
#                'SimulationConditions': 'Beam3500GeV-May2010-MagOff-Fix1',
#                'EventType': '30000000',
#                'FileType': 'DST',
#                'ProcessingPass': '/Sim01/Reco08',
#                'Visible': 'Y',
#                'ConfigVersion': 'MC10',
#                'DataQuality': ['OK']}

#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 301)

#   def getFilesWithMetadata1(self):
#     """
#     This is used to test the getFiles
#     """
#     bkQuery = {'ConfigName': 'LHCb',
#                'ConfigVesrion': 'Collision12',
#                'ProcessingPass': '/Real Data/Reco13a/Stripping19a',
#                'FileType': 'BHADRON.MDST',
#                'Visible': 'Y',
#                'EventType': 90000000,
#                'ConditionDescription': 'Beam4000GeV-VeloClosed-MagDown',
#                'DataQuality': 'OK'}

#     paramNames = [
#         'FileName',
#         'EventStat',
#         'FileSize',
#         'CreationDate',
#         'JobStart',
#         'JobEnd',
#         'WorkerNode',
#         'FileType',
#         'RunNumber',
#         'FillNumber',
#         'FullStat',
#         'DataqualityFlag',
#         'EventInputStat',
#         'TotalLuminosity',
#         'Luminosity',
#         'InstLuminosity',
#         'TCK',
#         'GUID',
#         'ADLER32',
#         'EventType',
#         'MD5SUM',
#         'VisibilityFlag',
#         'JobId',
#         'GotReplica',
#         'InsertTimeStamp']
#     retVal = self.bk.getFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertEqual(retVal['Value']['TotalRecords'], 439)
#     self.assertEqual(len(retVal['Value']['Records']), 439)
#     self.assertEqual(retVal['Value']['ParameterNames'], paramNames)

#     # now test the data datakingcondition
#     bkQuery.pop('ConditionDescription')
#     bkQuery['DataTakingConditions'] = 'Beam4000GeV-VeloClosed-MagDown'
#     retVal = self.bk.getFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertEqual(retVal['Value']['TotalRecords'], 439)
#     self.assertEqual(len(retVal['Value']['Records']), 439)
#     self.assertEqual(retVal['Value']['ParameterNames'], paramNames)

#     bkQuery['RunNumber'] = [115055]
#     retVal = self.bk.getFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertEqual(retVal['Value']['TotalRecords'], 439)
#     self.assertEqual(len(retVal['Value']['Records']), 439)
#     self.assertEqual(len(retVal['Value']['ParameterNames']), paramNames)

#     bkQuery['TCK'] = ['0x95003d']
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertEqual(retVal['Value']['TotalRecords'], 439)
#     self.assertEqual(len(retVal['Value']['Records']), 439)
#     self.assertEqual(retVal['Value']['ParameterNames'], paramNames)

#   def test_getFilesWithMetadata2(self):
#     """
#     This si used to test the ganga queries
#     Now test the run numbers
#     """

#     bkQuery = {'ConfigName': 'LHCb',
#                'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
#                'EventType': 90000000,
#                'FileType': 'EW.DST',
#                'ProcessingPass': '/Real Data/Reco10/Stripping13b',
#                'Visible': 'Y',
#                'ConfigVersion': 'Collision11',
#                'DataQuality': ['OK']}

#     paramNames = [
#         'FileName',
#         'EventStat',
#         'FileSize',
#         'CreationDate',
#         'JobStart',
#         'JobEnd',
#         'WorkerNode',
#         'FileType',
#         'RunNumber',
#         'FillNumber',
#         'FullStat',
#         'DataqualityFlag',
#         'EventInputStat',
#         'TotalLuminosity',
#         'Luminosity',
#         'InstLuminosity',
#         'TCK',
#         'GUID',
#         'ADLER32',
#         'EventType',
#         'MD5SUM',
#         'VisibilityFlag',
#         'JobId',
#         'GotReplica',
#         'InsertTimeStamp']

#     retVal = self.bk.getFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertEqual(retVal['Value']['TotalRecords'], 2314)
#     self.assertEqual(len(retVal['Value']['Records']), 2314)
#     self.assertEqual(retVal['Value']['ParameterNames'], paramNames)

#     bkQuery['JobStartDate'] = "2011-06-03 01:14:00"
#     retVal = self.bk.getFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertEqual(retVal['Value']['TotalRecords'], 2049)
#     self.assertEqual(len(retVal['Value']['Records']), 2049)
#     self.assertEqual(retVal['Value']['ParameterNames'], paramNames)

#     bkQuery['JobEndDate'] = "2011-06-03 23:00:00"
#     retVal = self.bk.getFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)
#     self.assertEqual(retVal['Value']['TotalRecords'], 31)
#     self.assertEqual(len(retVal['Value']['Records']), 31)
#     self.assertEqual(retVal['Value']['ParameterNames'], paramNames)
#     bkQuery.pop('JobStartDate')
#     bkQuery.pop('JobEndDate')

#     bkQuery['RunNumber'] = [90104, 92048, 87851]
#     retVal = self.bk.getFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 2)
#     self.assertEqual(len(retVal['Value']['Records']), 2)
#     self.assertEqual(retVal['Value']['ParameterNames'], paramNames)

#     bkQuery.pop('RunNumber')
#     bkQuery['StartRun'] = 93102
#     bkQuery['EndRun'] = 93407

#     retVal = self.bk.getFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 178)
#     self.assertEqual(len(retVal['Value']['Records']), 178)
#     self.assertEqual(retVal['Value']['ParameterNames'], paramNames)

#     bkQuery.pop('StartRun')
#     bkQuery.pop('EndRun')
#     bkQuery['StartDate'] = "2011-06-15 19:15:25"

#     retVal = self.bk.getFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 125)
#     self.assertEqual(len(retVal['Value']['Records']), 125)
#     self.assertEqual(retVal['Value']['ParameterNames'], paramNames)

#     bkQuery['EndDate'] = "2011-06-16 19:15:25"
#     retVal = self.bk.getFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 125)
#     self.assertEqual(len(retVal['Value']['Records']), 125)
#     self.assertEqual(retVal['Value']['ParameterNames'], paramNames)

#   def test_getFilesWithMetadata3(self):
#     """
#     This is used to test the ganga queries
#     Now test the MC datasets
#     """
#     bkQuery = {'ConfigName': 'MC',
#                'ConditionDescription': 'Beam3500GeV-May2010-MagOff-Fix1',
#                'EventType': '30000000',
#                'FileType': 'DST',
#                'ProcessingPass': '/Sim01/Reco08',
#                'Visible': 'Y',
#                'ConfigVersion': 'MC10',
#                'DataQuality': ['OK']}

#     paramNames = [
#         'FileName',
#         'EventStat',
#         'FileSize',
#         'CreationDate',
#         'JobStart',
#         'JobEnd',
#         'WorkerNode',
#         'FileType',
#         'RunNumber',
#         'FillNumber',
#         'FullStat',
#         'DataqualityFlag',
#         'EventInputStat',
#         'TotalLuminosity',
#         'Luminosity',
#         'InstLuminosity',
#         'TCK',
#         'GUID',
#         'ADLER32',
#         'EventType',
#         'MD5SUM',
#         'VisibilityFlag',
#         'JobId',
#         'GotReplica',
#         'InsertTimeStamp']

#     retVal = self.bk.getFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 301)
#     self.assertEqual(len(retVal['Value']['Records']), 301)
#     self.assertEqual(retVal['Value']['ParameterNames'], paramNames)


# class TestDestoryDataset(BaseTestCase):
#   """
#   clean the db contetnt
#   """

#   def test_destroyDataset(self):
#     """
#     after the test the data will be destroyed
#     """
#     retVal = self.bk.deleteCertificationData()
#     self.assertTrue(retVal['OK'])


# class MCProductionTest (MCXMLReportInsert):

#   """
#   Test the existence of the inserted data.
#   """

#   def test_getFileTypeVersion(self):
#     """
#     test the file type version
#     """
#     retVal = self.bk.getFileTypeVersion(['/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_8.digi',
#                                          '/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_7.digi'])
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_8.digi'])
#     self.assertEqual(retVal['Value']['/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_8.digi'], 'ROOT')

#     self.assertTrue(retVal['Value']['/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_7.digi'])
#     self.assertEqual(retVal['Value']['/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_7.digi'], 'ROOT')

#   def test_getProductionOutputFileTypes1(self):
#     """test the visibility of the file types for a given production
#     """
#     retVal = self.bk.getProductionOutputFileTypes({"Production": self.production})
#     self.assertTrue(retVal['OK'])
#     self.assertDictEqual(retVal['Value'], {'DIGI': 'N', 'SIM': 'Y'})

#     retVal = self.bk.getAvailableSteps({'StepName': 'Cert-Sim09b - 2012 - MU - Pythia8'})
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']['Records']) > 0)
#     stepid = retVal['Value']['Records'][0][0]
#     retVal = self.bk.getProductionOutputFileTypes({"Production": self.production, "StepId": stepid})
#     self.assertTrue(retVal['OK'])
#     self.assertDictEqual(retVal['Value'], {'SIM': 'Y'})

#     retVal = self.bk.getAvailableSteps({'StepName': 'Cert-Digi14a for 2012 (to use w Sim09)'})
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']['Records']) > 0)
#     stepid = retVal['Value']['Records'][0][0]
#     retVal = self.bk.getProductionOutputFileTypes({"Production": self.production, "StepId": stepid})
#     self.assertTrue(retVal['OK'])
#     self.assertDictEqual(retVal['Value'], {'DIGI': 'N'})

#     retVal = self.bk.getAvailableSteps({'StepName': 'Cert-L0 emulation - TCK 003d'})
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']['Records']) > 0)
#     stepid = retVal['Value']['Records'][0][0]
#     retVal = self.bk.getProductionOutputFileTypes({"Production": self.production, "StepId": stepid})
#     self.assertTrue(retVal['OK'])
#     self.assertDictEqual(retVal['Value'], {'DIGI': 'N'})

#     retVal = self.bk.getAvailableSteps(
#         {'StepName': 'Cert-TCK-0x4097003d Flagged MC - 2012 - to be used in multipleTCKs'})
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']['Records']) > 0)
#     stepid = retVal['Value']['Records'][0][0]
#     retVal = self.bk.getProductionOutputFileTypes({"Production": self.production, "StepId": stepid})
#     self.assertTrue(retVal['OK'])
#     self.assertDictEqual(retVal['Value'], {'DIGI': 'N'})

#     retVal = self.bk.getAvailableSteps({'StepName': 'Cert-Move TCK-0x4097003d from default location'})
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']['Records']) > 0)
#     stepid = retVal['Value']['Records'][0][0]
#     retVal = self.bk.getProductionOutputFileTypes({"Production": self.production, "StepId": stepid})
#     self.assertTrue(retVal['OK'])
#     self.assertDictEqual(retVal['Value'], {'DIGI': 'N'})

#     retVal = self.bk.getAvailableSteps({'StepName': 'Cert-L0 emulation - TCK 0042'})
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']['Records']) > 0)
#     stepid = retVal['Value']['Records'][0][0]
#     retVal = self.bk.getProductionOutputFileTypes({"Production": self.production, "StepId": stepid})
#     self.assertTrue(retVal['OK'])
#     self.assertDictEqual(retVal['Value'], {'DIGI': 'N'})

#     retVal = self.bk.getAvailableSteps(
#         {'StepName': 'Cert-TCK-0x40990042 Flagged MC - 2012 - to be used in multipleTCKs'})
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']['Records']) > 0)
#     stepid = retVal['Value']['Records'][0][0]
#     retVal = self.bk.getProductionOutputFileTypes({"Production": self.production, "StepId": stepid})
#     self.assertTrue(retVal['OK'])
#     self.assertDictEqual(retVal['Value'], {'DIGI': 'N'})

#     retVal = self.bk.getAvailableSteps({'StepName': 'Cert-Move TCK-0x40990042 from default location'})
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']['Records']) > 0)
#     stepid = retVal['Value']['Records'][0][0]
#     retVal = self.bk.getProductionOutputFileTypes({"Production": self.production, "StepId": stepid})
#     self.assertTrue(retVal['OK'])
#     self.assertDictEqual(retVal['Value'], {'DIGI': 'N'})

#   def test_getProductionOutputFileTypes2(self):
#     """test the visibility of the file types for a given production
#     """

#     retVal = self.bk.getAvailableSteps({'StepName': 'Cert-Sim09b - 2012 - MU - Pythia8'})
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']['Records']) > 0)
#     retVal = self.bk.getProductionOutputFileTypes({"Production": 3, 'StepId': retVal['Value']['Records'][0][0]})
#     self.assertTrue(retVal['OK'])
#     self.assertDictEqual(retVal['Value'], {'SIM': 'N'})

#     retVal = self.bk.getAvailableSteps({'StepName': 'Cert-Digi14a for 2012 (to use w Sim09)'})
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']['Records']) > 0)
#     retVal = self.bk.getProductionOutputFileTypes({"Production": 3, 'StepId': retVal['Value']['Records'][0][0]})
#     self.assertTrue(retVal['OK'])
#     self.assertDictEqual(retVal['Value'], {'DIGI': 'N'})

#     retVal = self.bk.getAvailableSteps(
#         {'StepName': 'Cert-TCK-0x40990042 Flagged MC - 2012 - to be used in multipleTCKs'})
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']['Records']) > 0)
#     retVal = self.bk.getProductionOutputFileTypes({"Production": 3, 'StepId': retVal['Value']['Records'][0][0]})
#     self.assertTrue(retVal['OK'])
#     self.assertDictEqual(retVal['Value'], {'DIGI': 'Y', 'XDIGI': 'Y'})


# class TestBookkeepingUserInterface(MCInsertTestCase):
#   """
#   For testing the User Interface using the inserted data.
#   This must work using an empty database
#   """

#   def test_getFilesSummary(self):
#     bkQuery = {'ConfigName': 'test', 'ConfigVersion': 'Jenkins', 'Production': 2, 'Visible': 'N'}
#     retVal = self.bk.getFilesSummary(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['ParameterNames'])
#     self.assertTrue(retVal['Value']['Records'])
#     self.assertTrue(retVal['Value']['TotalRecords'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(
#         ['NbofFiles', 'NumberOfEvents', 'FileSize', 'Luminosity', 'InstLuminosity']))
#     self.assertEqual(retVal['Value']['Records'][0], [1, 411, 241904920, 0, 0])

#     bkQuery['ReplicaFlag'] = 'No'
#     retVal = self.bk.getFilesSummary(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['ParameterNames'])
#     self.assertTrue(retVal['Value']['Records'])
#     self.assertTrue(retVal['Value']['TotalRecords'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(
#         ['NbofFiles', 'NumberOfEvents', 'FileSize', 'Luminosity', 'InstLuminosity']))
#     self.assertEqual(retVal['Value']['Records'][0], [6, 2466, 988452372, 0, 0])

#     bkQuery['FileType'] = 'DIGI'
#     retVal = self.bk.getFilesSummary(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['ParameterNames'])
#     self.assertTrue(retVal['Value']['Records'])
#     self.assertTrue(retVal['Value']['TotalRecords'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(
#         ['NbofFiles', 'NumberOfEvents', 'FileSize', 'Luminosity', 'InstLuminosity']))
#     self.assertEqual(retVal['Value']['Records'][0], [6, 2466, 988452372, 0, 0])

#     bkQuery['FileType'] = 'SIM'
#     bkQuery['ReplicaFlag'] = 'Yes'
#     bkQuery['Visible'] = 'Y'
#     retVal = self.bk.getFilesSummary(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['ParameterNames'])
#     self.assertTrue(retVal['Value']['Records'])
#     self.assertTrue(retVal['Value']['TotalRecords'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(
#         ['NbofFiles', 'NumberOfEvents', 'FileSize', 'Luminosity', 'InstLuminosity']))
#     self.assertEqual(retVal['Value']['Records'][0], [1, 411, 862802861, 0, 0])

#   def test_getFilesWithMetadata(self):
#     bkQuery = {'ConfigName': 'test', 'ConfigVersion': 'Jenkins', 'Production': 2, 'Visible': 'N'}
#     parameterNames = [u'FileName', u'EventStat', u'FileSize', u'CreationDate', u'JobStart', u'JobEnd', u'WorkerNode',
#                       u'FileType', u'RunNumber', u'FillNumber', u'FullStat', u'DataqualityFlag',
#                       u'EventInputStat', u'TotalLuminosity', u'Luminosity', u'InstLuminosity', u'TCK',
#                       u'GUID', u'ADLER32', u'EventType', u'MD5SUM',
#                       u'VisibilityFlag', u'JobId', u'GotReplica', u'InsertTimeStamp']
#     retVal = self.bk.getFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['ParameterNames'])
#     self.assertTrue(retVal['Value']['Records'])
#     self.assertTrue(retVal['Value']['TotalRecords'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(parameterNames))
#     self.assertEqual(len(retVal['Value']['Records']), 1)

#     bkQuery['ReplicaFlag'] = 'No'
#     retVal = self.bk.getFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['ParameterNames'])
#     self.assertTrue(retVal['Value']['Records'])
#     self.assertTrue(retVal['Value']['TotalRecords'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 6)
#     self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(parameterNames))
#     self.assertEqual(len(retVal['Value']['Records']), 6)

#     bkQuery['FileType'] = 'DIGI'
#     retVal = self.bk.getFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['ParameterNames'])
#     self.assertTrue(retVal['Value']['Records'])
#     self.assertTrue(retVal['Value']['TotalRecords'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 6)
#     self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(parameterNames))
#     self.assertEqual(len(retVal['Value']['Records']), 6)

#     bkQuery['FileType'] = 'SIM'
#     bkQuery['ReplicaFlag'] = 'Yes'
#     bkQuery['Visible'] = 'Y'
#     retVal = self.bk.getFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['ParameterNames'])
#     self.assertTrue(retVal['Value']['Records'])
#     self.assertTrue(retVal['Value']['TotalRecords'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(parameterNames))
#     self.assertEqual(len(retVal['Value']['Records']), 1)

#     bkQuery['EventType'] = 11104131
#     retVal = self.bk.getFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['ParameterNames'])
#     self.assertTrue(retVal['Value']['Records'])
#     self.assertTrue(retVal['Value']['TotalRecords'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(parameterNames))
#     self.assertEqual(len(retVal['Value']['Records']), 1)

#     bkQuery['ProcessingPass'] = '/Sim09b'
#     retVal = self.bk.getFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['ParameterNames'])
#     self.assertTrue(retVal['Value']['Records'])
#     self.assertTrue(retVal['Value']['TotalRecords'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(parameterNames))
#     self.assertEqual(len(retVal['Value']['Records']), 1)

#     bkQuery['FileType'] = 'DIGI'
#     bkQuery['ReplicaFlag'] = 'Yes'
#     bkQuery['Visible'] = 'N'
#     retVal = self.bk.getFilesWithMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['ParameterNames'])
#     self.assertTrue(retVal['Value']['Records'])
#     self.assertTrue(retVal['Value']['TotalRecords'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(sorted(retVal['Value']['ParameterNames']), sorted(parameterNames))
#     self.assertEqual(len(retVal['Value']['Records']), 1)

#   def test_getFiles(self):
#     bkQuery = {'ConfigName': 'test', 'ConfigVersion': 'Jenkins', 'Production': 2, 'Visible': 'N'}

#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 1)

#     bkQuery['ReplicaFlag'] = 'No'
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 6)

#     bkQuery['FileType'] = 'DIGI'
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 6)

#     bkQuery['FileType'] = 'SIM'
#     bkQuery['ReplicaFlag'] = 'Yes'
#     bkQuery['Visible'] = 'Y'
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])

#     self.assertEqual(len(retVal['Value']), 1)

#     bkQuery['ProcessingPass'] = '/Sim09b'
#     bkQuery['ReplicaFlag'] = 'Yes'
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 1)

#     bkQuery['EventType'] = 11104131
#     bkQuery['ReplicaFlag'] = 'Yes'
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 1)

#     bkQuery['FileType'] = 'DIGI'
#     bkQuery['ReplicaFlag'] = 'Yes'
#     bkQuery['Visible'] = 'N'
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 1)

#   def test_getFilesNbEvents(self):
#     bkQuery = {'ConfigName': 'LHCb',
#                'ConfigVersion': 'Collision10',
#                'DataQuality': ['OK'],
#                'DataTakingConditions': 'Beam3500GeV-VeloClosed-MagDown',
#                'EventType': '90000000',
#                'FileType': 'RAW',
#                'NbOfEvents': True,
#                'ProcessingPass': '/Real Data',
#                'ReplicaFlag': 'Yes'}
#     retVal = self.bk.getFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value'], [5868758])

#   def test_getProductions(self):
#     bkQuery = {'ConditionDescription': 'Beam4000GeV-2012-MagUp-Nu2.5-Pythia8',
#                'ConfigName': 'test',
#                'ConfigVersion': 'Jenkins',
#                'EventType': 11104131,
#                'Production': 2,
#                'Visible': 'N'}
#     retVal = self.bk.getProductions(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['ParameterNames'])
#     self.assertTrue(retVal['Value']['Records'])
#     self.assertTrue(retVal['Value']['TotalRecords'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(retVal['Value']['Records'], [[2]])

#     bkQuery['FileType'] = 'SIM'
#     bkQuery['Visible'] = 'Y'
#     retVal = self.bk.getProductions(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['ParameterNames'])
#     self.assertTrue(retVal['Value']['Records'])
#     self.assertTrue(retVal['Value']['TotalRecords'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(retVal['Value']['Records'], [[2]])

#     bkQuery['FileType'] = 'DIGI'
#     bkQuery['Visible'] = 'N'
#     retVal = self.bk.getProductions(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['ParameterNames'])
#     self.assertTrue(retVal['Value']['Records'])
#     self.assertTrue(retVal['Value']['TotalRecords'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(retVal['Value']['Records'], [[2]])

#     bkQuery['ReplicaFlag'] = 'Yes'
#     retVal = self.bk.getProductions(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['ParameterNames'])
#     self.assertTrue(retVal['Value']['Records'])
#     self.assertTrue(retVal['Value']['TotalRecords'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(retVal['Value']['Records'], [[2]])

#     bkQuery['ProcessingPass'] = '/Sim09b'
#     retVal = self.bk.getProductions(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['ParameterNames'])
#     self.assertTrue(retVal['Value']['Records'])
#     self.assertTrue(retVal['Value']['TotalRecords'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(retVal['Value']['Records'], [[2]])

#     bkQuery['ProcessingPass'] = '/Sim09ba'
#     retVal = self.bk.getProductions(bkQuery)
#     self.assertFalse(retVal['OK'])

#   def test_getProcessingPass(self):
#     bkQuery = {'ConfigName': 'Test', 'ConfigVersion': 'Test01', 'ConditionDescription': 'Beam450GeV-MagDown'}
#     retVal = self.bk.getProcessingPass(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value'][0]['ParameterNames'])
#     self.assertTrue(retVal['Value'][0]['Records'])
#     self.assertTrue(retVal['Value'][0]['TotalRecords'])
#     self.assertEqual(retVal['Value'][0]['TotalRecords'], 1)
#     self.assertEqual(retVal['Value'][0]['Records'], [['Real Data']])

#     retVal = self.bk.getProcessingPass(bkQuery, '/Real Data')
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value'][1]['ParameterNames'])
#     self.assertTrue(retVal['Value'][1]['Records'])
#     self.assertTrue(retVal['Value'][1]['TotalRecords'])
#     self.assertEqual(retVal['Value'][1]['TotalRecords'], 1)
#     self.assertEqual(retVal['Value'][1]['ParameterNames'], ['EventType', 'Description'])
#     self.assertEqual(retVal['Value'][1]['Records'], [[30000000, 'minbias']])

#     bkQuery['RunNumber'] = 1122
#     retVal = self.bk.getProcessingPass(bkQuery, '/Real Data')
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value'][0]['ParameterNames'])
#     self.assertEqual(retVal['Value'][0]['TotalRecords'], 0)
#     self.assertTrue(retVal['Value'][1]['ParameterNames'])
#     self.assertEqual(retVal['Value'][1]['ParameterNames'], ['EventType', 'Description'])
#     self.assertLessEqual(retVal['Value'][1]['TotalRecords'], 1)

#   def test_getConditions(self):
#     bkQuery = {'ConfigName': 'Test', 'ConfigVersion': 'Test01'}
#     retVal = self.bk.getConditions(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value'][0]['TotalRecords'], 0)
#     self.assertEqual(retVal['Value'][1]['TotalRecords'], 1)

#     bkQuery['EventType'] = 30000000
#     retVal = self.bk.getConditions(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value'][0]['TotalRecords'], 0)
#     self.assertEqual(retVal['Value'][1]['TotalRecords'], 1)

#   def test_getMoreProductionInformations(self):
#     retVal = self.bk.getMoreProductionInformations(2)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['ConfigName'], 'test')
#     self.assertEqual(retVal['Value']['ConfigVersion'], 'Jenkins')
#     self.assertEqual(retVal['Value']['Simulation conditions'], 'Beam4000GeV-2012-MagUp-Nu2.5-Pythia8')
#     self.assertEqual(retVal['Value']['Processing pass'], '/Sim09b')
#     self.assertEqual(retVal['Value']['ProgramName'], 'Gauss')
#     self.assertEqual(retVal['Value']['ProgramVersion'], 'v49r5')

#   def test_getAvailableProductions(self):
#     retVal = self.bk.getAvailableProductions()
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 1)

#   def test_getProductionFiles(self):
#     retVal = self.bk.getProductionFiles(2, 'SIM')
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 1)
#     self.assertTrue('/lhcb/MC/2012/SIM/00056438/0000/00056438_00001025_test_1.sim' in retVal['Value'])
#     metadata = ['GotReplica', 'Visible', 'FileType', 'GUID', 'FileSize']
#     for met in metadata:
#       self.assertTrue(met in retVal['Value']['/lhcb/MC/2012/SIM/00056438/0000/00056438_00001025_test_1.sim'])

#   def test_getJobInfo(self):
#     retVal = self.bk.getJobInfo('/lhcb/MC/2012/SIM/00056438/0000/00056438_00001025_test_1.sim')
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value'][0]), 24)

#   def test_bulkJobInfo(self):
#     retVal = self.bk.bulkJobInfo(
#         {'lfn': ['/lhcb/MC/2012/SIM/00056438/0000/00056438_00001025_test_1.sim',
#                  '/lhcb/data/2016/RAW/Test/test/1122/0001122_test_1.raw']})
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value']['Successful'])
#     files = ['/lhcb/data/2016/RAW/Test/test/1122/0001122_test_1.raw',
#              '/lhcb/MC/2012/SIM/00056438/0000/00056438_00001025_test_1.sim']
#     self.assertEqual(sorted(retVal['Value']['Successful'].keys()), sorted(files))

#   def test_getJobInformation(self):
#     retVal = self.bk.getJobInformation({'Production': 2})
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 8)

#   def test_setFileDataQuality(self):
#     fileName = '/lhcb/MC/2012/SIM/00056438/0000/00056438_00001025_test_1.sim'
#     retVal = self.bk.setFileDataQuality(fileName, 'OK')
#     self.assertTrue(retVal['OK'])

#     retVal = self.bk.getFileMetadata(fileName)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['Successful'][fileName]['DataqualityFlag'], 'OK')

#   def test_getProcessingPassId(self):
#     retVal = self.bk.getProcessingPassId('/Real Data')
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value'] > 0)

#   def test_getFileAncestors(self):
#     fileName = '/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_8.digi'
#     retVal = self.bk.getFileAncestors(fileName)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']['Failed']), 0)
#     self.assertEqual(len(retVal['Value']['Successful']), 0)
#     self.assertEqual(len(retVal['Value']['WithMetadata']), 0)

#     retVal = self.bk.getFileAncestors(fileName, replica=False)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']['Failed']) == 0)
#     self.assertTrue(retVal['Value']['Successful'])
#     self.assertTrue(retVal['Value']['WithMetadata'])
#     self.assertEqual(len(retVal['Value']['Successful']), 1)
#     self.assertEqual(len(retVal['Value']['WithMetadata']), 1)

#     self.assertTrue(fileName in retVal['Value']['Successful'])
#     self.assertEqual(sorted(retVal['Value']['Successful'][fileName][0].keys()), sorted(['EventType',
#                                                                                         'FileType',
#                                                                                         'FileName',
#                                                                                         'Luminosity',
#                                                                                         'EventStat',
#                                                                                         'GotReplica',
#                                                                                         'InstLuminosity']))

#   def test_getFileDescendents(self):
#     fileName = '/lhcb/MC/2012/SIM/00056438/0000/00056438_00001025_test_1.sim'
#     retVal = self.bk.getFileDescendents([fileName], 1, 0, False)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']['Failed']), 0)
#     self.assertEqual(len(retVal['Value']['NotProcessed']), 0)
#     self.assertEqual(len(retVal['Value']['WithMetadata']), 1)
#     self.assertEqual(sorted(retVal['Value']['Successful'][fileName]),
#                      sorted(['/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_2.digi',
#                              '/lhcb/MC/2012/LOG/00056438/0000/00001025/Boole_00056438_00001025_test_2.log']))

#   def test_checkfile(self):
#     fileName = '/lhcb/MC/2012/SIM/00056438/0000/00056438_00001025_test_1.sim'
#     retVal = self.bk.checkfile(fileName)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value'][0]), 3)

#   def test_getSimConditions(self):
#     retVal = self.bk.getSimConditions()
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(len(retVal['Value']) > 0)

#   def test_getFileMetadata(self):
#     fileName = '/lhcb/MC/2012/SIM/00056438/0000/00056438_00001025_test_1.sim'
#     retVal = self.bk.getFileMetadata(fileName)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(sorted(retVal['Value']['Successful'][fileName].keys()), sorted(['GUID',
#                                                                                      'ADLER32',
#                                                                                      'FullStat',
#                                                                                      'EventType',
#                                                                                      'FileType',
#                                                                                      'MD5SUM',
#                                                                                      'VisibilityFlag',
#                                                                                      'InsertTimeStamp',
#                                                                                      'RunNumber',
#                                                                                      'JobId',
#                                                                                      'Luminosity',
#                                                                                      'FileSize',
#                                                                                      'EventStat',
#                                                                                      'GotReplica',
#                                                                                      'CreationDate',
#                                                                                      'InstLuminosity',
#                                                                                      'DataqualityFlag']))

#   def test_exists(self):
#     fileName = '/lhcb/MC/2012/SIM/00056438/0000/00056438_00001025_test_1.sim'
#     retVal = self.bk.exists(fileName)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(retVal['Value'][fileName])

#   def test_getProductionFilesStatus(self):
#     files = ['/lhcb/MC/2012/LOG/00056438/0000/00001025/Gauss_00056438_00001025_test_1.log',
#              '/lhcb/MC/2012/LOG/00056438/0000/00001025/Boole_00056438_00001025_test_2.log',
#              '/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_3.digi',
#              '/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_test_3.log',
#              '/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_4.digi',
#              '/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_test_4.log',
#              '/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_5.digi',
#              '/lhcb/MC/2012/LOG/00056438/0000/00001025/Noether_00056438_00001025_test_5.log',
#              '/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_6.digi',
#              '/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_test_6.log',
#              '/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_7.digi',
#              '/lhcb/MC/2012/LOG/00056438/0000/00001025/Moore_00056438_00001025_test_7.log',
#              '/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_8.digi',
#              '/lhcb/MC/2012/LOG/00056438/0000/00001025/Noether_00056438_00001025_test_8.log']
#     retVal = self.bk.getProductionFilesStatus(2)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['noreplica'], files)
#     self.assertEqual(retVal['Value']['replica'], ['/lhcb/MC/2012/SIM/00056438/0000/00056438_00001025_test_1.sim',
#                                                   '/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_2.digi'])

#   def test_getFileCreationLog(self):
#     retVal = self.bk.getFileCreationLog('/lhcb/MC/2012/SIM/00056438/0000/00056438_00001025_test_1.sim')
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value'], '/lhcb/MC/2012/LOG/00056438/0000/00001025/Gauss_00056438_00001025_test_1.log')

#   def test_getFileHistory(self):
#     retVal = self.bk.getFileHistory('/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_2.digi')
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     paramNames = ['FileId',
#                   'FileName',
#                   'ADLER32',
#                   'CreationDate',
#                   'EventStat',
#                   'Eventtype',
#                   'Gotreplica',
#                   'GUI',
#                   'JobId',
#                   'md5sum',
#                   'FileSize',
#                   'FullStat',
#                   'Dataquality',
#                   'FileInsertDate',
#                   'Luminosity',
#                   'InstLuminosity']
#     for param in paramNames:
#       self.assertTrue(param in retVal['Value']['ParameterNames'])

#   def test_getProductionNbOfJobs(self):
#     retVal = self.bk.getProductionNbOfJobs(2)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value'], [(8,)])

#   def test_getProductionNbOfEvents(self):
#     retVal = self.bk.getProductionNbOfEvents(2)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value'], [('SIM', 411, 11104131, None), ('DIGI', 411, 11104131, 411)])

#   def test_getProductionSizeOfFiles(self):
#     retVal = self.bk.getProductionSizeOfFiles(2)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value'], [(2097119140,)])

#   def test_getProductionNbOfFiles(self):
#     retVal = self.bk.getProductionNbOfFiles(2)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value'], [(1, 'SIM', 16), (7, 'DIGI', 16), (8, 'LOG', 16)])

#   def test_getProductionInformation(self):
#     retVal = self.bk.getProductionInformation(2)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 8)

#   def test_getNbOfJobsBySites(self):
#     retVal = self.bk.getNbOfJobsBySites(2)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value'], [(8, 'LCG.CERN.ch')])

#   def test_getProductionProcessedEvents(self):
#     retVal = self.bk.getProductionProcessedEvents(2)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value'], 411)

#   def test_getProductionProcessingPassSteps(self):
#     retVal = self.bk.getProductionProcessingPassSteps({'Production': 2})
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']['Records']), 8)

#   def test_getFileTypeVersion(self):
#     fileName = '/lhcb/MC/2012/DIGI/00056438/0000/00056438_00001025_test_2.digi'
#     retVal = self.bk.getFileTypeVersion(fileName)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(fileName in retVal['Value'])
#     self.assertEqual(retVal['Value'][fileName], 'ROOT')

#   def test_getDirectoryMetadata(self):
#     directory = '/lhcb/MC/2012/DIGI/00056438/'
#     retVal = self.bk.getDirectoryMetadata(directory)
#     self.assertTrue(retVal['OK'])
#     self.assertTrue(directory in retVal['Value']['Successful'])
#     self.assertEqual(retVal['Value']['Successful'][directory][0][
#                      'ConditionDescription'], 'Beam4000GeV-2012-MagUp-Nu2.5-Pythia8')
#     self.assertEqual(retVal['Value']['Successful'][directory][0]['ConfigName'], 'test')
#     self.assertEqual(retVal['Value']['Successful'][directory][0]['ConfigVersion'], 'Jenkins')
#     self.assertEqual(retVal['Value']['Successful'][directory][0]['EventType'], 11104131)
#     self.assertEqual(retVal['Value']['Successful'][directory][0]['FileType'], 'DIGI')
#     self.assertEqual(retVal['Value']['Successful'][directory][0]['Production'], 2)
#     self.assertEqual(retVal['Value']['Successful'][directory][0]['VisibilityFlag'], 'N')
#     self.assertEqual(retVal['Value']['Successful'][directory][0]['ProcessingPass'], '/Sim09b')

#   def test_getFilesForGUID(self):
#     retVal = self.bk.getFilesForGUID('546014C4-55C6-E611-8E94-02163E00F6B2')
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value'], '/lhcb/MC/2012/SIM/00056438/0000/00056438_00001025_test_1.sim')


#   def test_getProductionSummaryFromView(self):
#     retVal = self.bk.getProductionSummaryFromView({'Production': 2})
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value'][0]['ConditionDescription'], 'Beam4000GeV-2012-MagUp-Nu2.5-Pythia8')
#     self.assertEqual(retVal['Value'][0]['ConfigName'], 'test')
#     self.assertEqual(retVal['Value'][0]['ConfigVersion'], 'Jenkins')
#     self.assertEqual(retVal['Value'][0]['EventType'], 11104131)
#     self.assertEqual(retVal['Value'][0]['ProcessingPass'], '/Sim09b')
#     self.assertEqual(retVal['Value'][0]['Production'], 2)

#   def test_getProductionSummary(self):
#     retVal = self.bk.getProductionSummary({'Production': 2})
#     self.assertTrue(retVal['OK'])
#     paramNames = ['ConfigurationName',
#                   'ConfigurationVersion',
#                   'ConditionDescription',
#                   'Processing pass ',
#                   'EventType',
#                   'EventType description',
#                   'Production',
#                   'FileType',
#                   'Number of events']
#     self.assertTrue(retVal['Value']['ParameterNames'])
#     for param in retVal['Value']['ParameterNames']:
#       self.assertTrue(param in paramNames)

#     self.assertEqual(retVal['Value']['Records'], [['test',
#                                                    'Jenkins',
#                                                    'Beam4000GeV-2012-MagUp-Nu2.5-Pythia8',
#                                                    1091,
#                                                    11104131,
#                                                    'Bd_KpiKS=DecProdCut',
#                                                    2,
#                                                    'SIM',
#                                                    411]])

#   def test_getEventTypes(self):
#     retVal = self.bk.getEventTypes({'ConfigName': 'Test', 'ConfigVersion': 'Test01'})
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['Records'], [[30000000, 'minbias']])
#     self.assertEqual(retVal['Value']['ParameterNames'], ['EventType', 'Description'])

#     retVal = self.bk.getEventTypes({'Production': 2})
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['Records'], [[11104131, 'Bd_KpiKS=DecProdCut']])
#     self.assertEqual(retVal['Value']['ParameterNames'], ['EventType', 'Description'])

#   def test_getLimitedFiles(self):
#     bkQuery = {'Visible': 'Y', 'ConfigName': 'Test', 'ConditionDescription': 'Beam450GeV-MagDown',
#                'MaxItem': 25, 'EventType': '30000000', 'FileType': 'RAW', 'ProcessingPass':
#                '/Real Data', 'StartItem': 0,
#                'ConfigVersion': 'Test01', 'DataQuality': [u'OK', u'UNCHECKED']}
#     retVal = self.bk.getLimitedFiles(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 5)

#   def test_getListOfRuns(self):
#     bkQuery = {'Visible': 'Y', 'ConfigName': 'Test', 'ConditionDescription': 'Beam450GeV-MagDown',
#                'MaxItem': 25, 'EventType': '30000000', 'FileType': 'RAW', 'ProcessingPass':
#                '/Real Data', 'StartItem': 0,
#                'ConfigVersion': 'Test01', 'DataQuality': [u'OK', u'UNCHECKED']}
#     retVal = self.bk.getListOfRuns(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(len(retVal['Value']), 1)
#     self.assertEqual(retVal['Value'], [1122])

#   def test_getTCKs(self):
#     bkQuery = {'Visible': 'Y', 'ConfigName': 'Test', 'ConditionDescription': 'Beam450GeV-MagDown',
#                'MaxItem': 25, 'EventType': '30000000', 'FileType': 'RAW', 'ProcessingPass':
#                '/Real Data', 'StartItem': 0,
#                'ConfigVersion': 'Test01', 'DataQuality': [u'OK', u'UNCHECKED']}
#     retVal = self.bk.getTCKs(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value'], ['-0x7f6bffff'])

#   def test_getStepsMetadata(self):
#     bkQuery = {'Visible': 'Y', 'ConfigName': 'Test', 'ConditionDescription': 'Beam450GeV-MagDown',
#                'MaxItem': 25, 'EventType': '30000000', 'FileType': 'RAW', 'ProcessingPass':
#                '/Real Data', 'StartItem': 0,
#                'ConfigVersion': 'Test01', 'DataQuality': [u'OK', u'UNCHECKED']}
#     retVal = self.bk.getStepsMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     stepMeta = [['StepName', 'Real Data'],
#                 ['ApplicationName', 'Moore'],
#                 ['ApplicationVersion', 'v0r111'],
#                 ['OptionFiles', 'NULL'],
#                 ['DDDB', 'xyz'],
#                 ['CONDDB', 'xy'],
#                 ['ExtraPackages', 'NULL'],
#                 ['Visible', 'Y']]

#     for step in retVal['Value']['Records']:
#       for record in retVal['Value']['Records'][step]:
#         if record[0] not in 'StepId':
#           self.assertTrue(record in stepMeta)

#     bkQuery = {'ConfigName': 'MC',
#                'ConditionDescription': 'Beam7TeV-UpgradeML1.0-MagDown-Lumi2-25ns',
#                'EventType': '13104011',
#                'FileType': 'DST',
#                'ProcessingPass': '/Sim01/Rec03-WithTruth',
#                'ConfigVersion': 'Upgrade'}
#     retVal = self.bk.getStepsMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 4)
#     self.assertEqual(list(retVal['Value']['Records']), ['Step-2158', 'Step-2159', 'Step-901', 'Step-2160'])

#     bkQuery = {'ConfigName': 'MC',
#                'ConditionDescription': 'Beam3500GeV-May2010-MagDown-Fix1',
#                'EventType': '30000000',
#                'FileType': 'DST',
#                'ProcessingPass': '/Sim01/Trig0x002e002aFlagged/Reco08-MINBIAS',
#                'ConfigVersion': 'MC10'}

#     retVal = self.bk.getStepsMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(list(retVal['Value']['Records']), ['Step-11798'])

#     bkQuery = {'ConfigName': 'MC',
#                'ConditionDescription': 'Beam3500GeV-May2010-MagOff-Fix1',
#                'EventType': '30000000',
#                'FileType': 'DST',
#                'ProcessingPass': '/Sim01/Reco08',
#                'ConfigVersion': 'MC10'}
#     retVal = self.bk.getStepsMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(list(retVal['Value']['Records']), ['Step-13338'])

#     bkQuery = {'ConfigName': 'MC',
#                'ConditionDescription': 'Beam6500GeV-2015-MagUp-Nu1.6-25ns-Pythia8',
#                'EventType': '13714010',
#                'FileType': 'ALLSTREAMS.DST',
#                'ProcessingPass': '/Sim09a/Trig0x411400a2/Reco15a/Turbo02/Stripping24NoPrescalingFlagged',
#                'ConfigVersion': '2015'}
#     retVal = self.bk.getStepsMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 3)
#     self.assertEqual(list(retVal['Value']['Records']), ['Step-15153', 'Step-15155', 'Step-15154'])

#     bkQuery = {'ConfigName': 'LHCb',
#                'ConditionDescription': 'Beam3500GeV-VeloClosed-MagUp',
#                'EventType': '90000000',
#                'FileType': 'CHARMTOBESWUM.DST',
#                'ProcessingPass': '/Real Data/Reco12Trial/StrippingTrial/Stripping19b',
#                'ConfigVersion': 'Collision12'}
#     retVal = self.bk.getStepsMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(list(retVal['Value']['Records']), ['Step-14707'])

#     bkQuery = {'ConfigName': 'LHCb',
#                'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
#                'EventType': '90000000',
#                'FileType': 'EW.DST',
#                'ProcessingPass': '/Real Data/Reco10/Stripping13b',
#                'ConfigVersion': 'Collision11'}
#     retVal = self.bk.getStepsMetadata(bkQuery)
#     self.assertTrue(retVal['OK'])
#     self.assertEqual(retVal['Value']['TotalRecords'], 1)
#     self.assertEqual(list(retVal['Value']['Records']), ['Step-13438'])


# if __name__ == '__main__':

#   deleteTestSuite = unittest.defaultTestLoader.loadTestsFromTestCase(TestDestoryDataset)
#   unittest.TextTestRunner(verbosity=2, failfast=True).run(deleteTestSuite)
#   mcTestSuite = unittest.defaultTestLoader.loadTestsFromTestCase(MCInsertTestCase)
#   mcTestSuite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MCXMLReportInsert))
#   mcTestSuite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MCProductionTest))
#   unittest.TextTestRunner(verbosity=2, failfast=True).run(mcTestSuite)
#   suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestMethods)
#   suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestBookkeepingUserInterface))
#   suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestRemoveFiles))
#   suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestDestoryDataset))
#   testResult = unittest.TextTestRunner(verbosity=2, failfast=True).run(suite)
#   sys.exit(not testResult.wasSuccessful())
