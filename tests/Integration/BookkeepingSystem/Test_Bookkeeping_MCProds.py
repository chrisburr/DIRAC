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

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.tests.Utilities.utils import find_all

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


#############################################################################

# What's used for the tests
bk = BookkeepingClient()

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
  productionSteps['Steps'].append({'StepId': retVal['Value'],
				   'Visible': 'N',
				   'OutputFileTypes': [{'Visible': 'N',
							'FileType': 'DIGI'}]})

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
  for rep in ['Job_Report_MCFastSimulation.xml',
	      'Job_Report_MCReconstruction_1.xml',
	      'Job_Report_MCReconstruction_2.xml',
	      'Job_Report_MCMerge.xml']:
    bkFile = find_all(rep, '..', 'BookkeepingSystem')[0]
    with io.open(bkFile, 'r') as fd:
      bkXML = fd.read()
    res = bk.sendXMLBookkeepingReport(bkXML)
    assert res['OK']


def test_getSimConditions():
  """
  check the existence of the sim cond
  """
  retVal = bk.getSimConditions()
  assert retVal['OK'] is True
  assert len(retVal['Value']) >= 1
  assert simCondDict['SimDescription'] in (i[1] for i in retVal['Value'])
