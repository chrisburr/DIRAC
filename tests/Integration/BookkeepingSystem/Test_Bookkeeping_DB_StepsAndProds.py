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
This test connects directly to the DB, which must be present, and defined in the "CS"
"""

# pylint: disable=invalid-name,wrong-import-position

from __future__ import print_function

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from LHCbDIRAC.BookkeepingSystem.DB.OracleDB import OracleDB

# # sut
from LHCbDIRAC.BookkeepingSystem.DB.OracleBookkeepingDB import OracleBookkeepingDB

gLogger.setLevel('VERBOSE')

__RCSID__ = "$Id$"


#############################################################################
# Test data

step_gauss = {
    'Step': {
	'ApplicationName': 'Gauss',
	'Usable': 'Yes',
	'ApplicationVersion': 'v1r1',
	'ExtraPackages': '',
	'StepName': 'gauss',
	'ProcessingPass': 'Sim',
	'Visible': 'Y',
	'DDDB': 'gauss-dddb',
	'CONDDB': 'gauss-conddb',
	'OptionFiles': '/some/gauss/option/files'
    },
    'OutputFileTypes': [{'Visible': 'Y', 'FileType': 'SIM'}]
}

step_boole = {
    'Step': {
	'ApplicationName': 'Boole',
	'Usable': 'Yes',
	'ApplicationVersion': 'v2r2',
	'ExtraPackages': '',
	'StepName': 'boole',
	'ProcessingPass': 'Digi',
	'Visible': 'N',
	'DDDB': 'boole-dddb',
	'CONDDB': 'boole-conddb',
	'OptionFiles': '/some/boole/option/files'
    },
    'InputFileTypes': [{'Visible': 'Y', 'FileType': 'SIM'}],
    'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]
}

step_boole2 = {
    'Step': {
	'ApplicationName': 'Boole',
	'Usable': 'Yes',
	'ApplicationVersion': 'v2r3',
	'ExtraPackages': '',
	'StepName': 'boole2',
	'ProcessingPass': 'Digi2',
	'Visible': 'N',
	'DDDB': 'fromPreviousStep',
	'CONDDB': 'fromPreviousStep',
	'OptionFiles': '/some/boole2/option/files'
    },
    'InputFileTypes': [{'Visible': 'Y', 'FileType': 'SIM'}],
    'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]
}

step_moore = {
    'Step': {
	'ApplicationName': 'Moore',
	'Usable': 'Yes',
	'ApplicationVersion': 'v3r3',
	'ExtraPackages': '',
	'StepName': 'moore',
	'ProcessingPass': 'L0Trig',
	'Visible': 'N',
	'DDDB': 'fromPreviousStep',
	'CONDDB': 'fromPreviousStep',
	'OptionFiles': '/some/moore/option/files'
    },
    'InputFileTypes': [{'Visible': 'Y', 'FileType': 'DIGI'}],
    'OutputFileTypes': [{'Visible': 'Y', 'FileType': 'DIGI'}]
}

#############################################################################

# What's used for the tests
bk = OracleBookkeepingDB()

#############################################################################


def test_inserts():

  # # first delete from DB ####################
  bk.dbW_._query("DELETE FROM eventtypes")
  bk.dbW_._query("DELETE FROM filetypes")
  bk.dbW_._query("DELETE FROM stepscontainer")
  bk.dbW_._query("DELETE FROM steps")
  bk.dbW_._query("DELETE FROM productionscontainer")
  bk.dbW_._query("DELETE FROM processing")
  bk.dbW_._query("DELETE FROM simulationconditions")
  bk.dbW_._query("DELETE FROM configurations")
  # # #########################################

  # insert gauss step
  res = bk.insertStep(step_gauss)
  assert res['OK'] is True
  gaussStepID = res['Value']
  res = bk.getStepOutputFiles(gaussStepID)
  assert res['OK'] is True
  assert res['Value'] == [('SIM', 'Y')]

  # insert boole step
  res = bk.insertStep(step_boole)
  assert res['OK'] is True
  booleStepID = res['Value']
  res = bk.getStepInputFiles(booleStepID)
  assert res['OK'] is True
  assert res['Value'] == [('SIM', 'Y')]
  res = bk.getStepOutputFiles(booleStepID)
  assert res['OK'] is True
  assert res['Value'] == [('DIGI', 'N')]

  # insert boole/2 step
  res = bk.insertStep(step_boole2)
  assert res['OK'] is True
  boole2StepID = res['Value']
  res = bk.getStepInputFiles(boole2StepID)
  assert res['OK'] is True
  assert res['Value'] == [('SIM', 'Y')]
  res = bk.getStepOutputFiles(boole2StepID)
  assert res['OK'] is True
  assert res['Value'] == [('DIGI', 'N')]

  # insert moore step
  res = bk.insertStep(step_moore)
  assert res['OK'] is True
  mooreStepID = res['Value']
  res = bk.getStepInputFiles(mooreStepID)
  assert res['OK'] is True
  assert res['Value'] == [('DIGI', 'Y')]
  res = bk.getStepOutputFiles(mooreStepID)
  assert res['OK'] is True
  assert res['Value'] == [('DIGI', 'Y')]

  # FIXME: this getAvailableSteps should be expanded
  res = bk.getAvailableSteps({})
  assert res['OK'] is True
  stepsInDB = res['Value']['Records']
  assert {gaussStepID, booleStepID, boole2StepID, mooreStepID}.issubset(set([x[0] for x in stepsInDB]))

  # Production 1: [gauss]
  res = bk.addProductionSteps([{'StepId': gaussStepID}], 1)
  assert res['OK'] is True

  # Production 2: [gauss, boole]
  res = bk.addProductionSteps([{'StepId': gaussStepID}, {'StepId': booleStepID}], 2)
  assert res['OK'] is True

  # Production 3: [gauss, boole2]
  res = bk.addProductionSteps([{'StepId': gaussStepID}, {'StepId': boole2StepID}], 3)
  assert res['OK'] is True

  # Production 4: [gauss, boole, boole2]
  res = bk.addProductionSteps([{'StepId': gaussStepID}, {'StepId': booleStepID}, {'StepId': boole2StepID}], 4)
  assert res['OK'] is True

  # Production 5: [gauss, boole2, boole, moore]
  res = bk.addProductionSteps([{'StepId': gaussStepID}, {'StepId': boole2StepID},
			       {'StepId': booleStepID}, {'StepId': mooreStepID}], 5)
  assert res['OK'] is True

  # Production 6: [boole2, moore] (this should be in the same "production request" with 1)
  res = bk.addProductionSteps([{'StepId': boole2StepID}, {'StepId': mooreStepID}], 6)
  assert res['OK'] is True

  # Now testing getting the steps
  res = bk.getSteps(1)  # [gauss]
  assert res['OK'] is True
  assert res['Value'] == [('gauss', 'Gauss', 'v1r1',
			   '/some/gauss/option/files',
			   'gauss-dddb', 'gauss-conddb',
			   None, gaussStepID, 'Y')]

  res = bk.getSteps(2)  # [gauss, boole]
  assert res['OK'] is True
  assert res['Value'] == [('gauss', 'Gauss', 'v1r1',
			   '/some/gauss/option/files',
			   'gauss-dddb', 'gauss-conddb',
			   None, gaussStepID, 'Y'),
			  ('boole', 'Boole', 'v2r2',
			   '/some/boole/option/files',
			   'boole-dddb', 'boole-conddb',
			   None, booleStepID, 'N')]

  res = bk.getSteps(3)  # [gauss, boole2]
  assert res['OK'] is True
  assert res['Value'] == [('gauss', 'Gauss', 'v1r1',
			   '/some/gauss/option/files',
			   'gauss-dddb', 'gauss-conddb',
			   None, gaussStepID, 'Y'),
			  ('boole2', 'Boole', 'v2r3',
			   '/some/boole2/option/files',
			   'gauss-dddb', 'gauss-conddb',
			   None, boole2StepID, 'N')]

  res = bk.getSteps(4)  # [gauss, boole, boole2]
  assert res['OK'] is True
  assert res['Value'] == [('gauss', 'Gauss', 'v1r1',
			   '/some/gauss/option/files',
			   'gauss-dddb', 'gauss-conddb',
			   None, gaussStepID, 'Y'),
			  ('boole', 'Boole', 'v2r2',
			   '/some/boole/option/files',
			   'boole-dddb', 'boole-conddb',
			   None, booleStepID, 'N'),
			  ('boole2', 'Boole', 'v2r3',
			   '/some/boole2/option/files',
			   'boole-dddb', 'boole-conddb',
			   None, boole2StepID, 'N')]

  res = bk.getSteps(5)  # [gauss, boole2, boole, moore]
  assert res['OK'] is True
  assert res['Value'] == [('gauss', 'Gauss', 'v1r1',
			   '/some/gauss/option/files',
			   'gauss-dddb', 'gauss-conddb',
			   None, gaussStepID, 'Y'),
			  ('boole2', 'Boole', 'v2r3',
			   '/some/boole2/option/files',
			   'gauss-dddb', 'gauss-conddb',
			   None, boole2StepID, 'N'),
			  ('boole', 'Boole', 'v2r2',
			   '/some/boole/option/files',
			   'boole-dddb', 'boole-conddb',
			   None, booleStepID, 'N'),
			  ('moore', 'Moore', 'v3r3',
			   '/some/moore/option/files',
			   'boole-dddb', 'boole-conddb',
			   None, mooreStepID, 'N')]

  # Now dealing with the case where the current production does not have any step with an explicit DB tag
  res = bk.getSteps(6)  # [boole2, moore]
  assert res['OK'] is False
  # This fails because there's no processing pass for this production,
  # so no possibility of finding the parent production (this should never happen)

  res = bk.getSteps(6, {'ProcessingPass': 'MC/Sim/Digi2/L0Trig'})
  assert res['OK'] is False  # This (still) fails because there are no processing passes registered

  # Now registering a production and with it its processing pass
  # This is several steps...

  simcondDict = {'SimDescription': 'SimCond',
		 'BeamCond': 'BeamCond',
		 'BeamEnergy': 'BeamEnergy',
		 'Generator': 'Generator',
		 'MagneticField': 'MagneticField',
		 'DetectorCond': 'DetectorCond',
		 'Luminosity': 'Luminosity',
		 'G4settings': 'G4settings'}
  res = bk.insertSimConditions(simcondDict)
  assert res['OK'] is True
  res = bk.insertFileTypes('SIM', 'bofbof', 'ROOT')
  assert res['OK'] is True
  res = bk.insertFileTypes('DIGI', 'bof', 'ROOT')
  assert res['OK'] is True
  res = bk.insertEventTypes(12345, 'boh', 'primary')
  assert res['OK'] is True

  gaussStep = {'StepId': gaussStepID, 'Visible': 'Y',
	       'OutputFileTypes': [{'Visible': 'N', 'FileType': 'SIM'}]}
  booleStep = {'StepId': booleStepID, 'Visible': 'N',
	       'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]}

  res = bk.addProduction(7, simcond='SimCond', steps=[gaussStep, booleStep],
			 inputproc='Sim', configName='MC', configVersion='20', eventType=12345)
  assert res['OK'] is True

  res = bk.getSteps(6, {'ProcessingPass': '/Sim/Digi2/L0Trig'})
  assert res['OK'] is True
  assert res['Value'] == [('boole2', 'Boole', 'v2r3',
			   '/some/boole2/option/files',
			   'boole-dddb', 'boole-conddb',
			   None, boole2StepID, 'N'),
			  ('moore', 'Moore', 'v3r3',
			   '/some/moore/option/files',
			   'boole-dddb', 'boole-conddb',
			   None, mooreStepID, 'N')]

  # res = bk.getSteps(6)
  # print(res)
  # assert res['OK'] is True
