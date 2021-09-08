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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=invalid-name,wrong-import-position


from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
gLogger.setLevel('VERBOSE')

from .Utilities import wipeOutDB

# # sut
from LHCbDIRAC.BookkeepingSystem.DB.OracleBookkeepingDB import OracleBookkeepingDB


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

# # first delete the content from the DB
wipeOutDB()

#############################################################################


def test_Steps():

  # insert gauss step
  res = bk.insertStep(step_gauss)
  assert res['OK'], res['Message']
  gaussStepID = res['Value']
  res = bk.getStepOutputFiles(gaussStepID)
  assert res['OK'], res['Message']
  assert res['Value'] == [('SIM', 'Y')]

  # insert boole step
  res = bk.insertStep(step_boole)
  assert res['OK'], res['Message']
  booleStepID = res['Value']
  res = bk.getStepInputFiles(booleStepID)
  assert res['OK'], res['Message']
  assert res['Value'] == [('SIM', 'Y')]
  res = bk.getStepOutputFiles(booleStepID)
  assert res['OK'], res['Message']
  assert res['Value'] == [('DIGI', 'N')]

  # insert boole/2 step
  res = bk.insertStep(step_boole2)
  assert res['OK'], res['Message']
  boole2StepID = res['Value']
  res = bk.getStepInputFiles(boole2StepID)
  assert res['OK'], res['Message']
  assert res['Value'] == [('SIM', 'Y')]
  res = bk.getStepOutputFiles(boole2StepID)
  assert res['OK'], res['Message']
  assert res['Value'] == [('DIGI', 'N')]

  # insert moore step
  res = bk.insertStep(step_moore)
  assert res['OK'], res['Message']
  mooreStepID = res['Value']
  res = bk.getStepInputFiles(mooreStepID)
  assert res['OK'], res['Message']
  assert res['Value'] == [('DIGI', 'Y')]
  res = bk.getStepOutputFiles(mooreStepID)
  assert res['OK'], res['Message']
  assert res['Value'] == [('DIGI', 'Y')]

  # FIXME: this getAvailableSteps should be expanded
  res = bk.getAvailableSteps({})
  assert res['OK'], res['Message']
  stepsInDB = res['Value']['Records']
  assert {gaussStepID, booleStepID, boole2StepID, mooreStepID}.issubset(set([x[0] for x in stepsInDB]))

  # Production 1: [gauss]
  res = bk.addProductionSteps([{'StepId': gaussStepID}], 1)
  assert res['OK'], res['Message']

  # Production 2: [gauss, boole]
  res = bk.addProductionSteps([{'StepId': gaussStepID}, {'StepId': booleStepID}], 2)
  assert res['OK'], res['Message']

  # Production 3: [gauss, boole2]
  res = bk.addProductionSteps([{'StepId': gaussStepID}, {'StepId': boole2StepID}], 3)
  assert res['OK'], res['Message']

  # Production 4: [gauss, boole, boole2]
  res = bk.addProductionSteps([{'StepId': gaussStepID}, {'StepId': booleStepID}, {'StepId': boole2StepID}], 4)
  assert res['OK'], res['Message']

  # Production 5: [gauss, boole2, boole, moore]
  res = bk.addProductionSteps([{'StepId': gaussStepID}, {'StepId': boole2StepID},
                               {'StepId': booleStepID}, {'StepId': mooreStepID}], 5)
  assert res['OK'], res['Message']

  # Production 6: [boole2, moore] (this should be in the same "production request" with 1)
  res = bk.addProductionSteps([{'StepId': boole2StepID}, {'StepId': mooreStepID}], 6)
  assert res['OK'], res['Message']

  # Now testing getting the steps
  res = bk.getSteps(1)  # [gauss]
  assert res['OK'], res['Message']
  assert res['Value'] == [('gauss', 'Gauss', 'v1r1',
                           '/some/gauss/option/files',
                           'gauss-dddb', 'gauss-conddb',
                           None, gaussStepID, 'Y')]

  res = bk.getSteps(2)  # [gauss, boole]
  assert res['OK'], res['Message']
  assert res['Value'] == [('gauss', 'Gauss', 'v1r1',
                           '/some/gauss/option/files',
                           'gauss-dddb', 'gauss-conddb',
                           None, gaussStepID, 'Y'),
                          ('boole', 'Boole', 'v2r2',
                           '/some/boole/option/files',
                           'boole-dddb', 'boole-conddb',
                           None, booleStepID, 'N')]

  res = bk.getSteps(3)  # [gauss, boole2]
  assert res['OK'], res['Message']
  assert res['Value'] == [('gauss', 'Gauss', 'v1r1',
                           '/some/gauss/option/files',
                           'gauss-dddb', 'gauss-conddb',
                           None, gaussStepID, 'Y'),
                          ('boole2', 'Boole', 'v2r3',
                           '/some/boole2/option/files',
                           'fromPreviousStep', 'fromPreviousStep',
                           None, boole2StepID, 'N')]

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
  assert res['OK'], res['Message']
  res = bk.insertFileTypes('SIM', 'bofbof', 'ROOT')
  assert res['OK'], res['Message']
  res = bk.insertFileTypes('DIGI', 'bof', 'ROOT')
  assert res['OK'], res['Message']
  res = bk.insertEventTypes(12345, 'boh', 'primary')
  assert res['OK'], res['Message']

  gaussStep = {'StepId': gaussStepID, 'Visible': 'Y',
               'OutputFileTypes': [{'Visible': 'N', 'FileType': 'SIM'}]}
  booleStep = {'StepId': booleStepID, 'Visible': 'N',
               'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]}

  res = bk.addProduction(7, simcond='SimCond', steps=[gaussStep, booleStep],
                         inputproc='Sim', configName='MC', configVersion='20', eventType=12345)
  assert res['OK'], res['Message']

  res = bk.getSteps(6)
  assert res['OK'], res['Message']
  assert res['Value'] == [('boole2', 'Boole', 'v2r3',
                           '/some/boole2/option/files',
                           'fromPreviousStep', 'fromPreviousStep',
                           None, boole2StepID, 'N'),
                          ('moore', 'Moore', 'v3r3',
                           '/some/moore/option/files',
                           'fromPreviousStep', 'fromPreviousStep',
                           None, mooreStepID, 'N')]

  # Adding production 8 (with the same steps of 6, still "inheriting" from 7)

  boole2Step = {'StepId': boole2StepID, 'Visible': 'Y',
                'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]}
  mooreStep = {'StepId': mooreStepID, 'Visible': 'N',
               'OutputFileTypes': [{'Visible': 'N', 'FileType': 'DIGI'}]}
  res = bk.addProduction(8, simcond='SimCond', steps=[boole2Step, mooreStep],
                         inputproc='Sim', configName='MC', configVersion='20', eventType=12345)
  assert res['OK'], res['Message']

  res = bk.getSteps(8)
  assert res['OK'], res['Message']
  assert res['Value'] == [('boole2', 'Boole', 'v2r3',
                           '/some/boole2/option/files',
                           'fromPreviousStep', 'fromPreviousStep',
                           None, boole2StepID, 'N'),
                          ('moore', 'Moore', 'v3r3',
                           '/some/moore/option/files',
                           'fromPreviousStep', 'fromPreviousStep',
                           None, mooreStepID, 'N')]
