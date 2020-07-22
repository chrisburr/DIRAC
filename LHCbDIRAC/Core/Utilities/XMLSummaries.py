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
"""Utilities to check the XML summary files."""

import os
import ast
import io
import json
import xmltodict
from DIRAC import gLogger
from LHCbDIRAC.Core.Utilities.XMLTreeParser import XMLTreeParser

__RCSID__ = "$Id$"


def xmltojsonCat1(lCategory1):
  '''returns a Category1 dictionary
    :param lCategory1: list containing Category1 counters
    e.g. Category1 counter : <counter name="MCVeloHitPacker/# PackedData">50809</counter>
  '''
  result = {}
  for counter in lCategory1:
    key1, key2 = counter['@name'].split('/# ')
    result[key1] = {key2: int(counter['#text'])}

  return(result)


def xmltojsonCat2(lCategory2):
  '''returns a Category2 dictionary
    :param lCategory2: list containing Category2 counters
    e.g. Category2 counters:
    <counter name="TTHitMonitor/DeltaRay">1249</counter>
    <counter name="TTHitMonitor/betaGamma">28101829</counter>
    <counter name="TTHitMonitor/numberHits">17105</counter>
  '''
  result = {}
  for value in lCategory2:
    key1, key2 = value['@name'].split("/", 1)
    if key1 not in result:
      result[key1] = {}
    result[key1][key2] = int(value['#text'])
  return result


def xmltojsonCat3(lCategory3):
  '''returns a Category3 dictionary
    :param lCategory3: list containing Category3 counters
    e.g. Category3 counters:
    <counter name="CheckRichOpPhot/Diff.    - Aero. Exit x">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - Aero. Exit y">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - Aero. Exit z">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - Cherenkov Phi">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - Cherenkov Theta">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - Emission Point x">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - Emission Point y">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - Emission Point z">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - Energy">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - HPD In. Point x">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - HPD In. Point y">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - HPD In. Point z">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - HPD QW Point x">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - HPD QW Point y">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - HPD QW Point z">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - Parent Momentum x">38</counter>
    <counter name="CheckRichOpPhot/Diff.    - Parent Momentum y">46</counter>
    <counter name="CheckRichOpPhot/Diff.    - Parent Momentum z">-33</counter>
    <counter name="CheckRichOpPhot/Diff.    - Prim. Mirr. x">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - Prim. Mirr. y">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - Prim. Mirr. z">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - Sec. Mirr. x">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - Sec. Mirr. y">0</counter>
    <counter name="CheckRichOpPhot/Diff.    - Sec. Mirr. z">0</counter>
  '''
  result = dict()
  key1, key2 = lCategory3[0]['@name'].split(" - ", 1)
  key1 = key1.strip()
  # key2c and key3c are used if key3c in ['x', 'y', 'z']
  key2c = key2[:-2]
  key3c = key2[-1:]
  # key2cc and key3cc are used if key3cc in ['Phi', 'Eta']
  key2cc = key2[:- 4]
  key3cc = key2[-3:]
  if key3c in ['x', 'y', 'z'] and key2 != 'Energy':
    result[key1] = {key2c: {key3c: int(lCategory3[0]['#text'])}}
  elif key3cc in ['Phi', 'Eta']:
    result[key1] = {key2cc: {key3cc: int(lCategory3[0]['#text'])}}
  else:
    result[key1] = {key2: int(lCategory3[0]['#text'])}
  for enum in enumerate(lCategory3[1:]):
    key_1, key_2 = enum[1]['@name'].split(" - ")
    key_1 = key_1.strip()
    key_2c = key_2[:-2]
    key_3c = key_2[-1:]
    key_2cc = key_2[:-4]
    key_3cc = key_2[-3:]
    if key_2cc != key2cc:
      if key_3c in ['x', 'y', 'z'] and key_2 != 'Energy':
        result[key_1][key_2c] = {key_3c: int(enum[1]['#text'])}
      elif key_3cc in ['Phi', 'Eta']:
        result[key_1] = {key_2cc: {key_3cc: int(enum[1]['#text'])}}
      else:
        result[key_1].update({key_2: int(enum[1]['#text'])})
      key2 = key_2
      key2c = key_2c
      key2cc = key_2cc
      key3cc = key_3cc
    else:
      if key_3c in ['x', 'y', 'z'] and key_2 != 'Energy':
        result[key_1][key_2c].update({key_3c: int(enum[1]['#text'])})
      elif key_3cc in ['Phi', 'Eta']:
        result[key_1][key_2cc].update({key_3cc: int(enum[1]['#text'])})
      else:
        result[key_1].update({key_2: int(enum[1]['#text'])})

  return(result)


def ranges(mainList):
  ''' Returns a list containing the ranges of each category '''
  rangesList = [mainList[0], mainList[1]]
  for i in enumerate(mainList[2:]):
    if mainList[i[0] + 2] == mainList[i[0] + 1] + 1 and mainList[i[0] + 1] == mainList[i[0]] + 1:
      rangesList[len(rangesList) - 1] = mainList[i[0] + 2]
    else:
      rangesList.append(mainList[i[0] + 2])
  return rangesList


def difisnotnull(dict_3):
  ''' Returns True if a category 3 dictionary contains a field or a subfield that has a value different from 0 '''
  if isinstance(dict_3, dict):
    return any(difisnotnull(v) for v in dict_3.values())
  else:
    return dict_3 != 0


class XMLSummaryError(Exception):
  """Define error for XML summary."""

  def __init__(self, message=""):

    self.message = message
    Exception.__init__(self, message)

  def __str__(self):
    return "XMLSummaryError:" + repr(self.message)


################################################################################

class XMLSummary(object):
  """XML summary class."""

  def __init__(self, xmlFileName, log=None):
    """initialize a XML summary object, given a fileName, getting some relevant
    info."""

    if not log:
      self.log = gLogger.getSubLogger('XMLSummary')
    else:
      self.log = log

    self.xmlFileName = xmlFileName

    if not os.path.exists(self.xmlFileName):
      self.log.error("XML Summary Not Available", "%s" % self.xmlFileName)
      raise XMLSummaryError("XML Summary Not Available")

    if os.stat(self.xmlFileName)[6] == 0:
      self.log.error("Requested XML summary file is empty", "%s" % self.xmlFileName)
      raise XMLSummaryError("Requested XML summary file is empty")

    summary = XMLTreeParser()

    try:
      self.xmlTree = summary.parse(self.xmlFileName)
    except Exception as e:
      self.log.error("Error parsing xml summary", "%s" % str(e))
      raise XMLSummaryError("Error parsing xml summary")

    self.success = self.__getSuccess()
    self.step = self.__getStep()
    self.memory = self.__getMemory()
    self.inputStatus, self.failedInputURL = self.__getInputStatus()
    self.inputFileStats = self.__getInputFileStats()
    self.inputEventsTotal, self.inputsEvents = self.__getInputEvents()
    self.outputFileStats = self.__getOutputFileStats()
    self.outputEventsTotal, self.outputsEvents = self.__getOutputEvents()

################################################################################

  def analyse(self, inputsOnPartOK=False):
    """analyse the XML summary: this is a 'standard' analysis."""
    if inputsOnPartOK:
      self.log.warn("part status for input files is considered OK")
    if self.success == 'True' and self.step == 'finalize' and self._inputsOK(inputsOnPartOK) and self._outputsOK():
      self.log.info("XML Summary OK")
      return True
    self.log.warn("XML Summary reports errors")
    return False

################################################################################

  def _inputsOK(self, inputsOnPartOK=False):
    """check self.inputFileStats."""

    if inputsOnPartOK:
      if sum(self.inputFileStats.values()) == self.inputFileStats['part'] or \
              sum(self.inputFileStats.values()) == self.inputFileStats['full']:
        return True
      return False

    else:
      return bool(sum(self.inputFileStats.values()) == self.inputFileStats['full'])

################################################################################

  def _outputsOK(self):
    """check self.outputFileStats."""

    return bool(sum(self.outputFileStats.values()) == self.outputFileStats['full'])

################################################################################

  def __getSuccess(self):
    """get the success."""

    summary = self.xmlTree[0]

    successXML = summary.childrens('success')
    if len(successXML) != 1:
      raise XMLSummaryError("XMLSummary bad format: Nr of success items != 1")

    return successXML[0].value

################################################################################

  def __getStep(self):
    """Get the step."""

    summary = self.xmlTree[0]

    stepXML = summary.childrens('step')
    if len(stepXML) != 1:
      raise XMLSummaryError("XMLSummary bad format: Nr of step items != 1")

    return stepXML[0].value

################################################################################

  def __getMemory(self):
    """get the memory used."""

    summary = self.xmlTree[0]

    statXML = summary.childrens('usage')
    if len(statXML) != 1:
      raise XMLSummaryError("XMLSummary bad format: Nr of usage items != 1")

    statXML = statXML[0].childrens('stat')

    if len(statXML) != 1:
      raise XMLSummaryError("XMLSummary bad format: no stat")

    return statXML[0].value

################################################################################

  def __getInputStatus(self):
    """We know beforehand the structure of the XML, which makes our life
    easier.

    < summary >   ...   < input >   ...
    """

    files = []
    failedURLs = []
    summary = self.xmlTree[0]

    for inputF in summary.childrens('input'):
      for filename in inputF.childrens('file'):
        try:
          fileName = filename.attributes['name']
          if 'LFN:' in fileName:
            files.append((filename.attributes['name'], filename.attributes['status']))
          elif fileName.startswith('PFN:'):
            failedURLs.append((filename.attributes['name'], filename.attributes['status']))
        except Exception:
          raise XMLSummaryError("Bad formatted file keys")

    return files, failedURLs


################################################################################

  def __getInputFileStats(self):
    """Checks that every input file has reached the full status. Four possible
    statuses of the files:

    - full : the file has been fully read
    - part : the file has been partially read
    - mult : the file has been read multiple times
    - fail : failure while reading the file
    """

    fileCounter = {'full': 0,
                   'part': 0,
                   'mult': 0,
                   'fail': 0,
                   'other': 0}

    for fileIn, status in self.inputStatus:

      if status == 'fail':
        self.log.warn('Input File on status', '%s: %s' % (fileIn, status))
        fileCounter['fail'] += 1

      elif status == 'mult':
        self.log.warn('Input File on status', '%s: %s' % (fileIn, status))
        fileCounter['mult'] += 1

      elif status == 'part':
        self.log.warn('Input File on status', '%s: %s' % (fileIn, status))
        fileCounter['part'] += 1

      elif status == 'full':
        # If it is Ok, we do not print anything
        # self.log.warn( 'File %s is on status %s.' % ( file, status ) )
        fileCounter['full'] += 1

      # This should never happen, but just in case
      else:
        self.log.warn('Input File on unknown status', '%s: %s' % (fileIn, status))
        fileCounter['other'] += 1

    files = ['%d input file(s) on %s status' % (v, k) for k, v in fileCounter.iteritems() if v > 0]
    filesMsg = ', '.join(files)
    self.log.info('Inputs on status', filesMsg)

    return fileCounter

################################################################################

  def __getInputEvents(self):
    """We know beforehand the structure of the XML, which makes our life
    easier.

    < summary >   ...   < input >   ...
    """

    inputEventsTotal = 0
    inputsEvents = {}

    summary = self.xmlTree[0]

    for output in summary.childrens('input'):
      for fileIn in output.childrens('file'):
        inputEventsTotal += int(fileIn.value)
        inputsEvents[fileIn.attributes['name'].replace('LFN:', '').replace('PFN:', '').split('/').pop()] = fileIn.value

    return inputEventsTotal, inputsEvents

################################################################################

  def __getOutputStatus(self):
    """We know beforehand the structure of the XML, which makes our life
    easier.

    < summary >   ...   < output >   ...
    """

    files = []

    summary = self.xmlTree[0]

    for output in summary.childrens('output'):
      for fileIn in output.childrens('file'):
        try:
          files.append((fileIn.attributes['name'], fileIn.attributes['status']))
        except Exception:
          raise XMLSummaryError("Bad formatted file keys")

    return files

################################################################################

  def __getOutputEvents(self):
    """We know beforehand the structure of the XML, which makes our life
    easier.

    < summary >   ...   < output >   ...
    """

    outputEventsTotal = 0
    outputsEvents = {}

    summary = self.xmlTree[0]

    for output in summary.childrens('output'):
      for fileIn in output.childrens('file'):
        outputEventsTotal += int(fileIn.value)
        outputsEvents[fileIn.attributes['name'].replace('LFN:', '').replace('PFN:', '').split('/').pop()] = fileIn.value

    return outputEventsTotal, outputsEvents

################################################################################

  def __getOutputFileStats(self):
    """Checks that every output file has reached the full status. Four possible
    statuses of the files:

    - full : the file has been fully read
    - part : the file has been partially read
    - mult : the file has been read multiple times
    - fail : failure while reading the file
    """

    res = self.__getOutputStatus()

    fileCounter = {'full': 0,
                   'part': 0,
                   'mult': 0,
                   'fail': 0,
                   'other': 0}

    for filename, status in res:

      if status == 'fail':
        self.log.warn('Output File on status', '%s: %s.' % (filename, status))
        fileCounter['fail'] += 1

      elif status == 'mult':
        self.log.warn('Output File on status', '%s: %s.' % (filename, status))
        fileCounter['mult'] += 1

      elif status == 'part':
        self.log.warn('Output File on status', '%s: %s.' % (filename, status))
        fileCounter['part'] += 1

      elif status == 'full':
        # If it is Ok, we do not print anything
        # self.log.error( 'File %s is on status %s.' % ( filename, status ) )
        fileCounter['full'] += 1

      # This should never happen, but just in case
      else:
        self.log.warn('Output File on unknown status', '%s: %s.' % (filename, status))
        fileCounter['other'] += 1

    files = ['%d output file(s) on %s status' % (v, k) for k, v in fileCounter.iteritems() if v > 0]
    filesMsg = ', '.join(files)
    self.log.info('Outputs on status', filesMsg)

    return fileCounter

################################################################################

  def xmltojson(self):
      ''' The main function that takes the name of the XMLsummary file or the path to it
          as an entry parameter and creates a JSON file with the same name in the current directory '''

      JS = dict()
      JSO = dict()

      with io.open(self.xmlFileName, 'r') as file:
        # storing the lines in a list
        fileLines = file.readlines()
      # keeping only the counters lines
      fileLines = fileLines[fileLines.index(
          '\t<counters>\n'):fileLines.index('\t</counters>\n') + 1]
      # deleting the \t in the beginning of each line
      countersLines = [fileLines[i][1:] for i in range(len(fileLines))]
      # replacing Theta with Eta in order to simplify the process. It will be replaced back at the end
      countersText = ''.join(countersLines).replace('Theta', 'Eta')

      # Transforming the xml data into a dictionary
      jsonData = xmltodict.parse(countersText)
      listCounters = jsonData['counters']['counter']
      lCategory1 = list()
      lCategory2 = list()
      lCategory3 = list()
      # Selecting only the data that meet certain criteria
      for enum in enumerate(listCounters):
        if enum[1]['@name'].find('#') != -1 and enum[1]['@name'].find('Prev') == -1 and enum[1]['@name'].find('Next') == -1:  # noqa
          lCategory1.append(enum[0])
        elif enum[1]['@name'].find('/') != -1 and enum[1]['@name'].find('Original') == -1 and enum[1]['@name'].find('Unpacked') == -1 and enum[1]['@name'].find('Diff') == -1 and enum[1]['@name'].find('#') == -1 and enum[1]['@name'].find('Prev') == -1 and enum[1]['@name'].find('Next') == -1:  # noqa
          lCategory2.append(enum[0])
        elif enum[1]['@name'].find('Diff.') != -1 and enum[1]['@name'].find('Prev') == -1 and enum[1]['@name'].find('Next') == -1:  # noqa
          lCategory3.append(enum[0])
      # Filling the dictionary with the data that was kept
      for i in lCategory1:
        JS.update(xmltojsonCat1(listCounters[i:i + 1]))
      for i in range(0, len(ranges(lCategory2)), 2):
        JS.update(xmltojsonCat2(listCounters[ranges(lCategory2)[i]:ranges(lCategory2)[i + 1] + 1]))
      for i in range(0, len(ranges(lCategory3)), 2):
        if difisnotnull(xmltojsonCat3(listCounters[ranges(lCategory3)[i]:ranges(lCategory3)[i + 1] + 1])):
          JS.update(xmltojsonCat3(listCounters[ranges(lCategory3)[i]:ranges(lCategory3)[i + 1] + 1]))

      # Making the final changes in order to produce the json file
      JSO['Counters'] = JS
      text = str(JSO).replace('Eta', 'Theta')
      dico = ast.literal_eval(text)
      # Taking only the name of the file without the .xml in the end
      with io.open(self.xmlFileName[:-3] + 'json', 'w', encoding="utf-8") as fp:
        fp.write(unicode(json.dumps(dico)))

      return(dico)

################################################################################

def analyseXMLSummary(xmlFileName=None, xf_o=None, log=None, inputsOnPartOK=False):
  """Analyse a XML summary file."""

  if not xf_o:
    xf_o = XMLSummary(xmlFileName, log=log)
  return xf_o.analyse(inputsOnPartOK)

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
