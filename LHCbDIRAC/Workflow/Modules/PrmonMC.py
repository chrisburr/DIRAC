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
"""PrmonMC is a module that creates metrics for a running process and uploads them to ES"""


__RCSID__ = "$Id$"

import os
import json
import psutil

from DIRAC import S_OK, S_ERROR, gLogger
from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from LHCbDIRAC.ProductionManagementSystem.Client.MCStatsClient import MCStatsClient

class PrmonMC(ModuleBase):
	"""Upload to LogSE."""

	def __init__(self):
		"""Module initialization."""

		self.log = gLogger.getSubLogger("PrmonMC")
    super(PrmonMC, self).__init__(self.log)
		self.version = __RCSID__

	def createData():
		"""Method for creating the metrics data of an application."""

		if self.applicationPID:
			# Specifying the name of the output files
			fileName = 'prmon_%s_%s_%s' % (self.applicationName, self.production_id, self.prod_job_id)

			# Specifying the command that runs prmon given the PID of the application
			cmdPRMON =  "prmon --pid %s --filename %s.txt --json-summary %s.json" % (self.applicationPID, fileName, fileName)
			os.system(cmd)
			# this command will end once the application process is no longer running

			with open(fileName + '.json') as JS:
	      self.jsonData = json.load(JS)
	      self.log.verbose("Content of JSON file", "%s: %s" % (fileName + '.json', jsonData))

	      # Enriching the jsonData with the IDS
	      ids = dict()
	      ids['JobID'] = self.jobID
	      ids['ProductionID'] = self.production_id
	      ids['prod_job_id'] = self.prod_job_id
	      ids['applicationName'] = self.applicationName
	      ids['applicationVersion'] = self.applicationVersion
	      self.jsonData['ID'] = ids
	  else:
	  	self.log.info("PID not found, no data to create")

  def uploadData():
  	"""Method for uploading the metrics data of the application to ES."""

  	if self.applicationPID:
	  	mcMetricsClient = MCStatsClient()
	  	mcMetricsClient.indexName = 'lhcb-mcstats-Metrics-' + self.production_id
	  	res = mcMectricsClient.set('mcMetrics', self.jsonData)
	  	if not res['OK']:
	      self.log.error('the application\'s Metrics data not set, exiting without affecting workflow status', "%s: %s" % (str(jsonData), res['Message']))  # nopep8
	  else:
	  	self.log.info("PID not found, no data to upload")
