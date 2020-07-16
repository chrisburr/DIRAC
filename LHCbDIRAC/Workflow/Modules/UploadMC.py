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
"""UploadMC module is used to upload to ES the json files for MC statistics."""

__RCSID__ = "$Id$"

import os
import json

from DIRAC import S_OK, S_ERROR, gLogger
from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from LHCbDIRAC.ProductionManagementSystem.Client.MCStatsClient import MCStatsClient
from LHCbDIRAC.Core.Utilities.XMLSummaries import XMLSummary


class UploadMC(ModuleBase):
  """Upload to LogSE."""

  def __init__(self):
    """Module initialization."""

    self.log = gLogger.getSubLogger("UploadMC")
    super(UploadMC, self).__init__(self.log)

    self.version = __RCSID__

  def _resolveInputVariables(self):
    """standard method for resolving the input variables."""

    super(UploadMC, self)._resolveInputVariables()

  def execute(self, production_id=None, prod_job_id=None, wms_job_id=None,
              workflowStatus=None, stepStatus=None,
              wf_commons=None, step_commons=None,
              step_number=None, step_id=None):
    """Main executon method."""
    try:

      super(UploadMC, self).execute(self.version, production_id,
                                    prod_job_id, wms_job_id,
                                    workflowStatus, stepStatus,
                                    wf_commons, step_commons,
                                    step_number, step_id)

      self._resolveInputVariables()

      # looking for json files that are 'self.jobID_Errors_appName.json'
      for app in ['Gauss', 'Boole']:
        fn = '%s_Errors_%s.json' % (self.jobID, app)
        if os.path.exists(fn):
          with open(fn) as fd:
            try:
              jsonData = json.load(fd)
              self.log.verbose("Content of JSON file", "%s: %s" % (fn, jsonData))
              if self._enableModule():
                mcLogErrorsClient = MCStatsClient()
                mcLogErrorsClient.indexName = 'lhcb-mcstats-' + self.production_id
                res = mcLogErrorsClient.set('%s-LogErrors' % app, jsonData)
                if not res['OK']:
                  self.log.error('MC Error data not set, exiting without affecting workflow status',
                                 "%s: %s" % (str(jsonData), res['Message']))
              else:
                # At this point we can see exactly what the module would have uploaded
                self.log.info("Module disabled", "would have attempted to upload the following file %s" % fn)
            except Exception as ve:
              self.log.verbose("Exception loading the JSON file: content of %s follows" % fn)
              print fd.read()
              raise ve
        else:
          self.log.info("JSON file not found", fn)

      # looking for xml files that are 'summaryGauss_self.production_id_self.prod_job_id_1.xml'
      xmlfl = 'summaryGauss_%s_%s_1.xml' % (self.production_id, self.prod_job_id)
      if os.path.exists(xmlfl):
        try:
          xmlData = XMLSummary(xmlfl)
          xmlData.xmltojson()
          # At this point 'summaryGauss_self.production_id_self.prod_job_id_1.json' should have been created
          jsonfl = 'summaryGauss_%s_%s_1.json' % (self.production_id, self.prod_job_id)
          with open(jsonfl) as JS:
            jsonData = json.load(JS)
            ids = dict()
            ids['JobID'] = self.jobID
            ids['ProductionID'] = self.production_id
            ids['prod_job_id'] = self.prod_job_id
            jsonData['ID'] = ids
            with open(jsonfl, 'w') as output:
              json.dump(jsonData, output, indent=2)

            self.log.verbose("Content of JSON file", "%s: %s" % (jsonfl, jsonData))
            if self._enableModule():
              mcLogGaussSummariesClient = MCStatsClient()
              mcLogGaussSummariesClient.indexName = 'lhcb-mcstats-GaussSummaries' + self.production_id
              res = mcLogGaussSummariesClient.set('Gauss-Summaries', jsonData)
              if not res['OK']:
                self.log.error('Gauss Summaries data not set, exiting without affecting workflow status', "%s: %s" % (str(jsonData), res['Message']))  # nopep8
            else:
              # At this point we can see exactly what the module would have uploaded
              self.log.info("Module disabled", "would have attempted to upload the following file %s" % jsonfl)
        except Exception as ve:
            self.log.verbose("Exception loading the JSON file: content of %s follows" % jsonfl)
            print JS.read()
            raise ve
      else:
        self.log.info("XML Gauss summary file not found", xmlfl)

      # looking for json files that are 'prmon_self.applicationName_self.applicationPID'
      fileName = 'prmon_%s_%s' % (self.applicationName, str(self.applicationPID))
      if self.applicationPID:
        with open(fileName + '.json') as JS:
          jsonData = json.load(JS)
          self.log.verbose("Content of JSON file", "%s: %s" % (fileName + '.json', jsonData))

        # Enriching the jsonData with the IDS
        ids = dict()
        ids['JobID'] = self.jobID
        ids['ProductionID'] = self.production_id
        ids['prod_job_id'] = self.prod_job_id
        ids['applicationName'] = self.applicationName
        ids['applicationVersion'] = self.applicationVersion
        jsonData['ID'] = ids

        # Uploading the metrics data
        mcMetricsClient = MCStatsClient()
        mcMetricsClient.indexName = 'lhcb-mcstats-Metrics-' + self.production_id
        res = mcMetricsClient.set('mcMetrics', jsonData)
        if not res['OK']:
          self.log.error('the application\'s Metrics data not set, exiting without affecting workflow status', "%s: %s" % (str(jsonData), res['Message']))  # nopep8
      else:
        self.log.info("PID not found, no data to upload")

      return S_OK()

    except Exception as e:
      self.log.exception("Failure in UploadMC execute module", lException=e)
      return S_ERROR(repr(e))

    finally:
      super(UploadMC, self).finalize(self.version)
