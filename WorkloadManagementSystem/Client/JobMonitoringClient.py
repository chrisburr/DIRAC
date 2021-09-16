""" Class that contains client access to the job monitoring handler. """

from __future__ import absolute_import

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.Core.Utilities.JEncode import strToIntDict


@createClient("WorkloadManagement/JobMonitoring")
class JobMonitoringClient(Client):
    def __init__(self, **kwargs):

        super(JobMonitoringClient, self).__init__(**kwargs)
        self.setServer("WorkloadManagement/JobMonitoring")

    @deprecated("Unused")
    def traceJobParameters(
        self,
        site,
        localID,
        parameterList=None,
        attributeList=None,
        date=None,
        until=None,
    ):
        return self._getRPC().traceJobParameters(
            site, localID, parameterList, attributeList, date, until
        )

    @deprecated("Unused")
    def traceJobParameter(self, site, localID, parameter, date=None, until=None):
        return self._getRPC().traceJobParameter(site, localID, parameter, date, until)

    @ignoreEncodeWarning
    def getJobsStatus(self, jobIDs):
        res = self._getRPC().getJobsStatus(jobIDs)

        # Cast the str keys to int
        if res["OK"]:
            res["Value"] = strToIntDict(res["Value"])
        return res

    @ignoreEncodeWarning
    def getJobParameters(self, jobIDs, parName=None):
        res = self._getRPC().getJobParameters(jobIDs, parName)

        # Cast the str keys to int
        if res["OK"]:
            res["Value"] = strToIntDict(res["Value"])
        return res

    @ignoreEncodeWarning
    def getJobsParameters(self, jobIDs, parameters):
        res = self._getRPC().getJobsParameters(jobIDs, parameters)

        # Cast the str keys to int
        if res["OK"]:
            res["Value"] = strToIntDict(res["Value"])
        return res

    @ignoreEncodeWarning
    def getJobsMinorStatus(self, jobIDs):
        res = self._getRPC().getJobsMinorStatus(jobIDs)

        # Cast the str keys to int
        if res["OK"]:
            res["Value"] = strToIntDict(res["Value"])
        return res

    @ignoreEncodeWarning
    def getJobsApplicationStatus(self, jobIDs):
        res = self._getRPC().getJobsApplicationStatus(jobIDs)

        # Cast the str keys to int
        if res["OK"]:
            res["Value"] = strToIntDict(res["Value"])
        return res

    @ignoreEncodeWarning
    def getJobsSites(self, jobIDs):
        res = self._getRPC().getJobsSites(jobIDs)

        # Cast the str keys to int
        if res["OK"]:
            res["Value"] = strToIntDict(res["Value"])
        return res

    @ignoreEncodeWarning
    def getJobsPrimarySummary(self, jobIDs):
        res = self._getRPC().getJobsPrimarySummary(jobIDs)

        # Cast the str keys to int
        if res["OK"]:
            res["Value"] = strToIntDict(res["Value"])
        return res
