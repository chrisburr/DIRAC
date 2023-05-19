from diracx.client import Dirac
from diracx.client.models import JobSearchParams

from diracx.cli.utils import get_auth_headers

from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue


def fetch(parameters, jobIDs):
    # breakpoint()
    with Dirac(endpoint="http://localhost:8000") as api:
        jobs = api.jobs.search(
            parameters=["JobID"] + parameters,
            search=[{"parameter": "JobID", "operator": "in", "values": jobIDs}],
            headers=get_auth_headers(),
        )
        return {j["JobID"]: {param: j[param] for param in parameters} for j in jobs}


class JobMonitoringClient:
    def __init__(self, *args, **kwargs):
        """TODO"""

    @convertToReturnValue
    def getJobsMinorStatus(self, jobIDs):
        return fetch(["MinorStatus"], jobIDs)

    @convertToReturnValue
    def getJobsStates(self, jobIDs):
        return fetch(["Status", "MinorStatus", "ApplicationStatus"], jobIDs)

    @convertToReturnValue
    def getJobsSites(self, jobIDs):
        return fetch(["Site"], jobIDs)
