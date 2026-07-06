""" DirectoryUsageAggregatorAgent folds the FileCatalog directory-usage journal into the
FC_DirectoryUsage counter table.

File registration (and deletion / replica operations) record their per-directory usage
deltas in an append-only journal (FC_DirectoryUsageJournal) instead of updating the shared
FC_DirectoryUsage counter row directly. That removes the single-hot-row lock contention that
otherwise serialised concurrent single-file registrations into the same directory. This
agent periodically drains the journal, folding the pending deltas into FC_DirectoryUsage.
The usage read paths add the not-yet-aggregated journal deltas on the fly, so reported
sizes stay exact regardless of how far behind the aggregation is.

This agent is only relevant when the FileCatalog uses the stored-procedure backend
(FileManager = FileManagerPs, DirectoryManager = DirectoryClosure); with other backends the
call is a harmless no-op.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN DirectoryUsageAggregatorAgent
  :end-before: ##END
  :dedent: 2
  :caption: DirectoryUsageAggregatorAgent options

"""
from DIRAC import S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.DataManagementSystem.DB.FileCatalogDB import FileCatalogDB


class DirectoryUsageAggregatorAgent(AgentModule):
    """Periodically fold the FileCatalog directory-usage journal into FC_DirectoryUsage."""

    def __init__(self, *args, **kwargs):
        """c'tor"""
        super().__init__(*args, **kwargs)

        self.fcDB = None

    def initialize(self):
        """Set up the FileCatalog DB"""
        self.fcDB = FileCatalogDB()
        return S_OK()

    def execute(self):
        """Trigger one aggregation cycle"""
        result = self.fcDB.aggregateDirectoryUsageJournal()
        if not result["OK"]:
            self.log.error("Failed to aggregate directory usage journal", result["Message"])
            return result

        return S_OK()
