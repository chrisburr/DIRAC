
==========================
Full Configuration Example
==========================

.. This file is created by docs/Tools/UpdateDiracCFG.py

Below is a complete example configuration with anotations for some sections::

  Operations
  {
    Defaults
    {
      Bookkeeping
      {
        AnOption = True
      }
    }
  }
  Systems
  {
    Bookkeeping
    {
      Services
      {
        BookkeepingManager
        {
          Port = 9167
          Authorization
          {
            Default = all
          }
        }
      }
    }
    DataManagement
    {
      Services
      {
        DataIntegrity
        {
          Port = 9150
          Authorization
          {
            Default = all
          }
        }
        RAWIntegrity
        {
          Port = 9198
          Authorization
          {
            Default = all
          }
        }
        StorageUsage
        {
          BasePath = storage
          Port = 9151
          Authorization
          {
            Default = all
          }
        }
        DataUsage
        {
          Port = 9164
          Authorization
          {
            Default = all
          }
        }
      }
      Agents
      {
        PopularityAgent
        {
          PollingTime = 43200
        }
        PopularityAnalysisAgent
        {
          #every week
          PollingTime = 604800
          AnalysisPeriod = 910
          TopDirectory = '/lhcb'
          DataPopularityURL = 'http://localhost:5000'
          MailRecipients = 'lhcb-datamanagement@cern.ch'
          MailEnabled = True
          SavedSpaceTarget = 100
          MinReplicas = 1
          MaxReplicas = 7
        }
        RAWIntegrityAgent
        {
          PollingTime = 60
        }
        SEUsageAgent
        {
          PollingTime = 43200
          MaximumDelaySinceStorageDump = 86400
        }
        StorageHistoryAgent
        {
          PollingTime = 43200
        }
        StorageUsageAgent
        {
          PollingTime = 43200
          BaseDirectory = /lhcb
          Ignore = /lhcb/user
          Ignore += /lhcb/test
        }
        TargzJobLogAgent
        {
          PollingTime = 3600
          Actions = Jobs
          JobAgeDays = 14
          ProdAgeDays = 14
          ProductionGlob = 000?????
        }
        UserStorageQuotaAgent
        {
          PollingTime = 172800
        }
        UserStorageUsageAgent
        {
          PollingTime = 43200
          ProxyLocation = runit/DataManagement/UserStorageUsageAgent/proxy
          UseProxies = True
          BaseDirectory = /lhcb/user
        }
      }
    }
    ProductionManagement
    {
      Services
      {
        MCStatsElasticDB
        {
          Port = 9177
          Authorization
          {
            Default = authenticated
          }
        }
        ProductionRequest
        {
          Port = 9163
          Authorization
          {
            Default = all
          }
        }
      }
      Agents
      {
        ProductionStatusAgent
        {
          PollingTime = 120
        }
        RequestTrackingAgent
        {
          PollingTime = 120
        }
        NotifyAgent
        {
          PollingTime = 1800
        }
      }
    }
    ResourceStatus
    {
      Agents
      {
        NagiosTopologyAgent
        {
          PollingTime = 3000
          DryRun = True
        }
        SLSAgent
        {
          PollingTime = 1800
        }
        ShiftDBAgent
        {
          PollingTime = 3600
        }
      }
    }
    Transformation
    {
      Services
      {
        TransformationManager
        {
          Port = 9131
          HandlerPath = LHCbDIRAC/TransformationSystem/Service/TransformationManagerHandler.py
          Authorization
          {
            Default = all
          }
        }
      }
      Agents
      {
        DataRecoveryAgent
        {
          PollingTime = 120
        }
        BookkeepingWatchAgent
        {
          PollingTime = 120
        }
        MCSimulationTestingAgent
        {
          PollingTime = 300
        }
      }
    }
    WorkloadManagement
    {
      Services
      {
        WMSSecureGW
        {
          Port = 3424
          HandlerPath = LHCbDIRAC/WorkloadManagementSystem/Service/WMSSecureGWHandler.py
          Authorization
          {
            Default = all
          }
        }
      }
      Agents
      {
        BKInputDataAgent
        {
          PollingTime = 30
        }
        AncestorFilesAgent
        {
          PollingTime = 30
        }
      }
    }
  }
