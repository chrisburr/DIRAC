########################################################################
# $Id: JobWrapper.py,v 1.42 2008/06/16 14:02:07 atsareg Exp $
# File :   JobWrapper.py
# Author : Stuart Paterson
########################################################################

""" The Job Wrapper Class is instantiated with arguments tailored for running
    a particular job. The JobWrapper starts a thread for execution of the job
    and a Watchdog Agent that can monitor progress.
"""

__RCSID__ = "$Id: JobWrapper.py,v 1.42 2008/06/16 14:02:07 atsareg Exp $"

from DIRAC.DataManagementSystem.Client.ReplicaManager               import ReplicaManager
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog               import PoolXMLCatalog
from DIRAC.RequestManagementSystem.Client.RequestContainer          import RequestContainer
from DIRAC.RequestManagementSystem.Client.RequestClient             import RequestClient
from DIRAC.WorkloadManagementSystem.Client.SandboxClient            import SandboxClient
from DIRAC.WorkloadManagementSystem.JobWrapper.WatchdogFactory      import WatchdogFactory
from DIRAC.AccountingSystem.Client.Types.Job                        import Job as AccountingJob
from DIRAC.ConfigurationSystem.Client.PathFinder                    import getSystemSection
from DIRAC.WorkloadManagementSystem.Client.JobReport                import JobReport
from DIRAC.Core.DISET.RPCClient                                     import RPCClient
from DIRAC.Core.Utilities.ModuleFactory                             import ModuleFactory
from DIRAC.Core.Utilities.Subprocess                                import shellCall
from DIRAC.Core.Utilities.Subprocess                                import Subprocess
from DIRAC.Core.Utilities.File                                      import getGlobbedTotalSize
from DIRAC                                                          import S_OK, S_ERROR, gConfig, gLogger
import DIRAC

import os, re, sys, string, time, shutil, threading, tarfile, glob

EXECUTION_RESULT = {}

class JobWrapper:

  #############################################################################
  def __init__(self, jobID=None, jobReport=None):
    """ Standard constructor
    """
    self.section = getSystemSection('WorkloadManagement/JobWrapper')
    self.log = gLogger
    #Create the acctounting report
    self.accountingReport = AccountingJob()
    # Initialize for accounting
    self.wmsMajorStatus = "unknown"
    self.wmsMinorStatus = "unknown"
    #Set now as start time
    self.accountingReport.setStartTime()
    if not jobID:
      self.jobID=0
    else:
      self.jobID = jobID
    if jobReport:
      self.jobReport = jobReport
    else:
      self.jobReport = JobReport(self.jobID,'JobWrapper')

    # FIXME: this info is in DIRAC.rootPath
    self.root = os.getcwd()
    self.localSiteRoot = gConfig.getValue('/LocalSite/Root',self.root)
    self.__loadLocalCFGFiles(self.localSiteRoot)
    # FIXME why not use DIRAC.version
    self.diracVersion = 'DIRAC version v%dr%d build %d' %(DIRAC.majorVersion,DIRAC.minorVersion,DIRAC.patchLevel)
    self.maxPeekLines = gConfig.getValue(self.section+'/MaxJobPeekLines',20)
    self.defaultCPUTime = gConfig.getValue(self.section+'/DefaultCPUTime',600)
    self.defaultOutputFile = gConfig.getValue(self.section+'/DefaultOutputFile','std.out')
    self.defaultErrorFile = gConfig.getValue(self.section+'/DefaultErrorFile','std.err')
    # FIXME: should use gConfig.getValue(self.section+'/DiskSE',['-disk','-DST','-USER'] )
    self.diskSE            = gConfig.getValue(self.section+'/DiskSE','-disk,-DST,-USER')
    if type(self.diskSE) == type(' '):
      self.diskSE = self.diskSE.split(',')
    # FIXME: see above
    self.tapeSE            = gConfig.getValue(self.section+'/TapeSE','-tape,-RDST,-RAW')
    if type(self.tapeSE) == type(' '):
      self.tapeSE = self.tapeSE.split(',')
    self.cleanUpFlag  = gConfig.getValue(self.section+'/CleanUpFlag',False)
    self.localSite = gConfig.getValue('/LocalSite/Site','Unknown')
    self.pilotRef = gConfig.getValue('/LocalSite/PilotReference','Unknown')
    self.vo = gConfig.getValue('/DIRAC/VirtualOrganization','lhcb')
    self.bufferLimit = gConfig.getValue(self.section+'/BufferLimit',10485760)
    self.defaultOutputSE = gConfig.getValue(self.section+'/DefaultOutputSE','CERN-FAILOVER')
    self.rm = ReplicaManager()
    self.log.verbose('===========================================================================')
    self.log.verbose('CVS version %s' %(__RCSID__))
    self.log.verbose(self.diracVersion)
    self.log.verbose('Developer tag: 2')
    self.currentPID = os.getpid()
    self.log.verbose('Job Wrapper started under PID: %s' % self.currentPID )
    self.log.verbose('==========================================================================')
    self.log.verbose('sys.path is: \n%s' %(string.join(sys.path,'\n')))
    self.log.verbose('==========================================================================')
    if not os.environ.has_key('PYTHONPATH'):
      self.log.verbose('PYTHONPATH is: null')
    else:
      pypath = os.environ['PYTHONPATH']
      self.log.verbose('PYTHONPATH is: \n%s' %(string.join(string.split(pypath,':'),'\n')))
      self.log.verbose('==========================================================================')
    if os.environ.has_key('LD_LIBRARY_PATH_SAVE'):
      if os.environ.has_key('LD_LIBRARY_PATH'):
        os.environ['LD_LIBRARY_PATH'] = os.environ['LD_LIBRARY_PATH']+':'+os.environ['LD_LIBRARY_PATH_SAVE']
      else:
        os.environ['LD_LIBRARY_PATH'] = os.environ['LD_LIBRARY_PATH_SAVE']

    if not os.environ.has_key('LD_LIBRARY_PATH'):
      self.log.verbose('LD_LIBRARY_PATH is: null')
    else:
      ldpath = os.environ['LD_LIBRARY_PATH']
      self.log.verbose('LD_LIBRARY_PATH is: \n%s' %(string.join(string.split(ldpath,':'),'\n')))
      self.log.verbose('==========================================================================')

    if not self.cleanUpFlag:
      self.log.verbose('CleanUp Flag is disabled by configuration')
    self.log.verbose('Trying to import LFC File Catalog client')
    try:
      from DIRAC.DataManagementSystem.Client.Catalog.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
      self.fileCatalog = LcgFileCatalogCombinedClient()
    except Exception,x:
      msg = 'Failed to create LcgFileCatalogClient with exception:'
      self.log.fatal(msg)
      self.log.fatal(str(x))
    #Set defaults for some global parameters to be defined for the accounting report
    self.owner='unknown'
    self.jobGroup='unknown'
    self.jobType='unknown'
    self.processingType='uknown'
    self.userGroup='unknown'
    self.jobClass='unknown'
    self.inputDataFiles=0
    self.outputDataFiles=0
    self.inputDataSize=0
    self.inputSandboxSize=0
    self.outputSandboxSize=0
    self.outputDataSize=0
    self.diskSpaceConsumed=0
    self.processedEvents = 0

  #############################################################################
  def initialize(self, arguments):
    """ Initializes parameters and environment for job.
    """
    self.__report('Running','Job Initialization')
    self.log.info('Starting Job Wrapper Initialization for Job %s' %(self.jobID))
    jobArgs = arguments['Job']
    self.log.verbose(jobArgs)
    ceArgs = arguments ['CE']
    self.log.verbose(ceArgs)
    self.__setInitialJobParameters(arguments)

    #Fill some parameters for the accounting report
    if jobArgs.has_key('Owner'):
      self.owner=jobArgs['Owner']
    if jobArgs.has_key('JobGroup'):
      self.jobGroup=jobArgs['JobGroup']
    if jobArgs.has_key('JobType'):
      self.jobType=jobArgs['JobType']
    if jobArgs.has_key('InputData'):
      dataParam=jobArgs['InputData']
      if dataParam and not type(dataParam)==type([]):
        dataParam=[dataParam]
      self.inputDataFiles=len(dataParam)
    if jobArgs.has_key('OutputData'):
      dataParam=jobArgs['OutputData']
      if dataParam and not type(dataParam)==type([]):
        dataParam=[dataParam]
      self.outputDataFiles=len(dataParam)
    if jobArgs.has_key('ProcessingType'):
      self.processingType=jobArgs['ProcessingType']
    if jobArgs.has_key('OwnerGroup'):
      self.userGroup=jobArgs['OwnerGroup']
    if jobArgs.has_key('JobSplitType'):
      self.jobClass=jobArgs['JobSplitType']

    # Prepare the working directory and cd to there
    if self.jobID:
      if os.path.exists(self.jobID):
        shutil.rmtree(str(self.jobID))
      os.mkdir(str(self.jobID))
      os.chdir(str(self.jobID))
    else:
      self.log.info('JobID is not defined, running in current directory')

  #############################################################################
  def __loadLocalCFGFiles(self,localRoot):
    """Loads any extra CFG files residing in the local DIRAC site root.
    """
    files = os.listdir(localRoot)
    self.log.debug('Checking directory %s for *.cfg files' %localRoot)
    for i in files:
      if re.search('.cfg$',i):
        gConfig.loadFile('%s/%s' %(localRoot,i))
        self.log.debug('Found local .cfg file %s' %i)

  #############################################################################
  def execute(self, arguments):
    """The main execution method of the Job Wrapper
    """
    self.log.info('Job Wrapper is starting execution phase for job %s' %(self.jobID))
    os.environ['DIRACROOT'] = self.localSiteRoot
    self.log.verbose('DIRACROOT = %s' %(self.localSiteRoot))
    os.environ['DIRACPYTHON'] = sys.executable
    self.log.verbose('DIRACPYTHON = %s' %(sys.executable))
    os.environ['DIRACSITE'] = self.localSite
    self.log.verbose('DIRACSITE = %s' %(self.localSite))

    jobArgs = arguments['Job']
    ceArgs = arguments ['CE']

    if jobArgs.has_key('MaxCPUTime'):
      jobCPUTime = int(jobArgs['MaxCPUTime'])
    else:
      self.log.info('Job %s has no CPU time limit specified, applying default of %s' %(self.jobID,self.defaultCPUTime))
      jobCPUTime = self.defaultCPUTime

    if jobArgs.has_key('Executable'):
      executable = jobArgs['Executable']
    else:
      msg = 'Job %s has no specified executable' %(self.jobID)
      self.log.warn(msg)
      return S_ERROR(msg)

    jobArguments = ' '
    if jobArgs.has_key('Arguments'):
      jobArguments = jobArgs['Arguments']

    executable = os.path.expandvars(executable)
    exeThread = None
    spObject = None

    if re.search('DIRACROOT',executable):
      executable = executable.replace('$DIRACROOT',self.localSiteRoot)
      self.log.verbose('Replaced $DIRACROOT for executable as %s' %(self.localSiteRoot))

    if os.path.exists(executable):
      self.__report('Running','Application')
      spObject = Subprocess(timeout=False,bufferLimit=int(self.bufferLimit))
      command = '%s %s' % (executable,jobArguments)
      self.log.verbose('Execution command: %s' %(command))
      maxPeekLines = self.maxPeekLines
      exeThread = ExecutionThread(spObject,command,maxPeekLines)
      exeThread.start()
    else:
      return S_ERROR('Path to executable %s not found' %(executable))

    watchdogFactory = WatchdogFactory()
    watchdogInstance = watchdogFactory.getWatchdog(self.currentPID, exeThread, spObject, jobCPUTime)
    if not watchdogInstance['OK']:
      self.log.warn(watchdogInstance['Message'])
      return S_ERROR('Could not create Watchdog instance')

    self.log.verbose('WatchdogInstance %s' %(watchdogInstance))
    watchdog = watchdogInstance['Value']
    self.log.verbose('Calibrating Watchdog instance')
    watchdog.calibrate()
    if exeThread.isAlive():
      self.log.info('Application thread is started in Job Wrapper')
      watchdog.run()
    else:
      self.log.warn('Application thread stopped very quickly...')

    if exeThread.isAlive():
      self.log.warn('Watchdog exited before completion of execution thread')
      while exeThread.isAlive():
        time.sleep(5)

    outputs = None
    if EXECUTION_RESULT.has_key('Thread'):
      threadResult = EXECUTION_RESULT['Thread']
      if not threadResult['OK']:
        self.log.warn(threadResult['Message'])
      else:
        outputs = threadResult['Value']

    if outputs:
      errorFileName = self.defaultErrorFile
      outputFileName = self.defaultOutputFile
      status = threadResult['Value'][0]
      stdout = threadResult['Value'][1]
      stderr = threadResult['Value'][2]
      #Send final heartbeat of a configurable number of lines here
      self.log.verbose('Sending final application standard output heartbeat')
      self.__sendFinalStdOut(stdout)
      self.log.verbose('Execution thread status = %s' %(status))
      if jobArgs.has_key('StdError'):
        errorFileName = jobArgs['StdError']
      if jobArgs.has_key('StdOutput'):
        outputFileName = jobArgs['StdOutput']
      self.log.verbose('Writing stdOutput to %s' %(outputFileName))
      outputFile = open(outputFileName,'w')
      print >> outputFile, stdout
      outputFile.close()
      self.log.verbose('Writing stdError to %s' %(errorFileName))
      errorFile = open(errorFileName,'w')
      print >> errorFile, stderr
      errorFile.close()
    else:
      self.log.warn('No outputs generated from job execution')
      toCheck = os.listdir(os.getcwd())
      for directory in toCheck:
        if os.path.isdir(directory):
          self.log.verbose('Files in directory %s are:' %(directory))
          for i in os.listdir(directory): print i
        else:
          self.log.verbose('File %s' %(directory))

    return S_OK()

  #############################################################################
  def __sendFinalStdOut(self,stdout):
    """After the Watchdog process has finished, this function sends a final
       report to be presented in the StdOut in the web page via the heartbeat
       mechanism.
    """
    cpuConsumed = self.__getCPU()['Value']
    self.log.info('Total CPU Consumed is: %s' %(cpuConsumed))
    #TODO: add cpu units after normalization
    splitRes = stdout.split('\n')
    appStdOut = ''
    if len(splitRes)>self.maxPeekLines:
      appStdOut = string.join(splitRes[len(splitRes)-self.maxPeekLines:],'\n')
    else:
      self.log.verbose('Standard output is less than %s lines long' %(self.maxPeekLines))
      appStdOut = stdout

    curTime = time.asctime(time.gmtime())
    header = 'Last %s lines of application output from JobWrapper on %s [UTC]:' % (self.maxPeekLines,curTime)
    border = ''
    for i in xrange(len(header)):
      border+='='
    cpuTotal = 'CPU Total for job is %s (h:m:s)' %(cpuConsumed)
    header = '\n%s\n%s\n%s\n%s\n' % (border,header,cpuTotal,border)
    appStdOut = header+appStdOut
    self.log.info(appStdOut)
    heartBeatDict = {}
    staticParamDict = {'StandardOutput':appStdOut}
    if self.jobID:
      jobReport  = RPCClient('WorkloadManagement/JobStateUpdate')
      result = jobReport.sendHeartBeat(int(self.jobID),heartBeatDict,staticParamDict)
      if not result['OK']:
        self.log.warn('Problem sending final heartbeat standard output from JobWrapper')
        self.log.warn(result)

    return result

  #############################################################################
  def __getCPU(self):
    """Uses os.times() to get CPU time and returns HH:MM:SS after conversion.
    """
    #TODO: normalize CPU consumed via scale factor
    utime, stime, cutime, cstime, elapsed = EXECUTION_RESULT['CPU']
    cpuTime = utime + stime + cutime + cstime
    self.log.verbose("Total CPU time consumed = %s" % (cpuTime))
    result = self.__getCPUHMS(cpuTime)
    return result

  #############################################################################
  def __getCPUHMS(self,cpuTime):
    mins, secs = divmod(cpuTime, 60)
    hours, mins = divmod(mins, 60)
    humanTime = '%02d:%02d:%02d' % (hours, mins, secs)
    self.log.verbose('Human readable CPU time is: %s' %humanTime)
    self.__setJobParam('TotalCPUTime(s)',cpuTime)
    return S_OK(humanTime)

  #############################################################################
  def resolveInputData(self,arguments):
    """Input data is resolved here for the first iteration of SRM2 testing.
    """
    self.__report('Running','Input Data Resolution')

    jobArgs = arguments['Job']
    if not jobArgs.has_key('InputData'):
      msg = 'Could not obtain job input data requirement from available parameters'
      self.log.warn(msg)
      return S_ERROR(msg)

    ceArgs = arguments['CE']
    if not ceArgs.has_key('LocalSE'):
      csLocalSE = gConfig.getValue('LocalSite/LocalSE','')
      if not csLocalSE:
        msg = 'Job has input data requirement but no site LocalSE defined'
        self.log.warn(msg)
        return S_ERROR(msg)
      else:
        ceArgs['LocalSE'] = csLocalSE

    inputData = jobArgs['InputData']
    self.log.verbose('Input Data is: \n%s' %(inputData))
    if type(inputData)==type(' '):
      inputData = [inputData]

    localSEList = ceArgs['LocalSE']
    if type(localSEList)==type(' '):
      localSEList=localSEList.split(',')

    msg = 'Job Wrapper cannot resolve input data with null '
    if not inputData:
      msg += 'job input data parameter '
      self.log.warn(msg)
      return S_ERROR(msg)
    if not localSEList:
      msg += 'site localSE list'
      self.log.warn(msg)
      return S_ERROR(msg)

    if not jobArgs.has_key('InputDataModule'):
      msg = 'Job has no input data resolution module'
      self.log.warn(msg)
      return S_ERROR(msg)

    inputDataPolicy = jobArgs['InputDataModule']
    self.log.verbose('Job input data requirement is \n%s' %(string.join(inputData,',\n')))
    self.log.verbose('Job input data resolution policy module is %s' %(inputDataPolicy))
    self.log.info('Site has the following local SEs: %s' %(string.join(localSEList,', ')))
    lfns = [string.replace(fname,'LFN:','') for fname in inputData]

    optReplicas = {}
    optGUIDs = {}
    if arguments.has_key('Optimizer'):
      optArgs = arguments['Optimizer']
      optDict = None
      try:
        optDict = eval(optArgs['InputData'])
        optReplicas = optDict['Value']
        self.log.info('Found optimizer catalogue result')
        self.log.verbose(optReplicas)
      except Exception,x:
        optDict = None
        self.log.warn(str(x))
        self.log.warn('Optimizer information could not be converted to a dictionary will call catalogue directly')

    resolvedData = {}
    result = self.__checkFileCatalog(lfns,localSEList,optReplicas)
    if not result['OK']:
      self.log.info('Could not obtain replica information from Optimizer File Catalog information')
      self.log.warn(result)
      result = self.__checkFileCatalog(lfns,localSEList)
      if not result['OK']:
        self.log.warn('Could not obtain replica information from File Catalog directly')
        self.log.warn(result)
        return S_ERROR(result['Message'])
      else:
        resolvedData = result
    else:
      resolvedData = result

    #add input data size to accounting report (since resolution successful)
    for lfn,mdata in resolvedData['Value']['Successful'].items():
      if mdata.has_key('Size'):
        lfnSize = mdata['Size']
        if not type(lfnSize)==type(long(1)):
          try:
            lfnSize = long(lfnSize)
          except Exception,x:
            lfnSize = 0
            self.log.info('File size for LFN:%s was not a long integer, setting size to 0' %(lfn))
        self.inputDataSize+=lfnSize

    configDict = {'JobID':self.jobID,'LocalSEList':localSEList,'DiskSEList':self.diskSE,'TapeSEList':self.tapeSE}
    self.log.info(configDict)
    argumentsDict = {'FileCatalog':resolvedData,'Configuration':configDict,'InputData':lfns}
    self.log.info(argumentsDict)
    moduleFactory = ModuleFactory()
    moduleInstance = moduleFactory.getModule(inputDataPolicy,argumentsDict)
    if not moduleInstance['OK']:
      return moduleInstance

    module = moduleInstance['Value']
    result = module.execute()
    if not result['OK']:
      self.log.warn('Input data resolution failed')
      self.__report('Failed','Input Data Resolution')
      return result

    return S_OK()

  #############################################################################
  def __checkFileCatalog(self,lfns,localSEList,optReplicaInfo=None):
    """This function returns dictionaries containing all relevant parameters
       to allow data access from the relevant file catalogue.  Optionally, optimizer
       parameters can be supplied here but if these are not sufficient, the file catalogue
       is subsequently consulted.

       N.B. this will be considerably simplified when the DMS evolves to have a
       generic FC interface and a single call for all available information.
    """
    replicas = optReplicaInfo
    if not replicas:
      replicas = self.__getReplicaMetadata(lfns)
      if not replicas['OK']:
        return replicas

    self.log.verbose(replicas)
    failedReplicas = []
    pfnList = []
    originalReplicaInfo = replicas

    #First make a check in case replicas have been removed from the local site
    for lfn,reps in replicas['Value']['Successful'].items():
      localReplica = False
      for localSE in localSEList:
        if reps.has_key(localSE):
          localReplica = True
      if not localReplica:
        failedReplicas.append(lfn)

    #Check that all LFNs have at least one replica and GUID
    if failedReplicas:
      #in principle this is not a failure but depends on the policy of the VO
      #datasets can be downloaded from another site
      self.log.info('The following file(s) were found not to have replicas for available LocalSEs:\n%s' %(string.join(failedReplicas,',\n')))

    failedGUIDs = []
    for lfn,reps in replicas['Value']['Successful'].items():
      if not lfn in failedReplicas:
        if not reps.has_key('GUID'):
          failedGUIDs.append(lfn)

    if failedGUIDs:
      self.log.info('The following file(s) were found not to have a GUID:\n%s' %(string.join(failedGUIDs,',\n')))

    return replicas

  #############################################################################
  def __getReplicaMetadata(self,lfns):
    """ Wrapper function to consult LFC for all necessary file metadata
        and check the result.  To be revisited when file catalogue interface
        is available and when all info can be returned from a single call.
    """
    start = time.time()
    repsResult = self.fileCatalog.getReplicas(lfns)
    timing = time.time() - start
    self.log.info('Replica Lookup Time: %.2f seconds ' % (timing) )
    if not repsResult['OK']:
      self.log.warn(repsResult['Message'])
      return repsResult

    badLFNCount = 0
    badLFNs = []
    catalogResult = repsResult['Value']

    if catalogResult.has_key('Failed'):
      for lfn,cause in catalogResult['Failed'].items():
        badLFNCount+=1
        badLFNs.append('LFN:%s Problem: %s' %(lfn,cause))

    if catalogResult.has_key('Successful'):
      for lfn,replicas in catalogResult['Successful'].items():
        if not replicas:
          badLFNCount+=1
          badLFNs.append('LFN:%s Problem: Null replica value' %(lfn))

    if badLFNCount:
      self.log.warn('Job Wrapper found %s problematic LFN(s) for job %s' % (badLFNCount,self.jobID))
      param = string.join(badLFNs,'\n')
      self.log.info(param)
      result = self.__setJobParam('MissingLFNs',param)
      return S_ERROR('Input Data Not Available')

    #Must retrieve GUIDs from LFC for files
    start = time.time()
    guidDict = self.fileCatalog.getFileMetadata(lfns)
    timing = time.time() - start
    self.log.info('GUID Lookup Time: %.2f seconds ' % (timing) )
    if not guidDict['OK']:
      self.log.warn('Failed to retrieve GUIDs from file catalogue')
      self.log.warn(guidDict['Message'])
      return guidDict

    failed = guidDict['Value']['Failed']
    if failed:
      self.log.warn('Could not retrieve GUIDs from catalogue for the following files')
      self.log.warn(failed)
      return S_ERROR('Missing GUIDs')

    for lfn,reps in repsResult['Value']['Successful'].items():
      guidDict['Value']['Successful'][lfn].update(reps)

    catResult = guidDict
    return catResult

  #############################################################################
  def processJobOutputs(self,arguments):
    """Outputs for a job may be treated here.
    """
    self.__report('Completed','Uploading Job Outputs')
    jobArgs = arguments['Job']

    #first iteration of this, no checking of wildcards or oversize sandbox files etc.
    outputSandbox = []
    if jobArgs.has_key('OutputSandbox'):
      outputSandbox = jobArgs['OutputSandbox']
      self.log.verbose('OutputSandbox files are: %s' %(string.join(outputSandbox,', ')))
    outputData = []
    if jobArgs.has_key('OutputData'):
      outputData = jobArgs['OutputData']
      if not type(outputData) == type([]):
        outputData = string.split(outputData,';')
      self.log.verbose('OutputData files are: %s' %(string.join(outputData,', ')))

    #First resolve any wildcards for output files and work out if any files are missing
    resolvedSandbox = self.__resolveOutputSandboxFiles(outputSandbox)
    if not resolvedSandbox['OK']:
      self.log.warn('Output sandbox file resolution failed:')
      self.log.warn(result['Message'])
      self.__report('Failed','Resolving Output Sandbox')

    fileList = resolvedSandbox['Value']['Files']
    missingFiles = resolvedSandbox['Value']['Missing']
    if missingFiles:
      self.__setJobParam('OutputSandbox','MissingFiles: %s' %(string.join(missingFiles,', ')))

    self.__report('Completed','Uploading Output Sandbox')
    if fileList and self.jobID:
      self.outputSandboxSize = getGlobbedTotalSize(fileList)
      outputSandboxClient = SandboxClient('Output')
      result = outputSandboxClient.sendFiles(self.jobID, fileList)
      if not result['OK']:
        self.log.warn('Output sandbox upload failed:')
        self.log.warn(result['Message'])

    if jobArgs.has_key('Owner'):
      owner = jobArgs['Owner']
    else:
      msg = 'Job has no owner specified'
      self.log.warn(msg)
      return S_OK(msg)

    if jobArgs.has_key('OutputSE'):
      outputSE = jobArgs['OutputSE']
    else:
      outputSE = self.defaultOutputSE

    if outputData:
      result = self.__transferOutputDataFiles(owner,outputData,outputSE)
      if not result['OK']:
        return result

    return S_OK('Job outputs processed')

  #############################################################################
  def __resolveOutputSandboxFiles(self,outputSandbox):
    """Checks the output sandbox file list and resolves any specified wildcards.
       Also tars any specified directories.
    """
    missing = []
    okFiles = []
    for i in outputSandbox:
      self.log.verbose('Looking at OutputSandbox file/directory/wildcard: %s' %i)
      globList = glob.glob(i)
      for check in globList:
        if os.path.isfile(check):
          self.log.verbose('Found locally existing OutputSandbox file: %s' %check)
          okFiles.append(check)
        if os.path.isdir(check):
          self.log.verbose('Found locally existing OutputSandbox directory: %s' %check)
          cmd = 'tar cf %s.tar %s' %(check,check)
          result = shellCall(60,cmd)
          if not result['OK']:
            self.log.warn(result)
          if os.path.isfile('%s.tar' %(check)):
            self.log.verbose('Appending %s.tar to OutputSandbox' %check)
            okFiles.append('%s.tar' %(check))
          else:
            self.log.warn('Could not tar OutputSandbox directory: %s' %check)
            missing.append(check)

    for i in outputSandbox:
      if not i in okFiles:
        if not '%s.tar' %i in okFiles:
          if not re.search('\*',i):
            if not i in missing:
              missing.append(i)

    result = {'Missing':missing,'Files':okFiles}
    return S_OK(result)

  #############################################################################
  def __transferOutputDataFiles(self,owner,outputData,outputSE):
    """Performs the upload and registration in the LFC
    """
    self.log.verbose('Uploading output data files')
    self.__report('Completed','Uploading Output Data')
    self.log.verbose('Output data files %s to be uploaded to %s SE' %(string.join(outputData,', '),outputSE))
    missing = []
    for outputFile in outputData:
      if os.path.exists(outputFile):
        self.outputDataSize+=getGlobbedTotalSize(outputFile)
        lfn = self.__getLFNfromOutputFile(owner,outputFile)
        self.log.verbose('Attempting putAndRegister("%s","%s","%s")' %(lfn,outputFile,outputSE))
        upload = self.rm.putAndRegister(lfn, outputFile, outputSE)
        self.log.info(upload)
        if not upload['OK']:
          self.log.warn(upload['Message'])
          missing.append(outputFile)
        else:
          failed = upload['Value']['Failed']
          if failed:
            self.log.warn('Could not putAndRegister file %s with LFN %s to %s' %(outputFile,lfn,outputSE))
            self.log.warn(failed)
            missing.append(outputFile)
      else:
        self.log.warn('Output data file: %s is missing after execution' %(outputFile))

    if missing:
      self.__setJobParam('OutputData','MissingFiles: %s' %(string.join(missing,', ')))
      self.__report('Failed','Uploading Job OutputData')
      return S_ERROR('Failed to upload OutputData')

    return S_OK('OutputData uploaded successfully')

  #############################################################################
  def __getLFNfromOutputFile(self, owner, outputFile):
    """Provides a generic convention for VO output data
       files if no path is specified.
    """
    localfile = os.path.basename(string.replace(outputFile,"LFN:",""))
    lfn = outputFile
    if not re.search('^LFN:',outputFile):
      initial = owner[:1]
      lfn = '/'+self.vo+'/user/'+initial+'/'+owner+'/'+str(self.jobID)+'/'+localfile
    else:
      lfn = string.replace(outputFile,"LFN:","")

    return lfn

  #############################################################################
  def transferInputSandbox(self,inputSandbox):
    """Downloads the input sandbox for the job
    """
    sandboxFiles = []
    self.__report('Running','Downloading InputSandbox')
    for i in inputSandbox: sandboxFiles.append(os.path.basename(i))
    if type( inputSandbox ) != type([]):
      sandboxFiles = [inputSandbox]

    self.log.info('Downloading InputSandbox for job %s: %s' %(self.jobID,string.join(sandboxFiles)))
    if os.path.exists('%s/inputsandbox' %(self.root)):
      # This is a debugging tool, get the file from local storage to debug Job Wrapper
      sandboxFiles.append('jobDescription.xml')
      for inputFile in sandboxFiles:
        if os.path.exists('%s/inputsandbox/%s' %(self.root,inputFile)):
          self.log.info('Getting InputSandbox file %s from local directory for testing' %(inputFile))
          shutil.copy(self.root+'/inputsandbox/'+inputFile,inputFile)
      result = S_OK(sandboxFiles)
    elif not self.jobID:
      self.log.info('No JobID defined, no sandbox to download')
    else:
      inputSandboxClient = SandboxClient()
      result = inputSandboxClient.getSandbox(int(self.jobID))
      if not result['OK']:
        self.log.warn(result)
        self.__report('Running','Failed Downloading InputSandbox')
        return S_ERROR('InputSandbox download failed for job %s and sandbox %s' %(self.jobID,string.join(sandboxFiles)))

    self.log.verbose('Sandbox download result: %s' %(result))
    #for accounting report
    checkFileSize = []
    for sandboxFile in sandboxFiles:
      if not re.search('^lfn:',i) and not re.search('^LFN:',i):
        checkFileSize.append(sandboxFile)

    # FIXME: should make use os tarfile module to make the code more portable
    for sandboxFile in sandboxFiles:
      if re.search('.tar.gz$',sandboxFile) or re.search('.tgz$',sandboxFile):
        if os.path.exists(sandboxFile):
          self.log.verbose('Unpacking input sandbox file %s' %(sandboxFile))
          os.system('tar -zxf %s' %sandboxFile)

    lfns = []
    for i in inputSandbox:
      if re.search('^lfn:',i) or re.search('^LFN:',i):
        lfns.append(i)

    if lfns:
      self.__report('Running','Downloading InputSandbox LFN(s)')
      download = self.rm.getFile(lfns)
      if not download['OK']:
        self.log.warn(result)
        self.__report('Running','Failed Downloading InputSandbox LFN(s)')
        return S_ERROR(result['Message'])
      failed = download['Value']['Failed']
      if failed:
        self.log.warn('Could not download InputSandbox LFN(s)')
        self.log.warn(failed)
        return S_ERROR(str(failed))
      for lfn in lfns:
        if os.path.exists('%s/%s' %(self.root,os.path.basename(lfn))):
          checkFileSize.append(os.path.basename(lfn))

    if checkFileSize:
      self.inputSandboxSize = getGlobbedTotalSize(checkFileSize)

    return S_OK('InputSandbox downloaded')

  #############################################################################
  def finalize(self,arguments):
    """Perform any final actions to clean up after job execution.
    """
    self.log.info('Running JobWrapper finalization')
    self.__report('Done','Execution Complete')

    if not self.jobID:
      self.log.verbose('No accounting to be sent since running locally')
    else:
      self.diskSpaceConsumed = getGlobbedTotalSize('%s/%s' %(self.root,self.jobID))
      self.sendWMSAccounting()

    self.sendFailoverRequest()
    self.__cleanUp()
    return S_OK()

  #############################################################################
  def sendWMSAccounting(self,status='',minorStatus=''):
    """Send WMS accounting data.
    """
    if status:
      self.wmsMajorStatus = status
    if minorStatus:
      self.wmsMinorStatus = minorStatus

    self.accountingReport.setEndTime()
    #CPUTime and ExecTime
    if 'CPU' in EXECUTION_RESULT:
      utime, stime, cutime, cstime, elapsed = EXECUTION_RESULT['CPU']
    else:
      utime, stime, cutime, cstime, elapsed = os.times()
    cpuTime = utime + stime + cutime + cstime
    execTime = elapsed
    #Fill the data
    acData = {
               'User' : self.owner,
               'UserGroup' : self.userGroup,
               'JobGroup' : self.jobGroup,
               'JobType' : self.jobType,
               'JobClass' : self.jobClass,
               'ProcessingType' : self.processingType,
               'FinalMajorStatus' : self.wmsMajorStatus,
               'FinalMinorStatus' : self.wmsMinorStatus,
               'CPUTime' : cpuTime,
               'NormCPUTime' : cpuTime * gConfig.getValue ( "/LocalSite/CPUScalingFactor", 0.0 ),
               'ExecTime' : execTime,
               'InputDataSize' : self.inputDataSize,
               'OutputDataSize' : self.outputDataSize,
               'InputDataFiles' : self.inputDataFiles,
               'OutputDataFiles' : self.outputDataFiles,
               'DiskSpace' : self.diskSpaceConsumed,
               'InputSandBoxSize' : self.inputSandboxSize,
               'OutputSandBoxSize' : self.outputSandboxSize,
               'ProcessedEvents' : self.processedEvents
             }
    self.log.verbose('Accounting Report is:')
    self.log.verbose(acData)
    self.accountingReport.setValuesFromDict( acData )
    result = self.accountingReport.commit()
    return result

  #############################################################################
  def sendFailoverRequest(self):
    """ Create and send a combined job failover reauest if any
    """
    request = RequestContainer()
    requestName = 'job_%s_combined_request.xml' % self.jobID
    request.setRequestName(requestName)
    request.setJobID(self.jobID)
    request.setSourceComponent("Job_%s" % self.jobID)

    # JobReport part first
    result = self.jobReport.generateRequest()
    if result['OK']:
      reportRequest = result['Value']
      if reportRequest:
        request.update(reportRequest)

    # Accounting part
    result = self.accountingReport.commit()
    if not result['OK']:
      subrequest = DISETSubRequest(result['rpcStub']).getDictionary()
      request.addSubRequest(subrequest,'accounting')

    # Any other requests in the current directory
    rfiles = glob.glob('*_request.xml')
    for rfname in rfiles:
      rfile = open(rfname,'r')
      reqString = rfile.read()
      rfile.close()
      requestStored = RequestContainer(reqString)
      request.update(requestStored)

    # The request is ready, send it now
    if not request.isEmpty():
      requestClient = RequestClient()
      requestString = request.toXML()
      result = requestClient.setRequest(requestName, requestString)
      return result
    else:
      return S_OK()

  #############################################################################
  def __cleanUp(self):
    """Cleans up after job processing. Can be switched off via environment
       variable DO_NOT_DO_JOB_CLEANUP or by JobWrapper configuration option.
    """
    #Environment variable is a feature for DIRAC (helps local debugging).
    if os.environ.has_key('DO_NOT_DO_JOB_CLEANUP') or not self.cleanUpFlag:
      cleanUp = False
    else:
      cleanUp = True

    os.chdir(self.root)
    if cleanUp:
      self.log.verbose('Cleaning up job working directory')
      if os.path.exists(self.jobID):
        shutil.rmtree(self.jobID)

  #############################################################################
  def __setInitialJobParameters(self,arguments):
    """Sets some initial job parameters
    """
    parameters = []
    ceArgs = arguments['CE']
    if ceArgs.has_key('LocalSE'):
      parameters.append(('AgentLocalSE',string.join(ceArgs['LocalSE'],',')))
    if ceArgs.has_key('CompatiblePlatforms'):
      parameters.append(('AgentCompatiblePlatforms',string.join(ceArgs['CompatiblePlatforms'],',')))
    if ceArgs.has_key('PilotReference'):
      parameters.append(('Pilot_Reference', ceArgs['PilotReference']))
    if ceArgs.has_key('CPUScalingFactor'):
      parameters.append(('CPUScalingFactor', ceArgs['CPUScalingFactor']))

    parameters.append (('PilotAgent',self.diracVersion))
    result = self.__setJobParamList(parameters)
    return result

  #############################################################################
  def __report(self,status,minorStatus):
    """Wraps around setJobStatus of state update client
    """
    self.wmsMajorStatus = status
    self.wmsMinorStatus = minorStatus
    jobStatus = S_OK()
    if self.jobID:
      #jobReport  = RPCClient('WorkloadManagement/JobStateUpdate')
      jobStatus = self.jobReport.setJobStatus(int(self.jobID),status,minorStatus)
      self.log.verbose('setJobStatus(%s,%s,%s,%s)' %(self.jobID,status,minorStatus,'JobWrapper'))
      if not jobStatus['OK']:
        self.log.warn(jobStatus['Message'])
    else:
      self.log.verbose('JobID not defined, no status updates will be sent')

    return jobStatus

  #############################################################################
  def __setJobParam(self,name,value):
    """Wraps around setJobParameter of state update client
    """
    jobParam = S_OK()
    if self.jobID:
      #jobReport  = RPCClient('WorkloadManagement/JobStateUpdate')
      jobParam = self.jobReport.setJobParameter(str(name),str(value))
      self.log.verbose('setJobParameter(%s,%s,%s)' %(self.jobID,name,value))
      if not jobParam['OK']:
        self.log.warn(jobParam['Message'])
    else:
      self.log.verbose('JobID not defined, no parameter information will be reported')

    return jobParam

  #############################################################################
  def __setJobParamList(self,value):
    """Wraps around setJobParameters of state update client
    """
    jobParam = S_OK()
    if self.jobID:
      #jobReport  = RPCClient('WorkloadManagement/JobStateUpdate')
      jobParam = self.jobReport.setJobParameters(value)
      self.log.verbose('setJobParameters(%s,%s)' %(self.jobID,value))
      if not jobParam['OK']:
        self.log.warn(jobParam['Message'])
    else:
      self.log.verbose('JobID not defined, no parameter information will be reported')

    return jobParam

###############################################################################
###############################################################################

class ExecutionThread(threading.Thread):

  #############################################################################
  def __init__(self,spObject,cmd,maxPeekLines):
    threading.Thread.__init__(self)
    self.cmd = cmd
    self.spObject = spObject
    self.outputLines = []
    self.maxPeekLines = maxPeekLines

  #############################################################################
  def run(self):
    # FIXME: why local intances of object variables are created?
    cmd = self.cmd
    spObject = self.spObject
    start = time.time()
    initialStat = os.times()
    output = spObject.systemCall( cmd, callbackFunction = self.sendOutput, shell = True )
    EXECUTION_RESULT['Thread'] = output
    timing = time.time() - start
    EXECUTION_RESULT['Timing']=timing
    finalStat = os.times()
    EXECUTION_RESULT['CPU'] = []
    for i in range( len( finalStat ) ):
      EXECUTION_RESULT['CPU'].append( finalStat[i] - initialStat[i] )

  #############################################################################
  def getCurrentPID(self):
    return self.spObject.getChildPID()

  #############################################################################
  def sendOutput(self,stdid,line):
    self.outputLines.append(line)

  #############################################################################
  def getOutput(self,lines=0):
    if self.outputLines:
      size = len(self.outputLines)
      #reduce max size of output peeking
      # FIXME: this should be done in the receiving method (sendOutput)
      if size > self.maxPeekLines:
        cut = size - self.maxPeekLines
        self.outputLines = self.outputLines[cut:]
      #restrict to smaller number of lines for regular
      #peeking by the watchdog
      # FIXME: this is multithread, thus single line would be better
      if lines:
        size = len(self.outputLines)
        cut  = size - lines
        self.outputLines = self.outputLines[cut:]

      result = S_OK()
      result['Value'] = self.outputLines
    else:
      result = S_ERROR('No Job output found')

    return result

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#