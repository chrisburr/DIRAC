########################################################################
# $HeadURL$
# File :   ComputingElement.py
# Author : Stuart Paterson
########################################################################

"""  The Computing Element class provides the default options to construct
     resource JDL for subsequent use during the matching process.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.ClassAd.ClassAdLight      import *
from DIRAC.ConfigurationSystem.Client.Config        import gConfig
from DIRAC.Core.Security                            import File
from DIRAC.Core.Security.Misc                       import getProxyInfoAsString
from DIRAC                                          import S_OK, S_ERROR, gLogger, version

import os, re, string

INTEGER_PARAMETERS = ['CPUTime']
FLOAT_PARAMETERS = []
WAITING_TO_RUNNING_RATIO = 0.5
MAX_WAITING_JOBS = 1
MAX_TOTAL_JOBS = 1

class ComputingElement:

  #############################################################################
  def __init__(self, ceName):
    """ Standard constructor
    """
    self.log  = gLogger.getSubLogger( ceName )
    self.ceName = ceName
    self.ceType = ''
    #self.log.setLevel('debug') #temporary for debugging
    self.classAd = ClassAd('[]')
    self.ceRequirementDict = {}
    self.ceParameters = {}
    self.proxy = ''
    
    self.ceConfigDict = getLocalCEConfigDict(ceName)
    self.initializeParameters()
        
  def setProxy(self,proxy):
    """ Set proxy for this instance
    """      
    self.proxy = proxy
        
  def initializeParameters(self):
    """ Initialize the CE parameters after they are collected from various sources
    """  
    
    # Collect global defaults first
    result = self.__getCEParameters('/Resources/Computing/CEDefaults') #can be overwritten by other sections
    if not result['OK']:
      self.log.warn(result['Message'])
    result = self.__getCEParameters('/Resources/Computing/%s' % self.ceName )
    if not result['OK']:
      self.log.warn(result['Message'])
      
    # Get local CE configuration
    localConfigDict = getCEConfigDict(self.ceName)
    self.ceParameters.update(localConfigDict)

    result = self.__getSiteParameters()
    if not result['OK']:
      self.log.warn(result['Message'])
    result = self.__getResourceRequirements()
    if not result['OK']:
      self.log.warn(result['Message'])

    self._addCEConfigDefaults()
            
  def isValid(self):
    """ Check the sanity of the Computing Element definition
    """  
    for par in self.mandatoryParameters:
      if par not in self.ceParameters:
        return S_ERROR('Missing Mandatory Parameter in Configuration: %s' % par)        
    return S_OK()  
    

  #############################################################################
  def _addCEConfigDefaults( self ):
    """Method to make sure all necessary Configuration Parameters are defined
    """
    try:
      self.ceParameters['WaitingToRunningRatio'] = float(self.ceParameters['WaitingToRunningRatio'])
    except:
      # if it does not exist or it can not be converted, take the default
      self.ceParameters['WaitingToRunningRatio'] = WAITING_TO_RUNNING_RATIO

    try:
      self.ceParameters['MaxWaitingJobs'] = int(self.ceParameters['MaxWaitingJobs'])
    except:
      # if it does not exist or it can not be converted, take the default
      self.ceParameters['MaxWaitingJobs'] = MAX_WAITING_JOBS

    try:
      self.ceParameters['MaxTotalJobs'] = int(self.ceParameters['MaxTotalJobs'])
    except:
      # if it does not exist or it can not be converted, take the default
      self.ceParameters['MaxTotalJobs'] = MAX_TOTAL_JOBS



  #############################################################################
  def __getResourceRequirements(self):
    """Adds resource requirements to the ClassAd.
    """
    reqtSection = '/AgentJobRequirements'
    result = gConfig.getOptionsDict(reqtSection)
    if not result['OK']:
      self.log.warn(result['Message'])
      return S_OK(result['Message'])
    
    reqsDict = result['Value']
    self.ceRequirementDict.update( reqsDict )

    return S_OK('Added requirements')

  #############################################################################
  def __getInt(self,value):
    """To deal with JDL integer values.
    """
    tmpValue = None

    try:
     tmpValue = int(value.replace('"',''))
    except Exception, x:
      pass

    if tmpValue:
      value = tmpValue

    return value

  #############################################################################
  def __getSiteParameters(self):
    """Adds site specific parameters to the resource ClassAd.
    """
    osSection = '/Resources/Computing/OSCompatibility'
    platforms = {}
    result = gConfig.getOptionsDict(osSection)
    if not result['OK']:
      self.log.warn(result['Message'])
      # return S_ERROR(result['Message'])
    else:
      if not result['Value']:
        self.log.warn('Could not obtain %s section from CS' %(osSection))
        # return S_ERROR('Could not obtain %s section from CS' %(osSection))
      else:
        platforms = result['Value']
    self.log.debug('Platforms are %s' %(platforms))

    section = '/LocalSite'
    options = gConfig.getOptionsDict(section)
    if not options['OK']:
      self.log.warn(options['Message'])
      return S_ERROR(options['Message'])
    if not options['Value']:
      self.log.warn('Could not obtain %s section from CS' %(section))
      return S_ERROR('Could not obtain %s section from CS' %(section))

    localSite = options['Value']
    self.log.debug('Local site parameters are: %s' %(localSite))

    for option,value in localSite.items():
      if option == 'Architecture':
        self.ceParameters['LHCbPlatform'] = value
        self.ceParameters['Architecture'] = value
        if value in platforms.keys():
          compatiblePlatforms = platforms[value]
          self.ceParameters['CompatiblePlatforms'] = compatiblePlatforms.split(', ')
      elif option == 'LocalSE':
        self.ceParameters['LocalSE'] = value.split(', ')  
      else:
        self.ceParameters[option] = value  

    return S_OK()
  
  def reset(self):
    """ Make specific CE parameter adjustments here
    """
    pass
    

  def setParameters(self,ceOptions):
    """ Add parameters from the given dictionary overriding the previous values
    """
    self.ceParameters.update(ceOptions)
    for key in ceOptions:
      if key in INTEGER_PARAMETERS:
        self.ceParameters[key] = int(self.ceParameters[key])
      if key in FLOAT_PARAMETERS: 
        self.ceParameters[key] = float(self.ceParameters[key]) 
    self.reset()
    return S_OK()
  
  def getParameterDict(self):
    """  Get the CE complete parameter dictionary
    """
    
    return self.ceParameters

  #############################################################################
  def __getCEParameters(self,cfgName):
    """Adds CE specific parameters to the resource ClassAd.
    """
    section = cfgName
    options = gConfig.getOptionsDict(section)
    if not options['OK']:
      self.log.warn(options['Message'])
      return S_ERROR('Could not obtain %s section from CS' %(section))
    if not options['Value']:
      return S_ERROR('Empty CS section %s' %(section))

    ceOptions = options['Value']
    for key in ceOptions:
      if key in INTEGER_PARAMETERS:
        ceOptions[key] = int(ceOptions[key])
      if key in FLOAT_PARAMETERS: 
        ceOptions[key] = float(ceOptions[key]) 
    self.ceParameters.update(ceOptions)
    return S_OK()

  #############################################################################
  def __getParameters(self,param):
    """Returns CE parameter, e.g. MaxTotalJobs
    """
    if type(param)==type(' '):
      if param in self.ceParameters.keys():
        return S_OK(self.ceParameters[param])
      else:
        return S_ERROR('Parameter %s not found' %(param))
    elif type(param)==type([]):
      result = {}
      for p in param:
        if param in self.ceParameters.keys():
          result[param] = self.ceParameters[param]
      if len(result.keys()) == len(p):
        return S_OK(result)
      else:
        return S_ERROR('Not all specified parameters available')

  #############################################################################
  def setCPUTimeLeft(self, cpuTimeLeft = None):
    """Update the CPUTime parameter of the CE classAd, necessary for running in filling mode
    """
    if not cpuTimeLeft:
      # do nothing
      return S_OK()
    try:
      intCPUTimeLeft = int( cpuTimeLeft )
    except:
      return S_ERROR('Wrong type for setCPUTimeLeft argument')

    self.classAd.insertAttributeInt('CPUTime', intCPUTimeLeft)
    self.ceParameters['CPUTime'] = intCPUTimeLeft

    return S_OK(intCPUTimeLeft)


  #############################################################################
  def available(self, requirements = {} ):
    """This method returns True if CE is available and false if not.  The CE
       instance polls for waiting and running jobs and compares to the limits
       in the CE parameters.
    """
    # FIXME: need to take into account the possible requirements from the pilots,
    #        so far the cputime
    result = self.getDynamicInfo()
    if not result['OK']:
      self.log.warn('Could not obtain CE dynamic information')
      self.log.warn(result['Message'])
      return result
    else:
      runningJobs = result['RunningJobs']
      waitingJobs = result['WaitingJobs']
      submittedJobs = result['SubmittedJobs']

    maxTotalJobs = int(self.__getParameters('MaxTotalJobs')['Value'])
    waitingToRunningRatio = float(self.__getParameters('WaitingToRunningRatio')['Value'])
    # if there are no Running job we can submit to get at most 'MaxWaitingJobs'
    # if there are Running jobs we can increase this to get a ratio W / R 'WaitingToRunningRatio'
    maxWaitingJobs = int( max( int(self.__getParameters('MaxWaitingJobs')['Value']),
                               runningJobs * waitingToRunningRatio ) )

    self.log.verbose('Max Number of Jobs:', maxTotalJobs )
    self.log.verbose('Max W/R Ratio:', waitingToRunningRatio )
    self.log.verbose('Max Waiting Jobs:', maxWaitingJobs )

    # Determine how many more jobs can be submitted
    message = '%s CE: SubmittedJobs=%s' %(self.ceName,submittedJobs)
    message += ', WaitingJobs=%s, RunningJobs=%s' %(waitingJobs,runningJobs)
    totalJobs = runningJobs + waitingJobs

    message +=', MaxTotalJobs=%s' %(maxTotalJobs)

    if totalJobs >= maxTotalJobs:
      self.log.info( 'Max Number of Jobs reached:', maxTotalJobs )
      result['Value'] = 0
      message = 'There are %s waiting jobs and total jobs %s >= %s max total jobs' % (waitingJobs,totalJobs,maxTotalJobs)
    else:
      additionalJobs = 0
      if waitingJobs < maxWaitingJobs:
        additionalJobs = maxWaitingJobs - waitingJobs
        if totalJobs + additionalJobs >= maxTotalJobs:
          additionalJobs = maxTotalJobs - totalJobs

      result['Value'] = additionalJobs

    # totalCPU = self.__getParameters('TotalCPUs')
    # if not totalCPU['OK']:
    #   self.log.warn('TotalCPUs not specified, setting default of 1')
    #   totalCPU = 1
    # else:
    #   totalCPU = int(totalCPU['Value'])
    # if totalCPU:
    #  message +=', TotalCPU=%s' %(totalCPU)
    result['Message'] = message
    return result

  #############################################################################
  def writeProxyToFile(self,proxy):
    """CE helper function to write a CE proxy string to a file.
    """
    result = File.writeToProxyFile( proxy )
    if not result[ 'OK' ]:
      self.log.error('Could not write proxy to file',result[ 'Message' ])
      return result

    proxyLocation = result[ 'Value' ]
    result = getProxyInfoAsString( proxyLocation )
    if not result['OK']:
      self.log.error('Could not get proxy info',result)
      return result
    else:
      self.log.info('Payload proxy information:')
      print result['Value']

    return S_OK(proxyLocation)

  #############################################################################
  def getJDL(self):
    """Returns CE JDL as a string.
    """
    
    # Add the CE parameters
    for option,value in self.ceParameters.items():
      if type(option) == type( [] ):
        self.classAd.insertAttributeVectorString(option, value.split(', '))
      elif type(value) == type(' '):
        jdlInt = self.__getInt(value)
        if type(jdlInt) == type(1):
          self.log.debug('Found JDL integer attribute: %s = %s' %(option,jdlInt))
          self.classAd.insertAttributeInt(option, jdlInt)
        else:
          self.log.debug('Found string attribute: %s = %s' %(option,value))
          self.classAd.insertAttributeString(option, value)
      elif type(value) == type(1):
        self.log.debug('Found integer attribute: %s = %s' %(option,value))
        self.classAd.insertAttributeInt(option, value)
      else:
        self.log.warn('Type of option %s = %s not determined' %(option,value))
    
    # Add the CE requirements if any
    requirements = ''
    for option,value in self.ceRequirementDict.items():
      if type(value) == type(' '):
        jdlInt = self.__getInt(value)
        if type(jdlInt) == type(1):
          requirements += ' other.'+option+' == %d &&' %(jdlInt)
          self.log.debug('Found JDL reqt integer attribute: %s = %s' %(option,jdlInt))
          self.classAd.insertAttributeInt(option, jdlInt)
        else:
          requirements += ' other.'+option+' == "%s" &&' %(value)
          self.log.debug('Found string reqt attribute: %s = %s' %(option,value))
          self.classAd.insertAttributeString(option, value)
      elif type(value) == type(1):
        requirements += ' other.'+option+' == %d &&' %(value)
        self.log.debug('Found integer reqt attribute: %s = %s' %(option,value))
        self.classAd.insertAttributeInt(option, value)
      else:
        self.log.warn('Could not determine type of:  %s = %s' %(option,value))

    if requirements:
      if re.search('&&$',requirements):
        requirements = requirements[:-3]
      self.classAd.set_expression('Requirements',requirements)
    else:
      self.classAd.set_expression('Requirements','True')
    
    release = gConfig.getValue('/LocalSite/ReleaseVersion',version)
    self.classAd.insertAttributeString('DIRACVersion',release)
    if self.classAd.isOK():
      jdl = self.classAd.asJDL()
      return S_OK(jdl)
    else:
      return S_ERROR('ClassAd job is not valid')

  #############################################################################
  def sendOutput(self,stdid,line):
    """ Callback function such that the results from the CE may be returned.
    """
    print line

  #############################################################################
  def submitJob(self,executableFile,proxy,dummy=None):
    """ Method to submit job, should be overridden in sub-class.
    """
    name = 'submitJob()'
    self.log.error('ComputingElement: %s should be implemented in a subclass' %(name))
    return S_ERROR('ComputingElement: %s should be implemented in a subclass' %(name))

  #############################################################################
  def getDynamicInfo(self):
    """ Method to get dynamic job information, can be overridden in sub-class.
    """
    name = 'getDynamicInfo()'
    self.log.error('ComputingElement: %s should be implemented in a subclass' %(name))
    return S_ERROR('ComputingElement: %s should be implemented in a subclass' %(name))

def getLocalCEConfigDict(ceName):
  """ Collect all the local settings relevant to the CE configuration
  """
  ceConfigDict = getCEConfigDict(ceName)
  resourceDict = getResourceDict(ceName)
  ceConfigDict.update(resourceDict)
  return ceConfigDict

def getCEConfigDict( ceName ):
  """Look into LocalSite for configuration Parameters for this CE
  """
  ceConfigDict = {}
  if ceName:
    result = gConfig.getOptionsDict( '/LocalSite/%s' % ceName )
    if result['OK']:
      ceConfigDict= result['Value']
  return ceConfigDict

def getResourceDict( ceName = None ):
  """Look into LocalSite for Resource Requirements
  """
  from DIRAC.WorkloadManagementSystem.private.Queues import maxCPUSegments
  ret = gConfig.getOptionsDict( '/LocalSite/ResourceDict' )
  if not ret['OK']:
    resourceDict = {}
  else:
    # FIXME: es mejor copiar el diccionario?
    resourceDict = dict(ret['Value'])

  # if a CE Name is given, check the corresponding section
  if ceName:
    ret = gConfig.getOptionsDict( '/LocalSite/%s/ResourceDict' % ceName )
    if ret['OK']:
      resourceDict.update( dict(ret['Value']) )

  # now add some defaults
  resourceDict['Setup'] = gConfig.getValue('/DIRAC/Setup','None')
  if not 'CPUTime' in resourceDict:
    resourceDict['CPUTime'] = maxCPUSegments[-1]
  if not 'PilotType' in resourceDict:
    # FIXME: this is a test, we need the list of available types
    resourceDict['PilotType'] = 'private'

  return resourceDict

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#