"""
TornadoService represent one service services, your handler must inherith form this class
TornadoService may be used only by TornadoServer.

To create you must write this "minimal" code::

  from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
  class yourServiceHandler(TornadoService):

    @classmethod
    def initializeHandler(cls, infosDict):
      ## Called 1 time, at first request.
      ## You don't need to use super or to call any parents method, it's managed by the server

    def initializeRequest(self):
      ## Called at each request

    auth_someMethod = ['authenticated']
    def export_someMethod(self):
      #Insert your method here, don't forgot the return


Then you must configure service like any other service

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from io import open

import os
import time
try:
  import httplib
except ImportError:  # python 3 compatibility
  import http.client as httplib
from datetime import datetime
from tornado.web import RequestHandler
from tornado import gen
import tornado.ioloop
from tornado.ioloop import IOLoop

import DIRAC

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.DISET.AuthManager import AuthManager
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Utilities.DErrno import ENOAUTH
from DIRAC.Core.Utilities.JEncode import decode, encode
from DIRAC.FrameworkSystem.Client.MonitoringClient import MonitoringClient


class TornadoService(RequestHandler):  # pylint: disable=abstract-method
  """
    TornadoService main class, manage all tornado services
    Instantiated at each request
  """

  # Because we initialize at first request, we use a flag to know if it's already done
  __FLAG_INIT_DONE = False

  # MonitoringClient, we don't use gMonitor which is not thread-safe
  # We also need to add specific attributes for each service
  _monitor = None

  @classmethod
  def _initMonitoring(cls, serviceName, fullUrl):
    """
      Init monitoring specific to service
    """

    # Init extra bits of monitoring

    cls._monitor = MonitoringClient()
    cls._monitor.setComponentType(MonitoringClient.COMPONENT_WEB)

    cls._monitor.initialize()

    if tornado.process.task_id() is None:  # Single process mode
      cls._monitor.setComponentName('Tornado/%s' % serviceName)
    else:
      cls._monitor.setComponentName('Tornado/CPU%d/%s' % (tornado.process.task_id(), serviceName))

    cls._monitor.setComponentLocation(fullUrl)

    cls._monitor.registerActivity("Queries", "Queries served", "Framework", "queries", MonitoringClient.OP_RATE)

    cls._monitor.setComponentExtraParam('DIRACVersion', DIRAC.version)
    cls._monitor.setComponentExtraParam('platform', DIRAC.getPlatform())
    cls._monitor.setComponentExtraParam('startTime', datetime.utcnow())

    cls._stats = {'requests': 0, 'monitorLastStatsUpdate': time.time()}

    return S_OK()

  @classmethod
  def __initializeService(cls, relativeUrl, absoluteUrl):
    """
      Initialize a service, called at first request

      :param relativeUrl: the url, something like "/component/service"
      :param absoluteUrl: the url, something like "https://dirac.cern.ch:1234/component/service"
    """
    # Url starts with a "/", we just remove it
    serviceName = relativeUrl[1:]

    cls.log = gLogger
    cls._startTime = datetime.utcnow()
    cls.log.info("First use of %s, initializing service..." % relativeUrl)
    cls._authManager = AuthManager("%s/Authorization" % PathFinder.getServiceSection(serviceName))

    cls._initMonitoring(serviceName, absoluteUrl)

    cls._serviceName = serviceName
    cls._validNames = [serviceName]
    serviceInfo = {'serviceName': serviceName,
                   'serviceSectionPath': PathFinder.getServiceSection(serviceName),
                   'csPaths': [PathFinder.getServiceSection(serviceName)],
                   'URL': absoluteUrl
                   }
    cls._serviceInfoDict = serviceInfo

    cls.__monitorLastStatsUpdate = time.time()

    try:
      cls.initializeHandler(serviceInfo)
    # If anything happen during initialization, we return the error
    # broad-except is necessary because we can't really control the exception in the handlers
    except Exception as e:  # pylint: disable=broad-except
      gLogger.error(e)
      return S_ERROR('Error while initializing')

    cls.__FLAG_INIT_DONE = True
    return S_OK()

  @classmethod
  def initializeHandler(cls, serviceInfoDict):
    """
      This may be overwritten when you write a DIRAC service handler
      And it must be a class method. This method is called only one time,
      at the first request

      :param dict ServiceInfoDict: infos about services, it contains
                                    'serviceName', 'serviceSectionPath',
                                    'csPaths' and 'URL'
    """
    pass

  def initializeRequest(self):
    """
      Called at every request, may be overwritten in your handler.
    """
    pass

  # This is a Tornado magic method

  def initialize(self):  # pylint: disable=arguments-differ
    """
      Initialize the handler, called at every request.


      ..warning::
        DO NOT REWRITE THIS FUNCTION IN YOUR HANDLER
        ==> initialize in DISET became initializeRequest in HTTPS !
    """

    self.authorized = False
    self.method = None
    self.requestStartTime = time.time()
    self.credDict = None
    self.authorized = False
    self.method = None

    # On internet you can find "HTTP Error Code" or "HTTP Status Code" for that.
    # In fact code>=400 is an error (like "404 Not Found"), code<400 is a status (like "200 OK")
    self._httpError = httplib.OK
    if not self.__FLAG_INIT_DONE:
      init = self.__initializeService(self.srv_getURL(), self.request.full_url())
      if not init['OK']:
        self._httpError = httplib.INTERNAL_SERVER_ERROR
        gLogger.error("Error during initalization on %s" % self.request.full_url())
        gLogger.debug(init)
        return False

    self._stats['requests'] += 1
    self._monitor.setComponentExtraParam('queries', self._stats['requests'])
    self._monitor.addMark("Queries")
    return True

  def prepare(self):
    """
      prepare the request, it reads certificates and check authorizations.
    """
    self.method = self.get_argument("method")
    self.rawContent = self.get_argument('rawContent', default=False)
    self.log.notice("Incoming request on /%s: %s" % (self._serviceName, self.method))

    # Init of service must be checked here, because if it have crashed we are
    # not able to end request at initialization (can't write on client)
    if not self.__FLAG_INIT_DONE:
      error = encode("Service can't be initialized ! Check logs on the server for more informations.")
      self.__write_return(error)
      self.finish()

    try:
      self.credDict = self._gatherPeerCredentials()
    except Exception:  # pylint: disable=broad-except
      # If an error occur when reading certificates we close connection
      # It can be strange but the RFC, for HTTP, say's that when error happend
      # before authentication we return 401 UNAUTHORIZED instead of 403 FORBIDDEN
      self.reportUnauthorizedAccess(httplib.UNAUTHORIZED)

    try:
      hardcodedAuth = getattr(self, 'auth_' + self.method)
    except AttributeError:
      hardcodedAuth = None

    self.authorized = self._authManager.authQuery(self.method, self.credDict, hardcodedAuth)
    if not self.authorized:
      self.reportUnauthorizedAccess()

  # Make post a coroutine.
  # See https://www.tornadoweb.org/en/branch5.1/guide/coroutines.html#coroutines
  # for details
  @gen.coroutine
  def post(self):  # pylint: disable=arguments-differ
    """
      HTTP POST, used for RPC
      Call the remote method, client may send his method via "method" argument
      and list of arguments in JSON in "args" argument
    """

    # Execute the method in an executor (basically a separate thread)
    # Because of that, we cannot calls certain methods like `self.write`
    # in __executeMethod. This is because these methods are not threadsafe
    # https://www.tornadoweb.org/en/branch5.1/web.html#thread-safety-notes
    # However, we can still rely on instance attributes to store what should
    # be sent back (like self._httpError) (reminder: there is an instance
    # of this class created for each request)
    retVal = yield IOLoop.current().run_in_executor(None, self.__executeMethod)

    # Here it is safe to write back to the client, because we are not
    # in a thread anymore
    self.__write_return(retVal.result())
    self.finish()

  # This nice idea of streaming to the client cannot work because we are ran in an executor
  # and we should not write back to the client in a different thread.
  # See https://www.tornadoweb.org/en/branch5.1/web.html#thread-safety-notes
  # def export_streamToClient(self, filename):
  #   # https://bhch.github.io/posts/2017/12/serving-large-files-with-tornado-safely-without-blocking/
  #   #import ipdb; ipdb.set_trace()
  #   # chunk size to read
  #   chunk_size = 1024 * 1024 * 1  # 1 MiB

  #   with open(filename, 'rb') as f:
  #     while True:
  #       chunk = f.read(chunk_size)
  #       if not chunk:
  #         break
  #       try:
  #         self.write(chunk)  # write the chunk to response
  #         self.flush()  # send the chunk to client
  #       except StreamClosedError:
  #         # this means the client has closed the connection
  #         # so break the loop
  #         break
  #       finally:
  #         # deleting the chunk is very important because
  #         # if many clients are downloading files at the
  #         # same time, the chunks in memory will keep
  #         # increasing and will eat up the RAM
  #         del chunk
  #         # pause the coroutine so other handlers can run
  #         yield gen.sleep(0.000000001)  # 1 nanosecond

  #   return S_OK()

  @gen.coroutine
  def __executeMethod(self):
    """
      Execute the method called, this method is executed in an executor
      We have several try except to catch the different problem who can occurs

      - First, the method does not exist => Attribute error, return an error to client
      - second, anything happend during execution => General Exception, send error to client

      .. warning::
        This method is called in an executor, and so cannot use methods like self.write
        See https://www.tornadoweb.org/en/branch5.1/web.html#thread-safety-notes
    """

    # getting method
    try:
      # For compatibility reasons with DISET, the methods are still called ``export_*``
      method = getattr(self, 'export_%s' % self.method)
    except AttributeError as e:
      self._httpError = httplib.NOT_IMPLEMENTED
      return S_ERROR("Unknown method %s" % self.method)

    # Decode args
    args_encoded = self.get_body_argument('args', default=encode([]))

    args = decode(args_encoded)[0]
    # Execute
    try:
      self.initializeRequest()
      retVal = method(*args)
    except Exception as e:  # pylint: disable=broad-except
      gLogger.exception("Exception serving request", "%s:%s" % (str(e), repr(e)))
      retVal = S_ERROR(repr(e))
      self._httpError = httplib.INTERNAL_SERVER_ERROR

    return retVal

  def __write_return(self, dictionary):
    """
      Write to client what we want to return to client
      It must be a dictionary
    """

    # In case of error in server side we hide server CallStack to client
    if 'CallStack' in dictionary:
      del dictionary['CallStack']

    # Write status code before writing, by default error code is "200 OK"
    self.set_status(self._httpError)

    # This is basically only used for file download through
    # the 'streamToClient' method.
    if self.rawContent:
      # See 4.5.1 http://www.rfc-editor.org/rfc/rfc2046.txt
      self.set_header("Content-Type", "application/octet-stream")
      returnedData = dictionary
    else:
      self.set_header("Content-Type", "application/json")
      returnedData = encode(dictionary)

    self.write(returnedData)

  def reportUnauthorizedAccess(self, errorCode=401):
    """
      This method stop the current request and return an error to client


      :param int errorCode: Error code, 403 is "Forbidden" and 401 is "Unauthorized"
    """
    error = S_ERROR(ENOAUTH, "Unauthorized query")
    gLogger.error(
        "Unauthorized access to %s: %s from %s" %
        (self.request.path,
         self.credDict['DN'],
         self.request.remote_ip))

    self._httpError = errorCode
    self.__write_return(error)
    self.finish()

  def on_finish(self):
    """
      Called after the end of HTTP request
    """
    requestDuration = time.time() - self.requestStartTime
    gLogger.notice("Ending request to %s after %fs" % (self.srv_getURL(), requestDuration))

  def _gatherPeerCredentials(self):
    """
      Load client certchain in DIRAC and extract informations.

      The dictionary returned is designed to work with the AuthManager,
      already written for DISET and re-used for HTTPS.
    """

    chainAsText = self.request.get_ssl_certificate().as_pem()
    peerChain = X509Chain()

    # Here we read all certificate chain
    cert_chain = self.request.get_ssl_certificate_chain()
    for cert in cert_chain:
      chainAsText += cert.as_pem()

    # And we let some utilities do the job...
    # Following lines just get the right info, at the right place
    peerChain.loadChainFromString(chainAsText)

    # Retrieve the credentials
    res = peerChain.getCredentials(withRegistryInfo=False)
    if not res['OK']:
      raise Exception(res['Message'])

    credDict = res['Value']

    # We check if client sends extra credentials...
    if "extraCredentials" in self.request.arguments:
      extraCred = self.get_argument("extraCredentials")
      if extraCred:
        credDict['extraCredentials'] = decode(extraCred)[0]
    return credDict


####
#
#   Default method
#
####

  auth_ping = ['all']

  def export_ping(self):
    """
      Default ping method, returns some info about server.

      It returns the exact same information as DISET, for transparency purpose.
    """
    # COPY FROM DIRAC.Core.DISET.RequestHandler
    dInfo = {}
    dInfo['version'] = DIRAC.version
    dInfo['time'] = datetime.utcnow()
    # Uptime
    try:
      with open("/proc/uptime", 'rt') as oFD:
        iUptime = int(float(oFD.readline().split()[0].strip()))
      dInfo['host uptime'] = iUptime
    except Exception:  # pylint: disable=broad-except
      pass
    startTime = self._startTime
    dInfo['service start time'] = self._startTime
    serviceUptime = datetime.utcnow() - startTime
    dInfo['service uptime'] = serviceUptime.days * 3600 + serviceUptime.seconds
    # Load average
    try:
      with open("/proc/loadavg", 'rt') as oFD:
        dInfo['load'] = " ".join(oFD.read().split()[:3])
    except Exception:  # pylint: disable=broad-except
      pass
    dInfo['name'] = self._serviceInfoDict['serviceName']
    stTimes = os.times()
    dInfo['cpu times'] = {'user time': stTimes[0],
                          'system time': stTimes[1],
                          'children user time': stTimes[2],
                          'children system time': stTimes[3],
                          'elapsed real time': stTimes[4]
                          }

    return S_OK(dInfo)

  auth_echo = ['all']

  @staticmethod
  def export_echo(data):
    """
    This method used for testing the performance of a service
    """
    return S_OK(data)

  auth_whoami = ['authenticated']

  def export_whoami(self):
    """
      A simple whoami, returns all credential dictionary, except certificate chain object.
    """
    credDict = self.srv_getRemoteCredentials()
    if 'x509Chain' in credDict:
      # Not serializable
      del credDict['x509Chain']
    return S_OK(credDict)

####
#
#  Utilities methods, some getters.
#  From DIRAC.Core.DISET.requestHandler to get same interface in the handlers.
#  Adapted for Tornado.
#  These method are copied from DISET RequestHandler, they are not all used when i'm writing
#  these lines. I rewrite them for Tornado to get them ready when a new HTTPS service need them
#
####

  @classmethod
  def srv_getCSOption(cls, optionName, defaultValue=False):
    """
    Get an option from the CS section of the services

    :return: Value for serviceSection/optionName in the CS being defaultValue the default
    """
    if optionName[0] == "/":
      return gConfig.getValue(optionName, defaultValue)
    for csPath in cls._serviceInfoDict['csPaths']:
      result = gConfig.getOption("%s/%s" % (csPath, optionName, ), defaultValue)
      if result['OK']:
        return result['Value']
    return defaultValue

  def getCSOption(self, optionName, defaultValue=False):
    """
      Just for keeping same public interface
    """
    return self.srv_getCSOption(optionName, defaultValue)

  def srv_getRemoteAddress(self):
    """
    Get the address of the remote peer.

    :return: Address of remote peer.
    """
    return self.request.remote_ip

  def getRemoteAddress(self):
    """
      Just for keeping same public interface
    """
    return self.srv_getRemoteAddress()

  def srv_getRemoteCredentials(self):
    """
    Get the credentials of the remote peer.

    :return: Credentials dictionary of remote peer.
    """
    return self.credDict

  def getRemoteCredentials(self):
    """
    Get the credentials of the remote peer.

    :return: Credentials dictionary of remote peer.
    """
    return self.credDict

  def srv_getFormattedRemoteCredentials(self):
    """
      Return the DN of user
    """
    try:
      return self.credDict['DN']
    except KeyError:  # Called before reading certificate chain
      return "unknown"

  def srv_getServiceName(self):
    """
      Return the service name
    """
    return self._serviceInfoDict['serviceName']

  def srv_getURL(self):
    """
      Return the URL
    """
    return self.request.path
