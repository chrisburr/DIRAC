"""TODO"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import argparse
from collections import defaultdict
import fnmatch
import functools
import importlib
import importlib_resources
import os
import pkgutil
import sys

import six

try:
  from importlib.machinery import PathFinder
except ImportError:
  # Fallback for Python 2
  import imp

  class ModuleSpec(object):
    def __init__(self, name, submodule_search_locations):
      self.name = name
      self.submodule_search_locations = submodule_search_locations


  class PathFinder(object):
    @classmethod
    def find_spec(cls, name, path=None):
      try:
        _, pathname, _ = imp.find_module(name, path)
      except ImportError:
        return None
      else:
        return ModuleSpec(name, [pathname])



def iterateThenSort(func):
  @functools.wraps(func)
  def newFunc(modules):
    results = set()
    for module in modules:
      results |= func(module)
    return sorted(results)
  return newFunc


@iterateThenSort
def findSystems(module):
  """TODO"""
  return {x.name for x in _findSystems(module)}


@iterateThenSort
def findAgents(module):
  """TODO"""
  return {(system.name, obj_name) for system, obj_name in _findObject(module, "Agent", "Agent")}


@iterateThenSort
def findExecutors(module):
  """TODO"""
  return {(system.name, obj_name) for system, obj_name in _findObject(module, "Executor")}


@iterateThenSort
def findServices(module):
  """TODO"""
  return {(system.name, obj_name) for system, obj_name in _findObject(module, "Service", "Handler")}


@iterateThenSort
def findDatabases(module):
  """TODO"""
  # This can be "fn.name" when DIRAC is Python 3 only
  return {(system.name, os.path.basename(str(fn))) for system, fn in _findFile(module, "DB", "*DB.sql")}


def entrypointToExtension(entrypoint):
  """"Get the extension name from an EntryPoint object"""
  # In Python 3.9 this can be "entrypoint.module"
  module = entrypoint.pattern.match(entrypoint.value).groupdict()["module"]
  extensionName = module.split(".")[0]
  return extensionName


def extensionsByPriority():
  """Discover extensions using the setuptools metadata

  TODO: This should move into a function which can also be called to fill the CS
  """
  # This is Python 3 only, Python 2 installations should never try to use this
  from importlib import metadata

  priorties = defaultdict(list)
  for entrypoint in set(metadata.entry_points()['dirac']):
    extensionName = entrypointToExtension(entrypoint)
    extension_metadata = entrypoint.load()()
    priorties[extension_metadata["priority"]].append(extensionName)

  extensions = []
  for priority, extensionNames in sorted(priorties.items()):
    if len(extensionNames) != 1:
      print(
          "WARNING: Found multiple extensions with priority",
          "{} ({})".format(priority, extensionNames),
          file=sys.stderr,
      )
    # If multiple are passed, sort the extensions so things are deterministic at least
    extensions.extend(sorted(extensionNames))
  return extensions


def getExtensionMetadata(extensionName):
  """Get the metadata for a given extension name"""
  # This is Python 3 only, Python 2 installations should never try to use this
  from importlib import metadata

  for entrypoint in metadata.entry_points()['dirac']:
    if extensionName == entrypointToExtension(entrypoint):
      return entrypoint.load()()


def recurseImport(modName, parentModule=None, hideExceptions=False):
  from DIRAC import S_OK, S_ERROR, gLogger

  if parentModule is not None:
    raise NotImplementedError(parentModule)
  try:
    return S_OK(importlib.import_module(modName))
  except ImportError as excp:
    if str(excp).startswith("No module named"):
      return S_OK()
    errMsg = "Can't load %s" % modName
    if not hideExceptions:
      gLogger.exception(errMsg)
    return S_ERROR(errMsg)


def _findSystems(module):
  """TODO"""
  for _, name, _ in pkgutil.iter_modules(module.__path__):
    if name.endswith("System"):
      yield PathFinder.find_spec(name, path=module.__path__)


def _findObject(module, submoduleName, objectSuffix=""):
  """TODO"""
  for system in _findSystems(module):
    agentModule = PathFinder.find_spec(submoduleName, path=system.submodule_search_locations)
    if not agentModule:
      continue
    for _, name, _ in pkgutil.iter_modules(agentModule.submodule_search_locations):
      if name.endswith(objectSuffix):
        yield system, name


def _findFile(module, submoduleName, pattern="*"):
  """TODO"""
  for system in _findSystems(module):
    try:
      dbModule = importlib_resources.files(".".join([module.__name__, system.name, "DB"]))
    except ImportError:
      continue
    for file in dbModule.iterdir():
      if fnmatch.fnmatch(file.name, pattern):
        yield system, file


def parseArgs():
  parser = argparse.ArgumentParser()
  if six.PY3:
    subparsers = parser.add_subparsers(required=True, dest='function')
  else:
    subparsers = parser.add_subparsers()
  if six.PY3:
    defaultExtensions = extensionsByPriority()
  else:
    defaultExtensions = ["LHCbDIRAC", "DIRAC"]
  for func in [findSystems, findAgents, findExecutors, findServices, findDatabases]:
    subparser = subparsers.add_parser(func.__name__)
    subparser.add_argument("--extensions", nargs="+", default=defaultExtensions)
    subparser.set_defaults(func=func)
  args = parser.parse_args()
  # Get the result and print it
  extensions = [importlib.import_module(e) for e in args.extensions]
  for result in args.func(extensions):
    if not isinstance(result, str):
      result = " ".join(result)
    print(result)


if __name__ == "__main__":
  parseArgs()
